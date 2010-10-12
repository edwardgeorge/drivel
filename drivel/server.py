#!/usr/bin/python

"""
The drivel server program.

This program contains the drivel server class.
"""

from collections import defaultdict
import errno
import gc
import logging
import os
import pprint
import re
import sys
import time
import uuid

import eventlet
from eventlet import backdoor
from eventlet import event
from eventlet.green import socket
from eventlet import greenio
from eventlet import greenthread
from eventlet import hubs
from eventlet import queue
from eventlet import wsgi

import drivel.logstuff
from drivel.messaging.broker import Broker
from drivel.utils import debug
from drivel.wsgi import create_application

__all__ = ['Server', 'start']

PREFORK_SOCKETS = {}


def listen(addr):
    try:
        return PREFORK_SOCKETS[addr]
    except KeyError, e:
        return eventlet.listen(addr)


def statdumper(server, interval):
    while True:
        pprint.pprint(server.stats())
        eventlet.sleep(interval)


def timed_switch_out(self):
    self._last_switch_out = time.time()

greenthread.GreenThread.switch_out = timed_switch_out


class safe_exit(object):
    exit_advice = 'exit from telnet is ^]'

    def __call__(self):
        print self.exit_advice

    def __repr__(self):
        return self.exit_advice

    def __str__(self):
        return self.exit_advice


class dummylog(object):
    def write(self, data):
        pass


def get_config_section_for_name(config, section, name):
    if name is None:
        return config.get(section)
    else:
        return config.section_with_overrides('%s:%s' %
            (section, name))


class Server(object):
    def __init__(self, config, options):
        self.config = config
        self.options = options
        self.name = self.options.name
        if self.name is None:
            self.procid = self.name = uuid.uuid4()
        else:
            self.procid = '%s-%s' % (self.name, uuid.uuid4())
        self.server_config = self.get_config_section('server')
        self.components = {}
        self.broker = Broker(self.name, self.procid)
        self.wsgiroutes = []
        #concurrency = 4
        #if self.config.has_option('server', 'mq_concurrency'):
            #concurrency = self.config.getint('server', 'mq_concurrency')
        #self._pool = pool.Pool(max_size=concurrency)
        self._setupLogging()

    def get_config_section(self, section):
        name = self.name
        return get_config_section_for_name(self.config, section, name)

    def start(self, start_listeners=True):
        self.log('Server', 'info', 'starting server "%s" (%s)' %
            (self.name, self.procid))
        blisten = self.server_config.get('broker_listen', '')
        for i in blisten.split(','):
            if i:
                host, _, port = i.partition(':')
                port = int(port)
                self.broker.connections.listen((host, port))
        conns = self.get_config_section('connections')
        for k, v in conns:
            host, _, port = v.partition(':')
            port = int(port)
            self.broker.connections.connect((host, port), target=k)
        self.broker.start()
        components = self.get_config_section('components')
        for name in components:
            self.log('Server', 'info', 'adding "%s" component to %s' %
                (name, self.procid))
            self.components[name] = components.import_(name)(self,
                name)
        if start_listeners and 'backdoor_port' in self.config.server:
            # enable backdoor console
            bdport = self.config.getint(('server', 'backdoor_port'))
            self.log('Server', 'info', 'enabling backdoor on port %s'
                % bdport)
            eventlet.spawn(backdoor.backdoor_server,
                listen(('127.0.0.1', bdport)),
                locals={'server': self,
                        'debug': debug,
                        'exit': safe_exit(),
                        'quit': safe_exit(),
                        'stats': lambda: pprint.pprint(self.stats()),
                })
        app = create_application(self)
        dirs = self.server_config.get('static_directories', None)
        if dirs is not None:
            from drivel.contrib.fileserver import StaticFileServer
            app = StaticFileServer(dirs.split(','), app, self)
        self.wsgiapp = app
        pool = self.server_pool = eventlet.GreenPool(10000)
        if start_listeners and self.server_config.getboolean('start_www', True):
            numsimulreq = self.config.get(('http', 'max_simultaneous_reqs'))
            host = self.config.http.address
            port = self.config.http.getint('port')
            sock = listen((host, port))
            log = (self.options.nohttp or self.options.statdump) and \
                dummylog() or None
            self.log('Server', 'info', 'starting www server on %s:%s,'
                    ' component %s@%s' % (host, port, self.name, self.procid))
            wsgi.server(sock, app, custom_pool=pool, log=log)
        elif start_listeners:
            try:
                hubs.get_hub().switch()
            except KeyboardInterrupt, e:
                pass
            

    def stop(self):
        for name, mod in self.components.items():
            mod.stop()
            del self.components[name]
        if not self._greenlet.dead:
            self._greenlet.throw()

    def send(self, subscription, *message, **kwargs):
        self.log('Server', 'debug', 'receiving message for %s'
            ': %s' % (subscription, message))
        to = None
        if kwargs.get('broadcast', False):
            to = self.broker.BROADCAST
        elif 'address_to' in kwargs:
            to = kwargs['address_to']
        return self.broker.send(to, subscription, message)

    def subscribe(self, subscription, queue):
        self.log('Server', 'info', 'adding subscription to %s'
            % subscription)
        self.broker.subscribe(subscription, queue)

    def add_wsgimapping(self, mapping, subscription):
        if not isinstance(mapping, (tuple, list)):
            mapping = (None, mapping)

        mapping = (subscription, mapping[0], re.compile(mapping[1]))
        self.wsgiroutes.append(mapping)

    def log(self, logger, level, message):
        logger = logging.getLogger(logger)
        getattr(logger, level)(message)

    def _setupLogging(self):
        level = getattr(logging,
            self.config.server.log_level.upper())
        lh_name = self.config.server.get(
            "log_handler",
            "drivel.logstuff.StreamLoggingHandler")
        lh_class = eval(lh_name)
        lh = lh_class()
        lh.setFormatter(logging.Formatter(
                self.config.server.get(
                    "log_format",
                    "%(name)s:%(levelname)s:%(lineno)d:%(message)s")))
        root_logger = logging.getLogger("")
        root_logger.handlers = []
        root_logger.addHandler(lh)
        root_logger.setLevel(level)
        #logging.basicConfig(level=level, stream=sys.stdout)

    def stats(self, gc_collect=False):
        stats = dict((key, comp.stats()) for key, comp
            in self.components.items())
        hub = hubs.get_hub()
        gettypes = lambda t: [o for o in gc.get_objects() if
            type(o).__name__ == t]
        if gc_collect:
            gc.collect() and gc.collect()
        stats.update({
            'server': {
                'wsgi_free': self.server_pool.free(),
                'wsgi_running': self.server_pool.running(),
            },
            'eventlet': {
                'next_timers': len(hub.next_timers),
                'timers': len(hub.timers),
                'readers': len(hub.listeners['read']),
                'writers': len(hub.listeners['write']),
                'timers_count': hub.get_timers_count(),
            },
            'python': {
                'greenthreads': len(gettypes('GreenThread')),
                'gc_tracked_objs': len(gc.get_objects()),
            },
            'broker': self.broker.stats(),
        })
        return stats


def start(config, options):
    server_config = get_config_section_for_name(config, 'server',
        options.name)
    if 'hub_module' in server_config:
        hubs.use_hub(server_config.import_('hub_module'))

    if 'fork_children' in server_config:
        if 'prefork_listen' in server_config:
            _toaddr = lambda (host, port): (host, int(port))
            toaddr = lambda addrstr: _toaddr(addrstr.split(':', 1))
            addrs = map(toaddr, server_config['prefork_listen'].split(','))
            for addr in addrs:
                PREFORK_SOCKETS[addr] = eventlet.listen(addr)
        children = server_config['fork_children'].split(',')
        connections = {}
        for i, j in enumerate(children):
            for k, l in enumerate(children):
                if i != k and (i, k) not in connections:
                    a, b = socket.socketpair()
                    connections[(i, k)] = (l, a)
                    connections[(k, i)] = (j, b)
        for i, child in enumerate(children):
            print 'forking', child
            pid = os.fork()
            if pid == 0:
                # child
                hub = hubs.get_hub()
                if hasattr(hub, 'poll') and hasattr(hub.poll, 'fileno'):
                    # probably epoll which uses a filedescriptor
                    # and thus forking without exec is bad using that
                    # poll instance.
                    hubs.use_hub(hubs.get_default_hub())
                myconns = []
                for (l, r), s in connections.items():
                    if l == i:
                        myconns.append(s)
                    else:
                        s[1].close()
                options.name = child
                start_single(config, options, myconns)
                sys.exit(1)
        while True:
            try:
                pid, exitstatus = os.wait()
                print 'childprocess %d died' % pid
            except OSError, e:
                if e.errno == errno.ECHILD:
                    sys.exit(0)
                else:
                    raise
            except KeyboardInterrupt, e:
                print 'quitting...'
    else:
        start_single(config, options)


def start_single(config, options, sockets=[]):
    server = Server(config, options)
    for sock in sockets:
        target = None
        if isinstance(sock, tuple):
            target, sock = sock
        server.broker.connections.add(sock, target=target)

    if options.statdump:
        interval = options.statdump
        eventlet.spawn_after(interval, statdumper, server, interval)
    server.start()


if __name__ == '__main__':
    # included here for existing tools compatibility
    from drivel.startup import main
    main()
