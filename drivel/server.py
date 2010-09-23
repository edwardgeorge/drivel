#!/usr/bin/python

"""
The drivel server program.

This program contains the drivel server class.
"""

from __future__ import with_statement
from collections import defaultdict
import gc
import logging
import os
import mimetypes
import pprint
import re
import sys
import time
import uuid

import eventlet
from eventlet import backdoor
from eventlet import event
from eventlet import hubs
from eventlet import queue
from eventlet import wsgi

import drivel.logstuff
from drivel.messaging.broker import Broker
from drivel.utils import debug
from drivel.wsgi import create_application


def statdumper(server, interval):
    while True:
        pprint.pprint(server.stats())
        eventlet.sleep(interval)


def timed_switch_out(self):
    self._last_switch_out = time.time()

from eventlet.greenthread import GreenThread
GreenThread.switch_out = timed_switch_out


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


class StaticFileServer(object):
    """For testing purposes only. Use a real static file server.
    """
    def __init__(self, directory_list, wrapped_app, host):
        self.host = host
        self.host.log("httpd", "info", "serving static files: %s" %
            directory_list)
        self.directory_list = [os.path.realpath(x) for x in directory_list]
        self.wrapped_app = wrapped_app

    def __call__(self, env, start_response):
        for directory in self.directory_list:
            path = os.path.realpath(directory + env['PATH_INFO'])
            if not path.startswith(directory):
                start_response("403 Forbidden",
                    [('Content-Type', 'text/plain')])
                return ['Forbidden']
            if os.path.isdir(path):
                path = os.path.join(path, 'index.html')
            if os.path.exists(path):
                content_type, encoding = mimetypes.guess_type(path)
                if content_type is None:
                    content_type = 'text/plain'
                start_response("200 OK", [('Content-Type', content_type)])
                return file(path).read()
        return self.wrapped_app(env, start_response)


class Server(object):
    def __init__(self, config, options):
        self.config = config
        self.options = options
        self.name = self.options.name
        if self.name is None:
            self.name = uuid.uuid1()
        self.server_config = self.get_config_section('server')
        self.components = {}
        self.broker = Broker(self.name)
        self._mqueue = queue.Queue()
        self.wsgiroutes = []
        #concurrency = 4
        #if self.config.has_option('server', 'mq_concurrency'):
            #concurrency = self.config.getint('server', 'mq_concurrency')
        #self._pool = pool.Pool(max_size=concurrency)
        self._setupLogging()

    def get_config_section(self, section):
        name = self.name
        if name is None:
            return self.config.get(section)
        else:
            return self.config.section_with_overrides('%s:%s' %
                (section, name))

    def start(self, start_listeners=True):
        self.log('Server', 'info', 'starting server "%s"' % self.name)
        listen = self.server_config.get('broker_listen', '')
        for i in listen.split(','):
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
        for name in self.config.components:
            self.components[name] = self.config.components.import_(name)(self,
                name)
        if start_listeners and 'backdoor_port' in self.config.server:
            # enable backdoor console
            bdport = self.config.getint(('server', 'backdoor_port'))
            self.log('Server', 'info', 'enabling backdoor on port %s'
                % bdport)
            eventlet.spawn(backdoor.backdoor_server,
                eventlet.listen(('127.0.0.1', bdport)),
                locals={'server': self,
                        'debug': debug,
                        'exit': safe_exit(),
                        'quit': safe_exit(),
                        'stats': lambda: pprint.pprint(self.stats()),
                })
        app = create_application(self)
        dirs = self.config.server.get('static_directories', None)
        if dirs is not None:
            app = StaticFileServer(dirs.split(','), app, self)
        self.wsgiapp = app
        if start_listeners:
            numsimulreq = self.config.get(('http', 'max_simultaneous_reqs'))
            host = self.config.http.address
            port = self.config.http.getint('port')
            sock = eventlet.listen((host, port))
            pool = self.server_pool = eventlet.GreenPool(10000)
            log = (self.options.nohttp or self.options.statdump) and \
                dummylog() or None
            wsgi.server(sock, app, custom_pool=pool, log=log)

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
                'items': self._mqueue.qsize(),
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
            }
        })
        return stats


def start(config, options):
    if 'hub_module' in config.server:
        hubs.use_hub(config.server.import_('hub_module'))
    #from eventlet import patcher
    #patcher.monkey_patch(all=False, socket=True, select=True, os=True)
    server = Server(config, options)

    #def drop_to_shell(s, f):
        #from IPython.Shell import IPShell
        #s = IPShell([], {'server': server,
                         #'debug': debug,
                         #'stats': lambda: pprint.pprint(server.stats()),
                        #})
        #s.mainloop()
    #signal.signal(signal.SIGUSR2, drop_to_shell)

    if options.statdump:
        interval = options.statdump
        eventlet.spawn_after(interval, statdumper, server, interval)
    server.start()


if __name__ == '__main__':
    # included here for existing tools compatibility
    from drivel.startup import main
    main()
