from __future__ import with_statement
import errno
from functools import partial
import os
import re
import socket
import sys
import traceback
import weakref
# third-party imports
import eventlet
from eventlet import greenthread
from eventlet import hubs
from eventlet import wsgi
from eventlet.green import time
from webob import Request
# local imports
from drivel import component
from drivel.auth import UnauthenticatedUser
from drivel.utils import connwatch
from drivel.utils.importing import import_preferential
json = import_preferential('json', 'simplejson')


class TimeoutException(Exception):
    pass


class InvalidSession(Exception):
    pass


class ConnectionReplaced(Exception):
    pass


class ConnectionClosed(Exception):
    pass


class PathNotResolved(Exception):
    pass


class WSGIServer(object):
    def __init__(self, server, name,
            address='', port=0,
            maxconns=1000,
            config=None):
        self.server = server
        self.wsgiroutes = []
        self.app = self.application
        #self.app = self.respawn_linkable_middleware(self.app)
        self.app = self.error_middleware(self.app)
        self.authbackend = self.null_auth
        self.log = partial(server.log, 'WSGI:%s' % name)
        self.http_log = self.Logger()
        self.server_pool = self.ServerPool(maxconns)
        self.watch_connections = True
        self.watcher_pool = eventlet.GreenPool(maxconns)
        self._greenthread = None
        # config
        self.address = address
        self.port = port
        self.maxconns = maxconns
        self.timeout = 60
        if config is not None:
            self.configure(config)

    def stats(self):
        return {
            'free': self.server_pool.free(),
            'running': self.server_pool.running(),
            'waiting': self.server_pool.waiting(),
            'watchers': {
                'free': self.watcher_pool.free(),
                'running': self.watcher_pool.running(),
                'waiting': self.watcher_pool.waiting(),
            },
            'server_thread_running': bool(self._greenthread)
        }

    def configure(self, config):
        self.address = config.get('address')
        self.port = config.getint('port')
        self.maxconns = config.getint('max_conns', 10000)
        self.timeout = config.getint('maxwait')
        self.server_pool.resize(self.maxconns)
        self.watcher_pool.resize(self.maxconns)
        self.authbackend = config.import_('auth_backend')(self)
        return self

    class Logger(object):
        def __init__(self, logfunc=lambda data: None):
            self.write = logfunc

    class ServerPool(eventlet.GreenPool):
        # we want actual GreenThreads to link to...
        def spawn_n(self, *args, **kwargs):
            return self.spawn(*args, **kwargs)

    def start(self, sock=None, listen=eventlet.listen):
        if sock is not None:
            address, port = sock
        else:
            address, port = self.address, self.port
        sock = self.sock = listen((address, port))
        self.log('info', 'starting www server on %s:%s,' % (address, port))
        self._greenthread = eventlet.spawn(
            wsgi.server, sock, self.app,
            custom_pool=self.server_pool,
            log=self.http_log)
        return self

    def wait(self):
        if self._greenthread is None:
            raise Exception('server not started!')
        return self._greenthread.wait()

    def null_auth(self, request):
        return None

    def _path_to_subscriber(self, path):
        for s,k,r in self.wsgiroutes:
            match = r.search(path)
            if match:
                kw = match.groupdict()
                return s, k, kw
        raise PathNotResolved(path)

    def add_route(self, mapping, subscription):
        if not isinstance(mapping, (tuple, list)):
            mapping = (None, mapping)

        mapping = (subscription, mapping[0], re.compile(mapping[1]))
        self.wsgiroutes.append(mapping)

    def respawn_linkable_middleware(self, app):
        def middleware(environ, start_response):
            proc = eventlet.spawn(app, environ, start_response)
            return proc.wait()
        return application

    def error_middleware(self, app):
        def middleware(environ, start_response):
            try:
                return app(environ, start_response)
            except UnauthenticatedUser, e:
                self.log('debug', 'request cannot be authenticated')
                start_response('403 Forbidden', [
                        ('Content-type', 'text/html'),
                    ], sys.exc_info())
                return ['Could not be authenticated']
            except PathNotResolved, e:
                self.log('debug', 'no registered component for path %s' % (environ['PATH_INFO'], ))
                start_response('404 Not Found', [
                        ('Content-type', 'text/html'),
                    ], sys.exc_info())
                return ['404 Not Found']
            except Exception, e:
                self.log('error', 'an unexpected exception was raised: %s' % e)
                #log('error', 'traceback: %s' % traceback.format_exc())
                start_response('500 Internal Server Error', [
                        ('Content-type', 'text/html'),
                    ], sys.exc_info())
                return ['Server encountered an unhandled exception']
        return middleware

    def application(self, environ, start_response):
        rfile = getattr(environ['wsgi.input'], 'rfile', None)
        request = Request(environ)
        proc = weakref.ref(greenthread.getcurrent())
        body = str(request.body) if request.method == 'POST' else request.GET.get('body', '')
        watcher = None
        if rfile and self.watch_connections:
            watcher = connwatch.spawn_from_pool(self.watcher_pool,
                rfile, proc, ConnectionClosed, '')
        user = self.authbackend(request)
        subs, msg, kw = self._path_to_subscriber(request.path)
        try:
            with eventlet.Timeout(self.timeout):
                msgs = self.server.send(subs, msg, kw, user, request, proc).wait()
        except eventlet.Timeout, e:
            msgs = []
        except ConnectionClosed, e:
            msgs = []
        finally:
            if watcher:
                greenthread.kill(watcher)
        headers = [('Content-type', 'application/javascript'), ('Connection', 'close')]
        start_response('200 OK', headers)
        if 'jsonpcallback' in request.GET:
            msgs = '%s(%s)' % (request.GET['jsonpcallback'], json.dumps(msgs))
        elif not isinstance(msgs, basestring):
            msgs = json.dumps(msgs)
        return [msgs+'\r\n']

    def __call__(self, environ, start_response):
        return self.app(environ, start_response)
