from __future__ import with_statement
import errno
from functools import partial
import os
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


def _path_to_subscriber(routes, path):
    for s, k, r in routes:
        match = r.search(path)
        if match:
            kw = match.groupdict()
            return s, k, kw
    raise PathNotResolved(path)


def dothrow(gt, cgt):
    hubs.get_hub().schedule_call_local(0,
        greenthread.getcurrent().switch)
    cgt.throw()


def create_application(server):
    from components.session import SessionConflict  # circular import
    authbackend = server.config.http.import_('auth_backend')(server)
    tsecs = server.config.http.getint('maxwait')
    log = partial(server.log, 'WSGI')

    # error handling
    def error_middleware(app):
        def application(environ, start_response):
            try:
                return app(environ, start_response)
            except UnauthenticatedUser, e:
                log('debug', 'request cannot be authenticated')
                start_response('403 Forbidden', [
                        ('Content-type', 'text/html'),
                    ], exc_info=sys.exc_info())
                return ['Could not be authenticated']
            except PathNotResolved, e:
                log('debug', 'no registered component for path %s' %
                    (environ['PATH_INFO'], ))
                start_response('404 Not Found', [
                        ('Content-type', 'text/html'),
                    ], exc_info=sys.exc_info())
                return ['404 Not Found']
            except Exception, e:
                log('exception', 'an unexpected exception was raised: %s' % e)
                #log('error', 'traceback: %s' % traceback.format_exc())
                start_response('500 Internal Server Error', [
                        ('Content-type', 'text/html'),
                    ], exc_info=sys.exc_info())
                return ['Server encountered an unhandled exception']
        return application

    # coroutine stuff
    def linkablecoroutine_middleware(app):
        def application(environ, start_response):
            """run application in an coroutine that we can link and pass
            to application via wsgi environ so that it can use it.

            """
            proc = eventlet.spawn(app, environ, start_response)
            #environ['drivel.wsgi_proc'] = weakref.ref(proc)
            return proc.wait()
        return application

    def watchconnection(sock, proc):
        """listen for EOF on socket."""
        def watcher(sock, proc):
            fileno = "%s" % (sock.fileno(),)
            if proc():
                proc().link(dothrow, greenthread.getcurrent())
            else:
                return
            try:
                log('debug', 'watching connection %s for termination'
                    ' at client end' % fileno)
                hubs.trampoline(sock, read=True)
                d = sock.read()
                if not d and bool(proc()):
                    log('debug', 'terminating wsgi proc using closed sock %s' %
                        fileno)
                    greenthread.kill(proc(), ConnectionClosed())
            except socket.error, e:
                if e[0] == errno.EPIPE:
                    log('debug', 'got broken pipe on sock %s. terminating.' %
                        fileno)
                    if proc() is not None:
                        greenthread.kill(proc(), ConnectionClosed())
                else:
                    log('debug', 'got error %s for sock %' % (e, fileno))
            except IOError, e:
                if e.errno == errno.EPIPE:
                    log('debug', 'got broken pipe on sock %s. terminating.' %
                        fileno)
                    if proc() is not None:
                        greenthread.kill(proc(), ConnectionClosed())
                else:
                    log('debug', 'got error %s for sock %' % (e, fileno))
            #except LinkedExited, e:
                #pass
        g = eventlet.spawn(watcher, sock, proc)
        #proc.link(g)

    def access_control(request):
        origins = server.config.get('access-control-origins', {})
        if 'Origin' in request.headers:
            for key, origin in origins.items():
                if origin == request.headers['Origin']:
                    return [('Access-Control-Allow-Origin',
                        request.headers['Origin'])]
        return []

    # the actual wsgi app
    def application(environ, start_response):
        # webob can change this, so get it now!
        rfile = getattr(environ['wsgi.input'], 'rfile', None)
        request = Request(environ)
        #proc = environ['drivel.wsgi_proc']
        proc = weakref.ref(greenthread.getcurrent())
        if request.method == 'OPTIONS' and 'Origin' in request.headers and \
                'Access-Control-Request-Method' in request.headers:
            headers = access_control(request)
            if headers:
                headers.extend([('Access-Control-Max-Age', 1728000),
                     ('Access-Control-Allow-Methods', 'GET, POST')])
                if request.headers.get('Access-Control-Request-Headers', None):
                    headers.append(('Access-Control-Allow-Headers',
                        request.headers['Access-Control-Request-Headers']))
            start_response('200 OK', headers)
            return ['']
        if request.method not in ['GET', 'POST']:
            start_response('405 Method Not Allowed', [('Allow', 'GET, POST')])
            return ['']
        elif request.path == '/ping':
            start_response('200 OK', [('Content-type', 'text/plain')])
            return ['pong']
        elif request.path == '/favicon.ico':
            start_response('404 Not Found', [])
            return ['']
        user = authbackend(request)
        path = request.path.strip('/').split('/')
        log('debug', 'path: %s from: %s' % (request.path,
            request.headers['user-agent']))
        body = (str(request.body) if request.method == 'POST' else
            request.GET.get('body', ''))

        try:
            timeoutstarttime = time.time()
            timeouttimer = timeout.Timeout(tsecs, TimeoutException())
            if rfile:
                watchconnection(rfile, proc)
            sub, msg, kw = _path_to_subscriber(server.wsgiroutes, request.path)
            msgs = server.send(sub, msg, kw, user, request, proc).wait()
        except TimeoutException, e:
            log('debug', 'timeout reached for user %s after %ds' % (user,
                (time.time() - timeoutstarttime)))
            msgs = []
        except ConnectionClosed, e:
            log('debug', 'connection closed for user %s' % user)
            msgs = []
        finally:
            timeouttimer.cancel()
        # do response
        log('debug', 'got messages %s for user %s' % (msgs, user))
        headers = [('Content-type', 'application/javascript'),
            ('Connection', 'close')]
        headers.extend(access_control(request))
        start_response('200 OK', headers)
        if 'jsonpcallback' in request.GET:
            msgs = '%s(%s)' % (request.GET['jsonpcallback'], json.dumps(msgs))
        elif not isinstance(msgs, basestring):
            msgs = json.dumps(msgs)

        return [msgs + '\r\n']

    app = error_middleware(application)
    app = linkablecoroutine_middleware(app)
    return app


class WSGIServer(object):
    def __init__(self, server, name, config):
        self.server = server
        self.wsgi_config = config
        self.wsgiroutes = []
        self.app = self.application
        #self.app = self.respawn_linkable_middleware(self.app)
        self.app = self.error_middleware(self.app)
        self.log = partial(server.log, 'WSGI:%s' % name)
        self.http_log = self.Logger()
        if config:
            self.configure(config)

    def configure(self, config):
        self.address = config.get('address')
        self.port = config.getint('port')
        self.maxconns = config.getint('max_conns', 10000)

    class Logger(object):
        def __init__(self, logfunc=lambda data: None):
            self.write = logfunc

    def start(self, listen=eventlet.listen):
        pool = eventlet.GreenPool(self.maxconns)
        pool.spawn_n = pool.spawn  # we want actual GreenThreads to link to
        sock = listen((self.address, self.port))
        self._greenthread = eventlet.spawn(
            wsgi.server,
            sock,
            self.app,
            custom_pool=pool,
            log=self.http_log)

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
        def middlware(environ, start_response):
            try:
                return app(environ, start_response)
            except UnauthenticatedUser, e:
                self.log('debug', 'request cannot be authenticated')
                start_response('403 Forbidden', [
                        ('Content-type', 'text/html'),
                    ], exc_info=sys.exc_info())
                return ['Could not be authenticated']
            except PathNotResolved, e:
                self.log('debug', 'no registered component for path %s' % (environ['PATH_INFO'], ))
                start_response('404 Not Found', [
                        ('Content-type', 'text/html'),
                    ], exc_info=sys.exc_info())
                return ['404 Not Found']
            except Exception, e:
                self.log('error', 'an unexpected exception was raised: %s' % e)
                #log('error', 'traceback: %s' % traceback.format_exc())
                start_response('500 Internal Server Error', [
                        ('Content-type', 'text/html'),
                    ], exc_info=sys.exc_info())
                return ['Server encountered an unhandled exception']
        return middleware

    def application(self, environ, start_response):
        rfile = getattr(environ['wsgi.input'], 'rfile', None)
        request = Request(environ)
        proc = weakref.ref(greenthread.getcurrent())
        body = str(request.body) if request.method == 'POST' else request.GET.get('body', '')
        watcher = None
        if rfile:
            watcher = connwatch.spawn(rfile, proc, ConnectionClosed, '')
        subs, msg, kw = self._path_to_subscriber(server.wsgiroutes, request.path)
        try:
            with eventlet.Timeout(tsecs):
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
            msgs = '%s(%s)' % (request.GET['jsonpcallback'], simplejson.dumps(msgs))
        elif not isinstance(msgs, basestring):
            msgs = simplejson.dumps(msgs)
        return [msgs+'\r\n']

    def __call__(self, environ, start_response):
        return self.app(environ, start_response)
