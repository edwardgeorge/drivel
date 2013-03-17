from __future__ import with_statement
import errno
import logging
import os
import weakref

import eventlet
from eventlet.event import Event
from eventlet import greenio
from eventlet.green import select
from eventlet import hubs

from drivel.messaging.pyframed import Messaging, EOF
from drivel.utils.contextmanagers import EventWatch, EventReady

logger = logging.getLogger(__name__)

HEARTBEAT = 'heartbeat'
SEND_TO_ALL = '*'


class ConnectionError(Exception):
    def __init__(self, sock, errno, aliases):
        self.sock = sock
        self.errno = errno
        self.aliases = aliases

    def __str__(self):
        if self.errno is not None:
            e = self.errno
            return "fd %d [%d %s]: %s" % (self.sock.fileno(), e,
                                          errno.errorcode[e],
                                          os.strerror(e))
        return 'EOF for fd %d' % self.sock.fileno()


class ConnectionClosed(ConnectionError):
    def __init__(self, sock, aliases):
        super(ConnectionClosed, self).__init__(sock, None, aliases)


class Connections(object):
    def __init__(self, name, ownid):
        self.name = name
        self.id = ownid
        self._sockets = []
        self._listeners = []
        self._fd_listeners = {}
        self._targets = {}
        self._dhandlers = []
        self._chandlers = []
        self._get_ready = []
        self._event = Event()
        self._update_event = Event()
        self.ALL = SEND_TO_ALL

    # public api

    def listen(self, (addr, port)):
        sock = eventlet.listen((addr, port))
        sockname = sock.getsockname()
        logger.info('listening on %s:%d' % sockname)
        self._setup_listener(sock)
        return sockname

    def connect(self, (addr, port), target=None):
        sock = eventlet.connect((addr, port))
        self.add(sock, target)

    def send(self, to, data):
        if to == self.ALL or to is None:
            for i in self._sockets:
                try:
                    self._send_to(i, data)
                except ConnectionError:
                    pass
        elif isinstance(to, (tuple, list)):
            for i in to:
                try:
                    self.send(i, data)
                except ConnectionError:
                    pass
        else:
            for target in list(self._targets[to]):  # raises KeyError
                try:
                    self._send_to(target, data)
                except ConnectionError:
                    pass

    def send_heartbeat(self):
        self.send_to_all(HEARTBEAT)

    def send_to_all(self, data):
        self.send(self.ALL, data)

    def add_connect_handler(self, handler):
        self._chandlers.append(handler)

    def add_disconnect_handler(self, handler):
        self._dhandlers.append(handler)

    def stop_listening(self):
        addresses = []
        listeners = self._listeners
        fd_listeners = self._fd_listeners
        self._listeners = []
        self._fd_listeners = {}
        hub = hubs.get_hub()
        for sock, hublistener in fd_listeners.itervalues():
            addresses.append(sock.getsockname())
            listeners.remove(sock)
            hub.remove(hublistener)
            try:
                sock.close()
            except IOError:
                pass
        return addresses

    def shutdown(self):
        sockets = self._sockets
        self._sockets = []
        for sock in sockets:
            try:
                sock.shutdown()
            except IOError:
                pass

    # internals

    def add(self, sock, target=None):
        msgn = Messaging(sock)
        self._add(msgn, target)

    def filter(self, msg):
        if msg == HEARTBEAT:
            return False
        return True

    def alias(self, from_, to):
        self._register_target(to, self._targets[from_])

    # data model

    def __len__(self):
        return len(self._sockets)

    # private

    def _register_target(self, name, sock):
        self._targets.setdefault(name, set()).add(sock)

    def _fire_handlers(self, handlers, *args, **kwargs):
        for handler in handlers[:]:
            if isinstance(handler, weakref.ref):
                _handler = handler
                handler = handler()
                if handler is None:
                    handlers.remove(_handler)
                    continue
            eventlet.spawn(handler, *args, **kwargs)

    def _disconnected(self, sock, errno=None, data_to_send=None):
        self._sockets.remove(sock)
        aliases = self._names_for_connection(sock)
        for a in aliases:
            try:
                self._targets[a].remove(sock)
            except KeyError:
                pass
        self._fire_handlers(self._dhandlers,
                            sock, errno, aliases, data_to_send)

    def _setup_listener(self, sock):
        self._listeners.append(sock)
        hub = hubs.get_hub()
        fd = sock.fileno()
        hublistener = hub.add(hub.READ, fd, self._fd_connect)
        self._fd_listeners[fd] = (sock, hublistener)

    def _fd_connect(self, fd):
        sock, hublistener = self._fd_listeners[fd]
        res = greenio.socket_connect(sock.fd)
        if res is None:
            return
        connsock, addr = res
        logger.info('connection from %s:%d' % addr)
        self.add(connsock)
        self._fire_handlers(self._chandlers, connsock, addr)

    def _add(self, msgn, target=None):
        self._sockets.append(msgn)
        if target is not None:
            self._register_target(target, msgn)
        if not self._event.ready():
            self._event.send(True)
        if not self._update_event.ready():
            self._update_event.send(True)

    def _names_for_connection(self, conn):
        return [k for k, v in self._targets.items() if conn in v]

    def _select_for_read(self, sockets, timeout=None):
        try:
            return select.select(sockets, [], [], timeout=timeout)
        except ValueError:
            for i in self._sockets:
                if i.fileno() == -1:
                    self._sockets.remove(i)
            return select.select(sockets, [], [], timeout=timeout)

    def get(self):
        self._event.wait()
        while True:
            try:
                if self._get_ready:
                    ready = [self._get_ready.pop(0)]
                    if not ready[0].peek():
                        #continue
                        pass
                else:
                    if self._update_event.ready():
                        self._update_event = Event()
                    with EventWatch(self._update_event):
                        ready, _, _ = self._select_for_read(self._sockets)
                if ready:
                    return self._do_get_from_sock(ready[0])
            except EventReady:
                pass

    def _do_get_from_sock(self, sock):
        try:
            name, senderid, data = sock.wait()
            self._register_target(name, sock)
            self._register_target(senderid, sock)
            if sock.peek():
                self._get_ready.append(sock)
            if self.filter(data):
                return senderid, data
        except EOF, e:
            self._disconnected(sock, None)
            raise ConnectionClosed(sock, None)
        except IOError, e:
            self._disconnected(sock, e.errno)
            raise ConnectionError(sock, e.errno, None)

    def _send_to(self, msgn, data):
        try:
            msgn.send_concurrent((self.name, self.id, data))
        except IOError, e:
            self._disconnected(msgn, e.errno)
            raise ConnectionError(msgn, e.errno, None)
