from __future__ import with_statement
import errno
import logging
import os
import weakref

import eventlet
from eventlet.event import Event
from eventlet.green import select

from drivel.messaging.pyframed import Messaging, EOF
from drivel.utils.contextmanagers import EventWatch, EventReady

Logger = logging.getLogger

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
        self.sockets = []
        self.listeners = []
        self.targets = {}
        self.dhandlers = []
        self.chandlers = []
        self.get_ready = []
        self._event = Event()
        self.update_event = Event()
        self.update_listeners_event = Event()
        self._listener_gt = None
        self.ALL = SEND_TO_ALL

    def filter(self, msg):
        if msg == HEARTBEAT:
            return False
        return True

    def register_target(self, name, sock):
        self.targets.setdefault(name, set()).add(sock)

    def add_disconnect_handler(self, handler):
        self.dhandlers.append(handler)

    def add_connect_handler(self, handler):
        self.chandlers.append(handler)

    def _fire_handlers(self, handlers, *args, **kwargs):
        for handler in handlers[:]:
            if isinstance(handler, weakref.ref):
                _handler = handler
                handler = handler()
                if handler is None:
                    handlers.remove(_handler)
                    continue
            eventlet.spawn(handler, *args, **kwargs)

    def disconnected(self, sock, errno=None, data_to_send=None):
        self.sockets.remove(sock)
        aliases = self._names_for_connection(sock)
        for a in aliases:
            try:
                self.targets[a].remove(sock)
            except KeyError:
                pass
        self._fire_handlers(self.dhandlers,
                            sock, errno, aliases, data_to_send)

    def _listener(self):
        logger = Logger('drivel.messaging.connections.Connections._listener')
        while True:
            try:
                if self.update_listeners_event.ready():
                    self.update_listeners_event = Event()
                with EventWatch(self.update_listeners_event):
                    r, _, _ = self._select_for_read(self.listeners)
                if r:
                    s, addr = r[0].accept()
                    logger.info('connection from %s:%d' % addr)
                    self.add(s)
                    self._fire_handlers(self.chandlers, s, addr)
            except EventReady:
                pass

    def stop_listening(self):
        if self._listener_gt is not None:
            self._listener_gt.kill()
        listeners = self.listeners
        addresses = []
        self.listeners = []
        for s in listeners:
            addresses.append(s.getsockname())
            try:
                s.close()
            except IOError:
                pass
        return addresses

    def listen(self, (addr, port)):
        logger = Logger('drivel.messaging.connections.Connections.listen')
        sock = eventlet.listen((addr, port))
        sockname = sock.getsockname()
        logger.info('listening on %s:%d' % sockname)
        self.listeners.append(sock)

        if self._listener_gt is None or self._listener_gt.dead:
            self._listener_gt = eventlet.spawn(self._listener)
        return sockname

    def connect(self, (addr, port), target=None):
        sock = eventlet.connect((addr, port))
        self.add(sock, target)

    def add(self, sock, target=None):
        msgn = Messaging(sock)
        self._add(msgn, target)

    def _add(self, msgn, target=None):
        self.sockets.append(msgn)
        if target is not None:
            self.register_target(target, msgn)
        if not self._event.ready():
            self._event.send(True)
        if not self.update_event.ready():
            self.update_event.send(True)

    def _names_for_connection(self, conn):
        return [k for k, v in self.targets.items() if conn in v]

    def alias(self, from_, to):
        self.register_target(to, self.targets[from_])

    def _select_for_read(self, sockets, timeout=None):
        try:
            return select.select(sockets, [], [], timeout=timeout)
        except ValueError:
            for i in self.sockets:
                if i.fileno() == -1:
                    self.sockets.remove(i)
            return select.select(sockets, [], [], timeout=timeout)

    def get(self):
        self._event.wait()
        while True:
            try:
                if self.get_ready:
                    ready = [self.get_ready.pop(0)]
                    if not ready[0].peek():
                        #continue
                        pass
                else:
                    if self.update_event.ready():
                        self.update_event = Event()
                    with EventWatch(self.update_event):
                        ready, _, _ = self._select_for_read(self.sockets)
                if ready:
                    return self._do_get_from_sock(ready[0])
            except EventReady:
                pass

    def _do_get_from_sock(self, sock):
        try:
            name, senderid, data = sock.wait()
            self.register_target(name, sock)
            self.register_target(senderid, sock)
            if sock.peek():
                self.get_ready.append(sock)
            if self.filter(data):
                return senderid, data
        except EOF, e:
            self.disconnected(sock, None)
            raise ConnectionClosed(sock, None)
        except IOError, e:
            self.disconnected(sock, e.errno)
            raise ConnectionError(sock, e.errno, None)

    def send(self, to, data):
        if to == self.ALL or to is None:
            for i in self.sockets:
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
            for target in list(self.targets[to]):  # raises KeyError
                try:
                    self._send_to(target, data)
                except ConnectionError:
                    pass

    def send_heartbeat(self):
        self.send_to_all(HEARTBEAT)

    def send_to_all(self, data):
        self.send(self.ALL, data)

    def _send_to(self, msgn, data):
        try:
            msgn.send_concurrent((self.name, self.id, data))
        except IOError, e:
            self.disconnected(msgn, e.errno)
            raise ConnectionError(msgn, e.errno, None)

    def __len__(self):
        return len(self.sockets)
