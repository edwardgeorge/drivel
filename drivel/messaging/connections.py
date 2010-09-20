import errno
import os

import eventlet
from eventlet.event import Event
from eventlet.green import select

from drivel.messaging.pyframed import Messaging, EOF

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
                errno.errorcode[e], os.strerror(e))
        return 'EOF for fd %d' % self.sock.fileno()


class Connections(object):
    def __init__(self, ownid):
        self.id = ownid
        self.sockets = []
        self.listeners = []
        self.targets = {}
        self.get_ready = []
        self._event = Event()
        self.ALL = SEND_TO_ALL

    def filter(self, msg):
        if msg == HEARTBEAT:
            return False
        return True

    def disconnected(self, sock, errno=None):
        pass

    def listen(self, (addr, port)):
        sock = eventlet.listen((addr,port))
        self.listeners.append(sock)
        def listener(sock):
            while True:
                s, addr = sock.accept()
                print 'conn from', addr
                self.add(s)
        eventlet.spawn(listener, sock)
        return sock.fd.getsockname()

    def connect(self, (addr, port), target=None):
        sock = eventlet.connect((addr, port))
        self.add(sock, target)

    def add(self, sock, target=None):
        msgn = Messaging(sock)
        self._add(msgn, target)

    def _add(self, msgn, target=None):
        self.sockets.append(msgn)
        if target is not None:
            self.targets[target] = msgn
        if not self._event.ready():
            self._event.send(True)

    def _names_for_connection(self, conn):
        return [k for k,v in self.sockets.items() if v is conn]

    def alias(self, from_, to):
        self.targets[to] = self.targets[from_]

    def _select_for_read(self):
        try:
            return select.select(self.sockets, [], [])
        except ValueError, e:
            for i in self.sockets:
                if i.fileno() == -1:
                    self.sockets.remove(i)
            return select.select(self.sockets, [], [])

    def get(self):
        self._event.wait()
        while True:
            if self.get_ready:
                ready = [self.get_ready.pop(0)]
                if not ready[0].peek():
                    continue
            else:
                ready, _, _ = self._select_for_read()
            if ready:
                try:
                    sock = ready[0]
                    senderid, data = sock.wait()
                    self.targets[senderid] = sock
                    if sock.peek():
                        self.get_ready.append(sock)
                    if self.filter(data):
                        return senderid, data
                except EOF, e:
                    raise ConnectionError(sock, None, None)
                except IOError, e:
                    raise ConnectionError(sock, e.errno, None)

    def send(self, to, data):
        if to == self.ALL:
            for i in self.sockets:
                self._send_to(i, data)
        elif isinstance(to, (tuple, list)):
            for i in to:
                self.send(i, data)
        else:
            target = self.targets[to]  # raises KeyError
            self._send_to(target, data)

    def send_heartbeat(self):
        self.send_to_all(HEARTBEAT)

    def send_to_all(self, data):
        self.send(self.ALL, data)

    def _send_to(self, msgn, data):
        try:
            msgn.send((self.id, data))
        except IOError, e:
            raise ConnectionError(msgn, e.errno, None)

    def __len__(self):
        return len(self.sockets)
