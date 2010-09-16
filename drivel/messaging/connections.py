import eventlet
from eventlet.event import Event
from eventlet.green import select

from drivel.messaging.pyframed import Messaging

HEARTBEAT = 'heartbeat'
SEND_TO_ALL = '*'


class Connections(object):
    def __init__(self, ownid):
        self.id = ownid
        self.sockets = []
        self.listeners = []
        self.targets = {}
        self._event = Event()
        self.ALL = SEND_TO_ALL

    def filter(self, msg):
        if msg == HEARTBEAT:
            return False
        return True

    def listen(self, (addr, port)):
        sock = eventlet.listen((addr,port))
        self.listeners.append(sock)
        def listener(sock):
            while True:
                s, addr = sock.accept()
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

    def alias(self, from_, to):
        self.targets[to] = self.targets[from_]

    def get(self):
        self._event.wait()
        socks = self.sockets
        while True:
            ready, _, _ = select.select(socks, [], [])
            if ready:
                sock = ready[0]
                senderid, data = sock.wait()
                self.targets[senderid] = sock
                if self.filter(data):
                    return senderid, data

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
        msgn.send((self.id, data))

    def __len__(self):
        return len(self.sockets)
