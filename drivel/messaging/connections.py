import eventlet
from eventlet.green import select

from drivel.messaging.pyframed import Messaging

HEARTBEAT = 'heartbeat'


class Connections(object):
    def __init__(self, ownid):
        self.id = ownid
        self.sockets = []
        self.targets = {}

    def filter(self, msg):
        if msg == HEARTBEAT:
            return False
        return True

    def connect(self, (addr, port), target=None):
        sock = eventlet.connect((addr, port))
        self.add(sock, target)

    def add(self, sock, target=None):
        msgn = Messaging(sock)
        self.sockets.append(msgn)
        if target is not None:
            self.targets[target] = msgn

    def get(self):
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
        target = self.targets[to]  # raises KeyError
        self._send_to(target, data)

    def send_heartbeat(self):
        for sock in self.sockets.values():
            self._send_to(sock, HEARTBEAT)

    def _send_to(self, msgn, data):
        msgn.send((self.id, data))
