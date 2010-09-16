import eventlet
from eventlet.green import socket

from drivel.messaging.connections import Connections


def test():
    c1 = Connections('dummy1')
    c2 = Connections('dummy2')
    r, w = socket.socketpair()
    c1.add(r, 'dummy2')
    c2.add(w)

    c1.send('dummy2', 'test message')
    with eventlet.Timeout(1):
        sid, ret = c2.get()
    assert sid == 'dummy1'
    assert ret == 'test message'

    c2.send('dummy1', 'back atcha')
    with eventlet.Timeout(1):
        sid, ret = c1.get()
    assert sid == 'dummy2'
    assert ret == 'back atcha'
