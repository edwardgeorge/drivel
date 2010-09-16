import eventlet
from eventlet.green import socket
import mock

from drivel.messaging.connections import Connections
from drivel.messaging.connections import HEARTBEAT


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

def test_heartbeat():
    conns = [mock.Mock() for i in range(10)]
    c = Connections('dummy')
    for i in conns:
        c._add(i, id(i))

    c.send_heartbeat()
    for i in conns:
        assert i.send.called
        i.send.assert_called_with(('dummy', HEARTBEAT))

def test_send_to_all():
    conns = [mock.Mock() for i in range(10)]
    c = Connections('dummy')
    for i in conns:
        c._add(i, id(i))

    c.send_heartbeat()
    for i in conns:
        assert i.send.called
        i.send.assert_called_with(('dummy', HEARTBEAT))
