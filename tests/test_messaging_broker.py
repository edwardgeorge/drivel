import eventlet
from eventlet.green import socket
from eventlet.queue import Queue

from drivel.messaging.broker import Broker


def test_broadcast_to_other():
    q = Queue(1)
    r, w = socket.socketpair()
    b1 = Broker('d', 'dummy1')
    b1.connections.add(r)
    b2 = Broker('d', 'dummy2')
    b2.connections.add(w)
    b2.subscribe('sub', q)
    eventlet.sleep(0)

    b1.send(b1.BROADCAST, 'sub', 'message')
    with eventlet.Timeout(1):
        e, data = q.get()
    assert data == 'message'


def test_remote_event():
    q = Queue(1)
    r, w = socket.socketpair()
    b1 = Broker('d', 'dummy1')
    b1.connections.add(r)
    b2 = Broker('d', 'dummy2')
    b2.connections.add(w)
    b2.subscribe('sub', q)

    event = b1.send(b1.BROADCAST, 'sub', 'ping')
    with eventlet.Timeout(1):
        e, message = q.get()
    e.send('pong')
    with eventlet.Timeout(1):
        msg = event.wait()
        assert msg == 'pong', msg


def test_subscription():
    b = Broker('d', 'dummy')
    q = eventlet.Queue()
    b.subscribe('sub', q)
    b.process_msg((None, None), 'sub', 'foo')
    evt, ret = q.get_nowait()
    assert ret == 'foo', ret
