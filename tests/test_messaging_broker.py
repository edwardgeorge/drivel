import eventlet
from eventlet.green import socket

from drivel.messaging.broker import Broker

def test_broadcast_to_other():
    r, w = socket.socketpair()
    b1 = Broker('dummy1')
    b1.connections.add(r)
    b2 = Broker('dummy2')
    b2.connections.add(w)

    b1.send(b1.BROADCAST, 'sub', 'message')
    ret = b2.listen_one(enqueue=False)
    (bid, eid), sub, message = ret
    assert bid == 'dummy1'
    assert sub == 'sub'
    assert message == 'message'

def test_remote_event():
    r, w = socket.socketpair()
    b1 = Broker('dummy1')
    b1.connections.add(r)
    b2 = Broker('dummy2')
    b2.connections.add(w)

    event = b1.send(b1.BROADCAST, 'sub', 'ping')
    ret = b2.listen_one(enqueue=False)
    retevt = b2.events.returner_for(ret[0])
    retevt.send('pong')
    with eventlet.Timeout(1):
        b1.listen_one()
        b1.process_one()
        msg = event.wait()
        assert msg == 'pong', msg
