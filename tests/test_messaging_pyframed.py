from eventlet.green import socket
from drivel.messaging.pyframed import Messaging

def test():
    a, b = socket.socketpair()
    ma = Messaging(a)
    mb = Messaging(b)
    assert ma.wait(0) is None
    mb.send(['foo'])
    ret = ma.wait()
    assert ret == ['foo'], ret
