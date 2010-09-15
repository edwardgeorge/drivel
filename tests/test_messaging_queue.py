from eventlet.green import socket
from drivel.messaging.queue import Messaging

def test():
    ma = Messaging()
    assert ma.wait(0) is None
    ma.send(['foo'])
    ret = ma.wait()
    assert ret == ['foo'], ret
