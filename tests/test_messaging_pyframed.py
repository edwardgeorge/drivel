from nose import tools

from eventlet.green import socket
from drivel.messaging.pyframed import EOF
from drivel.messaging.pyframed import Messaging


def test():
    a, b = socket.socketpair()
    ma = Messaging(a)
    mb = Messaging(b)
    assert ma.wait(0) is None
    mb.send(['foo'])
    ret = ma.wait()
    assert ret == ['foo'], ret


@tools.raises(EOF)
def test_closed_remote_wait():
    a, b = socket.socketpair()
    ma = Messaging(a)
    mb = Messaging(b)

    # send a message for luck.
    mb.send('message')
    assert ma.wait() == 'message'
    b.close()
    ma.wait()
