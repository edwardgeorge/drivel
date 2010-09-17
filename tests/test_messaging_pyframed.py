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


# EOF is an explicit situation we need to handle in code
# everything else will see the normal exceptions raised
# and we don't want to interfere with those at this level.
# just some simple tests below for sanity.

@tools.raises(IOError)
def test_closed_remote_send():
    # trigger an EPIPE
    a, b = socket.socketpair()
    ma = Messaging(a)
    mb = Messaging(b)

    # send a message for luck.
    ma.send('message')
    assert mb.wait() == 'message'
    b.close()
    ma.send('another message')


@tools.raises(IOError)
def test_closed_socket_wait():
    # trigger an EBADF
    a, b = socket.socketpair()
    ma = Messaging(a)
    mb = Messaging(b)

    # send a message for luck.
    mb.send('message')
    assert ma.wait() == 'message'
    a.close()
    ma.wait()


@tools.raises(IOError)
def test_closed_socket_wait():
    # trigger an EBADF
    a, b = socket.socketpair()
    ma = Messaging(a)
    mb = Messaging(b)

    # send a message for luck.
    ma.send('message')
    assert mb.wait() == 'message'
    a.close()
    ma.send('another message')
