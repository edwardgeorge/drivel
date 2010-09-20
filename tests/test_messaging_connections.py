import eventlet
from eventlet.green import socket
import mock
from nose import tools

from drivel.messaging.connections import ConnectionClosed
from drivel.messaging.connections import ConnectionError
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

    c.send_to_all('foo')
    for i in conns:
        assert i.send.called
        i.send.assert_called_with(('dummy', 'foo'))

def test_send_to_many():
    conns = [(i, mock.Mock()) for i in range(10)]
    c = Connections('dummy')
    for target, sock in conns:
        c._add(sock, target)

    targets = [2, 3, 5, 9]
    c.send(targets, 'foo')
    for i, sock in conns:
        assert sock.send.called == (i in targets)

def test_server_sockets():
    c1 = Connections('server')
    c2 = Connections('client')

    addr, port = c1.listen(('127.0.0.1', 0))
    c2.connect((addr, port), 'server')
    c2.connect((addr, port), 'server2')
    c2.send('server', 'foo')
    with eventlet.Timeout(1):
        sid, data = c1.get()
    assert data == 'foo', data

def test_multiple_messages():
    c1 = Connections('server')
    c2 = Connections('client')

    addr, port = c1.listen(('127.0.0.1', 0))
    c2.connect((addr, port), 'server')
    c2.send('server', 'foo')
    c2.send('server', 'foo')
    c2.send('server', 'foo')
    with eventlet.Timeout(1):
        sid, data = c1.get()
        sid, data = c1.get()
        sid, data = c1.get()

@tools.raises(ConnectionClosed)
def test_EOF_on_connection():
    sock = eventlet.listen(('127.0.0.1', 0))
    addr, port = sock.fd.getsockname()
    c = Connections('dummy')
    c.connect((addr, port), 'remote')
    sock.close()
    c.get()

@tools.raises(ConnectionError)
def test_EPIPE_on_connection():
    a, b = socket.socketpair()
    c = Connections('dummy')
    c.add(a, 'remote')
    b.close()
    c.send('remote', 'message')

def test_EBADF_on_connection():
    a, b = socket.socketpair()
    c = Connections('dummy')
    c.add(a, 'remote')
    a.close()
    # the bad file-descriptor is silently removed so we
    # will block here on this timeout
    with eventlet.Timeout(0.1):
        tools.assert_raises(eventlet.Timeout, c.get)
    assert len(c.sockets) == 0

def test_updated_sockets_during_select():
    a, b = socket.socketpair()
    c = Connections('dummy')
    c.add(a, 'remote')
    def updater(c):
        a, b = socket.socketpair()
        c.add(a, 'new')
        d = Connections('remote_side')
        d.add(b, 'target')
        d.send('target', 'message')
    eventlet.spawn(updater, c)
    with eventlet.Timeout(1):
        c.get()
