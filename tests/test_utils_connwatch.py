import weakref

import eventlet
from eventlet import hubs
from eventlet.green import socket
from nose import tools

from drivel.utils import connwatch


def dummythread():
    hubs.get_hub().switch()


class TestException(Exception):
    pass


@tools.raises(TestException)
def test_connectionwatcher():
    r, w = socket.socketpair() #socket.AF_INET, socket.SOCK_STREAM)
    g = eventlet.spawn(dummythread)
    connwatch.spawn(r, weakref.ref(g), TestException)
    w.close()
    with eventlet.Timeout(1):
        g.wait()


@tools.raises(TestException)
def test_connectionwatcher_file():
    r, w = socket.socketpair() #socket.AF_INET, socket.SOCK_STREAM)
    g = eventlet.spawn(dummythread)
    rf = r.makefile('r')
    connwatch.spawn(rf, weakref.ref(g), TestException)
    w.close()
    with eventlet.Timeout(1):
        g.wait()
