from __future__ import with_statement
import eventlet
from eventlet.event import Event
from nose import tools

from drivel.utils.contextmanagers import EventReady
from drivel.utils.contextmanagers import EventWatch


@tools.raises(EventReady)
def test():
    with EventWatch() as e:
        e.send(None)
        eventlet.sleep(1)
        raise Exception()


@tools.raises(EventReady)
def test_ready_event():
    e = Event()
    e.send(None)
    with EventWatch(e):
        raise Exception()


def test_proxy_removed_from_waiters():
    e = Event()
    w = EventWatch(e)
    with w as e:
        proxy = w.proxy
    assert proxy not in e._waiters


@tools.raises(EventReady)
def test_throw_is_immediate():
    # we want to throw immediate so no exception thrown can interrupt us
    # outside the context manager's with block.
    from eventlet import greenthread
    w = EventWatch()
    w.event.send(None)
    w._watcher(greenthread.getcurrent())
    # if the throw is immediate, we should never get here
    raise Exception('failed')
