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

def test_for_gt_leak():
    import gc
    import weakref
    g = None
    w = EventWatch()
    with w as e:
        g = weakref.ref(w._g)
    # check greenthread is dead
    assert not bool(g())
    gc.collect()
    # check it's deleted
    assert g() is None
