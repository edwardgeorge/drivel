import eventlet
from eventlet import greenthread
from eventlet.event import Event


class EventReady(Exception):
    pass


class _GreenThreadProxy(object):
    def __init__(self, greenthread):
        self.greenthread = greenthread

    def switch(self, *a, **k):
        self.greenthread.throw(EventReady())

    def throw(self, *a, **k):
        self.switch()


class EventWatch(object):
    def __init__(self, event=None):
        if event is None:
            event = Event()
        self.event = event
        self.proxy = None

    def _watcher(self, ret_thread):
        self.event.wait()
        ret_thread.throw(EventReady())

    def __enter__(self):
        if self.event.ready():
            raise EventReady()
        cgt = greenthread.getcurrent()
        self.proxy = _GreenThreadProxy(cgt)
        self.event._waiters.add(self.proxy)
        return self.event

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.event._waiters.remove(self.proxy)
        self.proxy = None
        return False
