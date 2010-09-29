import eventlet
from eventlet import greenthread
from eventlet.event import Event

class EventReady(Exception):
    pass


class EventWatch(object):
    def __init__(self, event=None):
        if event is None:
            event = Event()
        self.event = event

    def _watcher(self, ret_thread):
        self.event.wait()
        ret_thread.throw(EventReady())

    def __enter__(self):
        if self.event.ready():
            raise EventReady()
        cgt = greenthread.getcurrent()
        self._g = eventlet.spawn(self._watcher, cgt)
        return self.event

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._g.kill()
        del self._g
        return False
