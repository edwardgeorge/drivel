import functools
import uuid
import weakref

from eventlet.event import Event
from eventlet.semaphore import Semaphore

class remoteevent(object):
    def __init__(self, id, procid, publisher, semaphore):
        self.id = id
        self.procid = procid
        self.publisher = publisher
        self.pubsem = semaphore

    def send(self, result=None, exc=None):
        data = {'result': result, 'exc': exc}
        message = {
            'envelopeto': self.id,
            'data': data,
        }
        self.pubsem.acquire()
        self.publisher.send('_return', message)
        self.pubsem.release()

class EventManager(object):
    def __init__(self, procid, publisher):
        self.events = {}
        self.procid = procid
        self.publisher = publisher
        self.pubsem = Semaphore(1)

    def _remove_event(self, id, val):
        if self.events[id] is val:
            del self.events[id]

    def create(self):
        id = uuid.uuid4().hex
        remove = functools.partial(self._remove_event, id)
        event = Event()
        self.events[id] = weakref.proxy(event, remove)
        event.id = id
        return event, id

    def getreturner(self, origin, id):
        return remoteevent(id, origin, self.publisher, self.pubsem)

    def return_(self, id, message):
        if id in self.events:
            self.events[id].send(
                result=message.get('result'),
                exc=message.get('exc')
            )
