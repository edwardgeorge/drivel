import uuid
import weakref

from eventlet.event import Event
from eventlet.semaphore import Semaphore

RETURN_SUB = '_return'


class RemoteEvent(object):
    def __init__(self, id, procid, publisher, semaphore):
        self.id = id
        self.procid = procid
        self.publisher = publisher
        self.pubsem = semaphore

    def send(self, result=None, exc=None):
        data = {'result': result, 'exc': exc}
        message = {
            'envelopeto': (self.procid, self.id),
            'data': data,
        }
        self.pubsem.acquire()
        self.publisher.send(self.procid, RETURN_SUB, message)
        self.pubsem.release()


class NullEvent(object):
    def send(self, *args, **kwargs):
        pass

    def ready(self):
        return False


class EventManager(object):
    def __init__(self, procid, publisher):
        self.events = weakref.WeakValueDictionary()
        self.procid = procid
        self.publisher = publisher
        self.pubsem = Semaphore(1)

    def create(self):
        id = uuid.uuid4().hex
        event = Event()
        self.events[id] = event
        event.id = id
        return event, id

    def getreturner(self, origin, id):
        return RemoteEvent(id, origin, self.publisher, self.pubsem)

    def returner_for(self, origin):
        if isinstance(origin, (list, tuple)):
            if origin[0] != self.procid:
                if origin[1] == 'null':
                    return NullEvent()
                return self.getreturner(origin[0], origin[1])
            origin = origin[1]
        if origin == 'null':
            return NullEvent()
        try:
            return self.events[origin]
        except KeyError:
            return NullEvent()

    def return_(self, id, message):
        if id in self.events:
            self.events[id].send(
                result=message.get('result'),
                exc=message.get('exc')
            )

    def __len__(self):
        return len(self.events)
