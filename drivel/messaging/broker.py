# moving the broker aspect from the server for easy refactoring.
import eventlet

from drivel.event import EventManager, RETURN_SUB
from drivel.messaging.connections import Connections


class Broker(object):
    def __init__(self, id):
        self.id = id
        self._mqueue = eventlet.Queue()
        self.events = EventManager(id, self)
        self.connections = Connections(id)
        self.subscriptions = {}
        self.BROADCAST = self.connections.ALL

    def subscribe(self, key, queue):
        self.subscriptions[key] = queue

    def unsubscribe(self, key):
        self.subscriptions.pop(key, None)

    def process(self):
        while True:
            try:
                self.process_one()
            except Exception, e:
                print 'error', e

    def process_one(self):
        eventid, subscription, message = self._mqueue.get()
        self.process_msg(eventid, subscription, message)

    def process_msg(self, eventid, subscription, message):
        event = self.events.returner_for(eventid)
        if subscription == RETURN_SUB:
            event = self.events.returner_for(message['envelopeto'])
            event.send(**message['data'])
        elif subscription in self.subscriptions:
            self.subscriptions[subscription].put((event, message))
        else:
            pass

    def listen(self):
        while True:
            try:
                self.listen_one()
            except Exception, e:
                print 'error', e

    def listen_one(self, enqueue=True):
        senderid, (eid, sub, msg) = self.connections.get()
        eventid = (senderid, eid)
        if enqueue:
            self._mqueue.put((eventid, sub, msg))
        else:
            return eventid, sub, msg

    def stats(self):
        return {
            'events': len(self.events),
            'connections': len(self.connections),
            'messages': self._mqueue.qsize(),
            'subscriptions': len(self.subscriptions),
        }

    def send(self, to, subscription, message):
        event, eventid = self.events.create()
        msg = (eventid, subscription, message)
        if to != self.id:
            self.connections.send(to, msg)
        else:
            self._mqueue.put(msg)
        return event