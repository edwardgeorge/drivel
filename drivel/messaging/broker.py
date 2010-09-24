# moving the broker aspect from the server for easy refactoring.
import eventlet
import logging

from drivel.event import EventManager, RETURN_SUB
from drivel.messaging.connections import Connections

Logger = logging.getLogger


class Broker(object):
    def __init__(self, name, id):
        self.id = id
        self._mqueue = eventlet.Queue()
        self.events = EventManager(id, self)
        self.connections = Connections(name, id)
        self.subscriptions = {}
        self.BROADCAST = self.connections.ALL

    def start(self):
        eventlet.spawn(self.process)
        eventlet.spawn(self.listen)

    def subscribe(self, key, queue):
        self.subscriptions[key] = queue

    def unsubscribe(self, key):
        self.subscriptions.pop(key, None)

    def process(self):
        logger = Logger('drivel.messaging.broker.Broker.process')
        while True:
            try:
                self.process_one()
            except Exception, e:
                logger.error('error in process', e)

    def process_one(self):
        eventid, subscription, message = self._mqueue.get()
        self.process_msg(eventid, subscription, message)

    def process_msg(self, eventid, subscription, message):
        event = self.events.returner_for(eventid)
        if subscription == RETURN_SUB:
            event = self.events.returner_for(message['envelopeto'])
            if not event.ready():
                event.send(**message['data'])
        elif subscription in self.subscriptions:
            self.subscriptions[subscription].put((event, message))
        else:
            pass

    def listen(self):
        logger = Logger('drivel.messaging.broker.Broker.listen')
        while True:
            try:
                self.listen_one()
            except Exception, e:
                logger.error('error in listen', e)

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
        logger = Logger('drivel.messaging.broker.Broker.send')
        event, eventid = self.events.create()
        msg = (eventid, subscription, message)
        if to is None:
            if subscription in self.subscriptions:
                self._mqueue.put(msg)
            else:
                self.connections.send(self.BROADCAST, msg)
        elif to != self.id:
            self.connections.send(to, msg)
        else:
            self._mqueue.put(msg)
        return event
