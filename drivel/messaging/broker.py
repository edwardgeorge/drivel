# moving the broker aspect from the server for easy refactoring.
import logging

import eventlet

from drivel.event import EventManager, RETURN_SUB
from drivel.messaging.connections import EventedConnections as Connections

logger = logging.getLogger(__name__)

SUB_DISCOVERY = 'subscription-discovery'
SUB_BROADCAST = 'subscription-broadcast'


class Broker(object):
    def __init__(self, name, id):
        self.id = id
        self.name = name
        self._mqueue = eventlet.Queue()
        self.events = EventManager(id, self)
        self.connections = Connections(name, id, self._handle_msg)
        self.connections.add_connect_handler(self._handle_connect)
        self.subscriptions = {}
        self.remote_subs = {}
        self.remote_seen = set()
        # constants
        self.BROADCAST = self.connections.ALL
        self.SUB_DISCOVERY = SUB_DISCOVERY
        self.SUB_BROADCAST = SUB_BROADCAST
        # metrics
        self.msgs_processed = 0

    def _handle_msg(self, senderid, data):
        eid, sub, msg = data
        eventid = (senderid, eid)
        self.process_msg(eventid, sub, msg)

    def _handle_connect(self, sock, addr):
        self.discover_subscriptions()

    def discover_subscriptions(self, from_=None):
        self.send(from_, self.SUB_DISCOVERY, self.id, link_event=False)

    def handle_discovery_request(self, from_):
        logger.info('got discovery request from %s' % from_)
        self.send(from_, self.SUB_BROADCAST, (self.id,
            self.subscriptions.keys()), link_event=False)
        if from_ is not None and from_ in self.remote_seen:
            self.discover_subscriptions(from_)

    def handle_discovery_response(self, message):
        remote_id, subscriptions = message
        logger.info('got discovery response from %s' % remote_id)
        for sub in subscriptions:
            self.remote_subs.setdefault(sub, set()).add(remote_id)
        self.remote_seen.add(remote_id)

    def subscribe(self, key, queue):
        self.subscriptions[key] = queue
        self.handle_discovery_request(None)

    def unsubscribe(self, key):
        self.subscriptions.pop(key, None)

    def process_msg(self, eventid, subscription, message):
        self.msgs_processed += 1
        event = self.events.returner_for(eventid)
        if subscription == RETURN_SUB:
            logger.debug('returning event to %r' % (eventid, ))
            event = self.events.returner_for(message['envelopeto'])
            if not event.ready():
                event.send(**message['data'])
        elif subscription == self.SUB_DISCOVERY:
            self.handle_discovery_request(message)
        elif subscription == self.SUB_BROADCAST:
            self.handle_discovery_response(message)
        elif subscription in self.subscriptions:
            self.subscriptions[subscription].put((event, message))
        else:
            #logger.debug('%s received message for unknown subscription: %s' %
                #(self.name, subscription))
            pass

    def stats(self):
        return {
            'events': len(self.events),
            'connections': len(self.connections),
            'messages': self._mqueue.qsize(),
            'subscriptions': len(self.subscriptions),
            'single_process': self.single_process,
            'processes': {
                'listen': bool(self._listen_gt),
                'process': bool(self._process_gt),
                'listen_and_process': bool(self._listen_and_process_gt),
            },
            'messages_processed': self.msgs_processed,
        }

    def send(self, to, subscription, message, link_event=True):
        if link_event:
            event, eventid = self.events.create()
        else:
            event, eventid = None, 'null'
        msg = (eventid, subscription, message)
        if to is None:
            if subscription in self.subscriptions:
                if self.single_process:
                    self.process_now(msg)
                else:
                    self._mqueue.put(msg)
            elif subscription in self.remote_subs:
                ids = list(self.remote_subs[subscription])
                self.connections.send(ids, msg)
            else:
                self.connections.send(self.BROADCAST, msg)
        elif to != self.id:
            self.connections.send(to, msg)
        else:
            self._mqueue.put(msg)
        return event
