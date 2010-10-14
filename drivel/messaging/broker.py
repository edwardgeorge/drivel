# moving the broker aspect from the server for easy refactoring.
import eventlet
import logging

from drivel.event import EventManager, RETURN_SUB
from drivel.messaging.connections import Connections

Logger = logging.getLogger

SUB_DISCOVERY = 'subscription-discovery'
SUB_BROADCAST = 'subscription-broadcast'


class Broker(object):
    def __init__(self, name, id):
        self.id = id
        self.name = name
        self._mqueue = eventlet.Queue()
        self.events = EventManager(id, self)
        self.connections = Connections(name, id)
        self.subscriptions = {}
        self.remote_subs = {}
        self.remote_seen = set()
        self.BROADCAST = self.connections.ALL
        # process control
        self.started = False
        self.single_process = True
        self.continue_listening = True
        self.continue_processing = True
        self._process_gt = None
        self._listen_gt = None
        self._listen_and_process_gt = None
        # metrics
        self.msgs_processed = 0

    def start(self):
        if self.single_process:
            if bool(self._listen_and_process_gt):
                return
            if bool(self._process_gt):
                self._process_gt.kill()
            if bool(self._listen_gt):
                self._listen_gt.kill()
            l = eventlet.spawn(self.listen_and_process)
            self._listen_and_process_gt = l
            l.link(self._ended, 'listen_and_process')
        else:
            if bool(self._listen_and_process_gt):
                self._listen_and_process_gt.kill()
            if not bool(self._process_gt):
                p = self._process_gt = eventlet.spawn(self.process)
                p.link(self._ended, 'process')
            if not bool(self._listen_gt):
                l = self._listen_gt = eventlet.spawn(self.listen)
                l.link(self._ended, 'listen')
        self.started = True
        self.discover_subscriptions()

    def discover_subscriptions(self, from_=None):
        self.send(from_, SUB_DISCOVERY, self.id, link_event=False)

    def handle_discovery_request(self, from_):
        logger = Logger('drivel.messaging.broker.Broker'
            '.handle_discovery_request')
        logger.info('got discovery request from %s' % from_)
        self.send(from_, SUB_BROADCAST, (self.id,
            self.subscriptions.keys()), link_event=False)
        if from_ is not None and from_ in self.remote_seen:
            self.discover_subscriptions(from_)

    def handle_discovery_response(self, message):
        logger = Logger('drivel.messaging.broker.Broker'
            '.handle_discovery_response')
        remote_id, subscriptions = message
        logger.info('got discovery response from %s' % remote_id)
        for sub in subscriptions:
            self.remote_subs.setdefault(sub, set()).add(remote_id)
        self.remote_seen.add(remote_id)

    def switch(self, single_process):
        if not isinstance(single_process, bool):
            raise TypeError()
        if self.single_process != single_process:
            self.single_process = single_process
        self.start()

    def _ended(self, gt, pname):
        pass

    def subscribe(self, key, queue):
        self.subscriptions[key] = queue
        if self.started:
            self.handle_discovery_request(None)

    def unsubscribe(self, key):
        self.subscriptions.pop(key, None)

    def process(self):
        logger = Logger('drivel.messaging.broker.Broker.process')
        #while self.continue_processing:
        while True:
            try:
                self.process_one()
            except Exception, e:
                logger.exception('error in process: %s' % (e, ))

    def process_one(self):
        logger = Logger('drivel.messaging.broker.Broker.process_one')
        eventid, subscription, message = self._mqueue.get()
        logger.debug('msg process: %r, %r' % (eventid, subscription))
        self.process_msg(eventid, subscription, message)

    def process_msg(self, eventid, subscription, message):
        logger = Logger('drivel.messaging.broker.Broker.process_msg')
        self.msgs_processed += 1
        event = self.events.returner_for(eventid)
        if subscription == RETURN_SUB:
            logger.debug('returning event to %r' % (eventid, ))
            event = self.events.returner_for(message['envelopeto'])
            if not event.ready():
                event.send(**message['data'])
        elif subscription == SUB_DISCOVERY:
            self.handle_discovery_request(message)
        elif subscription == SUB_BROADCAST:
            self.handle_discovery_response(message)
        elif subscription in self.subscriptions:
            self.subscriptions[subscription].put((event, message))
        else:
            #logger.debug('%s received message for unknown subscription: %s' %
                #(self.name, subscription))
            pass

    def process_now(self, message):
        logger = Logger('drivel.messaging.broker.Broker.process_one')
        try:
            self.process_msg(*message)
        except Exception, e:
            logger.exception('error in process_now: %s' % (e, ))

    def listen(self):
        logger = Logger('drivel.messaging.broker.Broker.listen')
        #while self.continue_listening:
        while True:
            try:
                self.listen_one()
            except Exception, e:
                logger.error('error in listen: %s' % (e, ))

    def listen_and_process(self):
        logger = Logger('drivel.messaging.broker.Broker.listen')
        #while self.continue_listening:
        while True:
            try:
                self.process_msg(*self.listen_one(False))
            except Exception, e:
                logger.error('error in listen_and_process: %s' % (e, ))

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
            'single_process': self.single_process,
            'processes': {
                'listen': bool(self._listen_gt),
                'process': bool(self._process_gt),
                'listen_and_process': bool(self._listen_and_process_gt),
            },
            'messages_processed': self.msgs_processed,
        }

    def send(self, to, subscription, message, link_event=True):
        logger = Logger('drivel.messaging.broker.Broker.send')
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
