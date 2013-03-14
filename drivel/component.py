from functools import partial
import weakref

import eventlet
from eventlet import queue
from eventlet import greenthread
from eventlet import hubs


class CancelOperation(Exception):
    pass


class Component(object):
    subscription = None
    asynchronous = True  # spawn a coroutine for each message
    message_pool_size = 1000  # set to use a pool rather than coroutines

    def __init__(self, server, name=None):
        self.server = server
        self.registered_name = name
        self._mqueue = queue.Queue()
        assert self.subscription is not None
        self.server.subscribe(self.subscription, self._mqueue)
        self._greenlet = eventlet.spawn(self._process)
        self._coropool = None
        self.received_messages = 0
        self.handled_messages = 0
        self.num_errors = 0
        if self.asynchronous:
            poolsize = self.message_pool_size
            self._coropool = eventlet.GreenPool(size=poolsize)
            self._execute = self._coropool.spawn
        else:
            self._coropool = eventlet.GreenPool(size=1)
            self._execute = lambda func, *args: self._coropool.spawn(func,
                *args).wait()
        self.log = partial(self.server.log, self.__class__.__name__)

    @property
    def config(self):
        return self.server.config

    def _process(self):
        while True:
            try:
                event, message = self._mqueue.get()
                self.received_messages += 1
                self._execute(self._handle_message, event, message)
            except Exception, e:
                pass

    def _handle_message(self, event, message):
        try:
            res = self.handle_message(message)
            self.handled_messages += 1
            self.send_to_event(event, res)
        except Exception, e:
            self.num_errors += 1
            self.send_to_event(event, exc=e)

    def send_to_event(self, event, *args, **kwargs):
        try:
            event.send(*args, **kwargs)
        except ReferenceError, e:
            pass

    def handle_message(self, message):
        raise NotImplementedError()

    def stop(self):
        self._greenlet.throw()
        if self._coropool:
            self._coropool.killall()

    def stats(self):
        stats = {}
        if self._coropool:
            stats.update({
                'free': self._coropool.free(),
                'running': self._coropool.running(),
                'balance': self._coropool.sem.balance,
            })
        stats.update({
            'items': self._mqueue.qsize(),
            'handled': self.handled_messages,
            'received': self.received_messages,
            'errors': self.num_errors,
            'alive': bool(self._greenlet),
        })
        return stats

    def _dothrow(self, gt, cgt):
        #print 'throwing cancel from:%s to:%s current:%s' % (gt, cgt,
        #    greenthread.getcurrent())
        if isinstance(cgt, weakref.ref):
            cgt = cgt()
            if cgt is None:
                return
        if isinstance(cgt, greenthread.GreenThread):
            cgt.kill(CancelOperation, None, None)
        else:
            hubs.get_hub().schedule_call_local(0,
                greenthread.getcurrent().switch)
            cgt.throw(CancelOperation())
