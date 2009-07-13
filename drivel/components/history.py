from collections import defaultdict
from collections import deque
import time

from eventlet import coros

from drivel.component import Component

class History(Component):
    subscription = 'history'
    def __init__(self, server):
        super(History, self).__init__(server)
        self.history = defaultdict(list)
        self.waiters = defaultdict(deque)

    def get(self, user, since=None):
        self.log('debug', 'looking for messages for user %s since %s' % (
            user.username, since))
        if user in self.history and len(self.history[user]):
            msgs = [msg for msg in self.history[user]
                if since is None or msg[0] > since]
            self.log('debug', 'found %d messages for user %s since %s' % (
                len(msgs), user.username, since))
            return msgs
        return None

    def _handle_message(self, event, message):
        method, user, data = message
        if method == 'get':
            msgs = self.get(user, data)
            if not msgs:
                # wait for incoming
                self.log('debug', 'waiting for incoming messages '
                    'for user %s' % user.username)
                evt = coros.event()
                self.waiters[user].append(evt)
                evt.wait()
                self.log('debug', 'received notification of messages '
                    'for user %s' % user.username)
                msgs = self.get(user, data)
            event.send(msgs)
        elif method == 'set':
            self.history[user].append((time.time(), data))
            event.send()
            if user in self.waiters and len(self.waiters[user]):
                self.log('debug', 'waking up waiters for user %s'
                    % user.username)
                while len(self.waiters[user]):
                    waiter = self.waiters[user].popleft()
                    waiter.send()

    def stats(self):
        stats = super(History, self).stats()
        stats.update({
            'waitingonnotification': sum(map(len, self.waiters.values())),
        })
        return stats

