from __future__ import with_statement
import eventlet

from drivel.component import Component

class Ping(Component):
    subscription='ping'

    def __init__(self, server, name=None):
        super(Ping, self).__init__(server, name)
        eventlet.spawn(self.pinger)

    def pinger(self):
        while True:
            event = self.server.send('pong', 'PING')
            try:
                with eventlet.Timeout(10):
                    ret = event.wait()
                    print ret
                eventlet.sleep(5)
            except Timeout, e:
                print 'failed to get ping!'

class Pong(Component):
    subscription='pong'

    def __init__(self, server, name=None):
        super(Pong, self).__init__(server, name)

    def handle_message(self, message):
        if message[0] == 'PING':
            print 'PING'
            return 'PONG'
        else:
            print 'got unknown ping type', message
