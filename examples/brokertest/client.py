import eventlet
from eventlet import debug
from drivel.messaging.broker import Broker
#eventlet.debug.hub_blocking_detection(True)
broker = Broker('client')
broker.connections.connect(('127.0.0.1', 8899))
eventlet.spawn(broker.listen)
eventlet.spawn(broker.process)

p = eventlet.GreenPool()

def do(broker):
    event = broker.send(broker.BROADCAST, 'hello', 'hello')
    #eventlet.sleep(0)
    event.wait()

while True:
    p.spawn(do, broker)
