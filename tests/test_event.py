from mock import Mock
from nose import tools

from drivel.event import EventManager

def test():
    publisher = Mock()
    man = EventManager('dummy', publisher)
    evt, eid = man.create()
    assert not evt.ready()

    ret = man.getreturner('dummy', eid)
    ret.send('foo')
    assert publisher.send.called
    args, kwargs = publisher.send.call_args
    rid = args[2]['envelopeto']
    assert rid[1] == eid, (rid, eid)
    man.return_(rid[1], args[2]['data'])
    assert evt.ready()
    assert evt.wait() == 'foo'

@tools.raises(KeyError)
def test_exception():
    publisher = Mock()
    man = EventManager('dummy', publisher)
    evt, eid = man.create()

    ret = man.getreturner('dummy', eid)
    ret.send(exc=KeyError())
    assert publisher.send.called
    args, kwargs = publisher.send.call_args
    rid = args[2]['envelopeto']
    man.return_(rid[1], args[2]['data'])
    assert evt.ready()
    evt.wait()
