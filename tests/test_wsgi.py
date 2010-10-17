import mock
from nose import tools
from webtest import TestApp

from drivel import wsgi
from drivel.wsgi import PathNotResolved
from drivel.wsgi import WSGIServer


class MockEvent(object):
    def __init__(self, response):
        self.resp = response

    def wait(self):
        return self.resp


def test_unregistered_path():
    server = WSGIServer(mock.Mock(), 'test')
    server.watch_connections = False
    app = TestApp(server)
    resp = app.get('/foo/', status=404)
    assert resp.status[:3] == "404", resp.status


def test_path_subscription():
    server = WSGIServer(mock.Mock(), 'test')
    tools.assert_raises(PathNotResolved,
        server._path_to_subscriber,
        '/foo/')
    server.add_route(r'^/foo/$', 'foo')
    assert server._path_to_subscriber('/foo/') == ('foo', None, {})
    # test keyword arguments
    server.add_route(('something', r'^/bar/(?P<test>[^/]+)/$'), 'bar')
    assert server._path_to_subscriber('/bar/hello/') == ('bar', 'something', 
        {'test': 'hello'})


def test_dummy_route():
    drvsrv = mock.Mock()
    server = WSGIServer(drvsrv, 'test')
    server.watch_connections = False
    server.add_route(('foo_test', r'^/foo/'), 'foo')
    drvsrv.send.side_effect = lambda *args, **kwargs: MockEvent('test response')
    app = TestApp(server)
    resp = app.get('/foo/')
    assert resp.status[:3] == "200", resp.status
    assert resp.body == 'test response\r\n'
    assert drvsrv.send.called
    # check the correct message is sent
    assert drvsrv.send.call_args[0][0:2] == ('foo', 'foo_test'), \
        drvsrv.send.call_args[0][0:2]

    # check jsonp response
    drvsrv.reset_mock()
    resp = app.get('/foo/?jsonpcallback=cb')
    assert resp.body == 'cb("test response")\r\n', resp.body

def test_dummy_route():
    drvsrv = mock.Mock()
    server = WSGIServer(drvsrv, 'test')
    server.watch_connections = False
    server.add_route(('foo_test', r'^/foo/'), 'foo')
    _r = [1, 2, '3']
    drvsrv.send.side_effect = lambda *args, **kwargs: MockEvent(_r)
    app = TestApp(server)
    resp = app.get('/foo/')
    assert wsgi.json.loads(resp.body) == _r
