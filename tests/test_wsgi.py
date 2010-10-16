import mock
from webtest import TestApp

from drivel.wsgi import WSGIServer

def test_unregistered_path():
    server = WSGIServer(mock.Mock(), 'test')
    server.watch_connections = False
    app = TestApp(server)
    resp = app.get('/foo/', status=404)
    assert resp.status[:3] == "404", resp.status
