from eventlet import coros
from eventlet import pools
import memcache
from ..component import Component
#from .green import memcache

class _MemcachePool(pools.Pool):
    def __init__(self, pool_size, servers, *args, **kwargs):
        super(_MemcachePool, self).__init__(max_size=pool_size)
        self.servers = servers
        self.client_args = args
        self.client_kwargs = kwargs

    def create(self):
        return memcache.Client(self.servers,
            *self.client_args, **self.client_kwargs)


class ClientPool(Component):
    subscription = 'memcache'

    def __init__(self, server):
        super(ClientPool, self).__init__(server)
        poolsize = server.config.getint('memcache', 'pool_size')
        servers = [value for name, value
            in server.config.items('memcache-servers')]
        self._pool = _MemcachePool(poolsize, servers)

    def _handle_message(self, event, message):
        method = message[0]
        args = message[1:]
        client = self._pool.get()
        ret = getattr(client, method)(*args)
        self._pool.put(client)
        event.send(ret)

