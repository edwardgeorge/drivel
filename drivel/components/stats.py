import eventlet

from drivel.component import Component
from drivel.component import WSGIComponent


class PathError(Exception):
    pass


def dpath(dict_, path):
    path = path.strip('/').split('/')
    result = dict_
    try:
        for i in path:
            if isinstance(result, dict):
                result = result[i]  # KeyError
            elif isinstance(result, (tuple, list)):
                i = int(i)  # ValueError
                result = result[i]  # IndexError
            else:
                raise PathError()
    except (KeyError, ValueError, IndexError), e:
        raise PathError()
    return result


class StatsComponent(WSGIComponent):
    subscription = "stats"
    urlmapping = {
        'stats': r'/stats(?P<path>/.+)?/$',
    }

    def __init__(self, server, name):
        super(StatsComponent, self).__init__(server, name)

    def do_stats(self, user, request, proc, path=None):
        stats = self.server.stats()
        if path:
            try:
                stats = dpath(stats, path)
            except PathError, e:
                # should 404
                return {'error': 'path not found'}
        return stats


class StatsCollectorComponent(WSGIComponent):
    subscription = "stats_collector"
    urlmapping = {
        'stats': r'/stats(?P<path>/.+)?/$',
    }

    def __init__(self, server, name):
        super(StatsCollectorComponent, self).__init__(server, name)
        self.known_responders = set()
        self.initial_discovery_sent = False

    def handle_message(self, message):
        cmd, rest = message[0], message[1:]
        if cmd == 'response':
            return self.do_response(rest)
        elif cmd == 'announce':
            return self.do_announce(rest)
        return super(StatsCollectorComponent, self).handle_message(message)

    def _collect_thread(self, procid):
        g = self.server.send('stats_responder',
                'collect',
                address_to=procid)
        return procid, g.wait()

    def collect(self):
        responders = [(i, ) for i in self.known_responders]
        if responders:
            pool = eventlet.GreenPool(len(responders))
            mapper = pool.starmap(self._collect_thread, responders)
            stats = dict(mapper)
        else:
            stats = {}
        if self.server.procid not in stats:
            stats[self.server.procid] = self.server.stats()
        return stats

    def discover(self):
        self.server.send('stats_responder',
                'discover',
                self.server.procid,
                'stats_collector',
                'announce',
                broadcast=True,
                link_event=False)

    def do_stats(self, user, request, proc, path=None):
        if len(self.known_responders) < 1 or not self.initial_discovery_sent:
            self.discover()
            self.initial_discovery_sent = True
            eventlet.sleep(1)
        stats = self.collect()
        if path
            try:
                stats = dpath(stats, path)
            except PathError, e:
                # should 404
                return {'error': 'path not found'}
        return stats

    def do_announce(self, message):
        procid, = message
        assert isinstance(procid, basestring)
        self.known_responders.add(procid)

    def do_discover(self, message):
        self.discover()


class StatsResponderComponent(Component):
    subscription = "stats_responder"

    def handle_message(self, message):
        cmd, rest = message[0], message[1:]
        if cmd == 'discover':
            return self.do_discover(rest)
        if cmd == 'collect':
            return self.do_collect(rest)

    def do_discover(self, message):
        collectorid, sub, msg = message
        self.server.send(sub,
            msg, self.server.procid,
            address_to=collectorid,
            link_event=False)

    def do_collect(self, message):
        return self.server.stats()
