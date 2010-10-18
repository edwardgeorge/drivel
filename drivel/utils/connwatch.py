import errno
import logging
import select
import socket

import eventlet
from eventlet import greenthread
from eventlet import hubs


def poll_check(sock, poll=select.poll, mask=select.POLLHUP|select.POLLERR):
    p = poll()
    try:
        p.register(sock, mask)
        a = p.poll(0)
        return bool(a)
    finally:
        if hasattr(p, 'close'):
            p.close()


def connectionwatcher(sock, proc_weakref, *exc_args):
    logger = logging.getLogger('drivel.utils.connwatch.connectionwatcher')
    enums = [errno.EPIPE, errno.ECONNRESET]
    def kill():
        p = proc_weakref()
        if p:
            greenthread.kill(p, *exc_args)
    while True:
        hubs.trampoline(sock, read=True)
        try:
            if hasattr(sock, 'read'):
                data = sock.read()
            else:
                data = sock.recv(4096)
            if not data:
                kill()
                return
            else:
                logging.error('got unexpected data %r from sock %d' % (data, sock.fileno()))
        except socket.error, e:
            if e[0] in enums:
                kill()
                return
            logging.error('unexpected socket.error %d: %s' % (e[0], errno.errorcode[e[0]]))
        except IOError, e:
            if e.errno in enums:
                kill()
                return
            logging.error('unexpected IOError %d: %s' % (e.errno, errno.errorcode[e.errno]))


def spawn(sock, proc, *exc_args):
    return eventlet.spawn(connectionwatcher, sock, proc, *exc_args)


def spawn_from_pool(pool, sock, proc, *exc_args):
    return pool.spawn(connectionwatcher, sock, proc, *exc_args)
