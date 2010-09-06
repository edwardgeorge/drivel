import errno
import select

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
        except socket.error, e:
            if e[0] in enums:
                kill()
                return
        except IOError, e:
            if e.errno in enums:
                kill()
                return


def spawn(sock, proc, *exc_args):
    return eventlet.spawn(connectionwatcher, sock, proc, *exc_args)
