from __future__ import with_statement
import struct

import eventlet
from eventlet.green import socket
from eventlet.semaphore import Semaphore

from drivel.utils.importing import import_preferential
pickle = import_preferential('cPickle', 'pickle')

LEN_HEADER = struct.Struct('!I')


class EOF(Exception):
    pass


class Messaging(object):
    def __init__(self, sock, serialiser=pickle):
        self.sock = sock
        self.buff = ''
        self._ser = serialiser
        self._len = None
        self._bsz = 4096
        self._sem = Semaphore()

    def fileno(self):
        return self.sock.fileno()

    def peek(self):
        if self._len is None and len(self.buff) >= LEN_HEADER.size:
            header, self.buff = (self.buff[:LEN_HEADER.size],
                                 self.buff[LEN_HEADER.size:])
            (self._len,) = LEN_HEADER.unpack(header)
        if self._len is not None and self._len <= len(self.buff):
            return True
        return False

    def wait(self, timeout=None, _do_recv=True):
        timer = eventlet.Timeout(timeout) if timeout is not None else None
        try:
            while True:
                if self.peek():
                    ret, self.buff = (self.buff[:self._len],
                                      self.buff[self._len:])
                    self._len = None
                    return self._ser.loads(ret)
                elif _do_recv:
                    ret = self.sock.recv(self._bsz)
                    if not ret:
                        raise EOF()
                    self.buff = self.buff + ret
                else:
                    return
        except eventlet.Timeout:
            return None
        finally:
            if timer:
                timer.cancel()

    def _do_recv(self):
        # for internal use
        ret = self.sock.recv(self._bsz)
        if not ret:
            return
        self.buff = self.buff + ret
        return len(ret)

    def send(self, data):
        data = self._ser.dumps(data)
        data = LEN_HEADER.pack(len(data)) + data
        self.sock.send(data)

    def send_concurrent(self, data):
        with self._sem:
            self.send(data)

    def shutdown(self):
        self.sock.shutdown(socket.SHUT_WR)
        self.sock.close()
