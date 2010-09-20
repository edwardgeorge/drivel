import pickle
import struct

import eventlet

from drivel.utils.importing import import_preferential
pickle = import_preferential('cPickle', 'pickle')

LEN_HEADER = struct.Struct('>I')

class EOF(Exception):
    pass


class Messaging(object):
    def __init__(self, sock, serialiser=pickle):
        self.sock = sock
        self.buff = ''
        self._ser = serialiser
        self._len = None
        self._bsz = 4096

    def fileno(self):
        return self.sock.fileno()

    def peek(self):
        if self._len is None and len(self.buff) >= LEN_HEADER.size:
            header, self.buff = self.buff[:LEN_HEADER.size], self.buff[LEN_HEADER.size:]
            (self._len,) = LEN_HEADER.unpack(header)
        if self._len is not None and self._len <= len(self.buff):
            return True
        return False

    def wait(self, timeout=None):
        timer = eventlet.Timeout(timeout) if timeout is not None else None
        try:
            while True:
                if self.peek():
                    ret, self.buff = self.buff[:self._len], self.buff[self._len:]
                    self._len = None
                    return self._ser.loads(ret)
                else:
                    ret = self.sock.recv(self._bsz)
                    if not ret:
                        raise EOF()
                    self.buff = self.buff + ret
        except eventlet.Timeout, e:
            return None
        finally:
            if timer:
                timer.cancel()

    def send(self, data):
        data = self._ser.dumps(data)
        data = LEN_HEADER.pack(len(data)) + data
        self.sock.send(data)
