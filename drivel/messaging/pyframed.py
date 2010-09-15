import pickle
import struct

import eventlet

LEN_HEADER = struct.Struct('>I')

class Messaging(object):
    def __init__(self, sock, serialiser=pickle):
        self.sock = sock
        self.buff = ''
        self._ser = serialiser
        self._len = None
        self._bsz = 4096

    def wait(self, timeout=None):
        timer = eventlet.Timeout(timeout) if timeout is not None else None
        try:
            while True:
                if self._len is not None and self._len <= self.buff:
                    ret, self.buff = self.buff[:self._len], self.buff[self._len:]
                    self._len = None
                    return self._ser.loads(ret)
                elif self._len is None and len(self.buff) >= LEN_HEADER.size:
                    header, self.buff = self.buff[:LEN_HEADER.size], self.buff[LEN_HEADER.size:]
                    (self._len,) = LEN_HEADER.unpack(header)
                else:
                    self.buff += self.sock.recv(self._bsz)
        except eventlet.Timeout, e:
            return None
        finally:
            if timer:
                timer.cancel()

    def send(self, data):
        data = self._ser.dumps(data)
        data = LEN_HEADER.pack(len(data)) + data
        self.sock.send(data)