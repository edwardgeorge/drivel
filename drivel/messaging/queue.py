from eventlet.queue import Queue

class Messaging(object):
    def __init__(self, queue=None):
        if queue is None:
            queue = Queue()
        self.queue = queue

    def wait(self, timeout=None):
        return self.queue.get(timeout=timeout)

    def send(self, data):
        self.queue.put(data)
