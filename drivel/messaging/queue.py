from eventlet.queue import Queue, Empty


class Messaging(object):
    def __init__(self, queue=None):
        if queue is None:
            queue = Queue()
        self.queue = queue

    def wait(self, timeout=None):
        try:
            return self.queue.get(timeout=timeout)
        except Empty, e:
            return None

    def send(self, data):
        self.queue.put(data)
