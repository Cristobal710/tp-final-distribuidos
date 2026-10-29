import queue
import threading
import time

from common.protocol import internal


def noop():
    pass


class OutputQueue:

    def __init__(self):
        self.messages = []
        self.threads = []
        self.closed = False

    def send(self, message):
        self.messages.append(message)
        self.threads.append(threading.get_ident())

    def close(self):
        self.closed = True

    def decoded(self, msg_type=None):
        messages = [internal.decode(message) for message in self.messages]
        return [value for kind, value in messages if msg_type is None or kind == msg_type]


class ConsumerQueue:

    def __init__(self):
        self.pending = queue.Queue()
        self.acks = 0
        self.stopped = False

    def run_in_consumer_thread(self, callback):
        self.pending.put(callback)

    def stop_consuming(self):
        self.stopped = True

    def deliver(self, worker, message):
        worker._on_message(message, self.ack, noop)

    def ack(self):
        self.acks += 1

    def pump(self):
        while not self.pending.empty():
            self.pending.get_nowait()()

    def until(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while not predicate():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError("El consumidor no termino a tiempo")
            try:
                callback = self.pending.get(timeout=min(remaining, 0.05))
            except queue.Empty:
                continue
            callback()


class GroupControl(OutputQueue):

    def __init__(self):
        super().__init__()
        self.workers = []

    def send(self, message):
        super().send(message)
        for worker in self.workers:
            worker._on_control_message(message, noop, noop)
