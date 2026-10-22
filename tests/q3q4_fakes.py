import threading

from common.protocol import internal
from common.protocol.internal import MsgType


# Funcion que no hace nada, para usar como ack y nack.
def noop():
    pass


# Cola de salida falsa que guarda lo que le mandan ya decodificado.
class FakeOutputQueue:

    def __init__(self):
        self.sent = []
        self.closed = False

    def send(self, message):
        self.sent.append(internal.decode(message))

    def close(self):
        self.closed = True

    def data(self, msg_type=None):
        return [value for kind, value in self.sent
                if kind not in (MsgType.EOF, MsgType.LATE_EOF) and msg_type in (None, kind)]

    def eofs(self):
        return [(kind, value) for kind, value in self.sent if kind in (MsgType.EOF, MsgType.LATE_EOF)]


# Cola de entrada falsa de un solo hilo que entrega los mensajes en orden.
class FakeInputQueue:

    def __init__(self, messages=()):
        self.messages = list(messages)
        self.acks = 0
        self.stopped = False

    def start_consuming(self, callback):
        for message in self.messages:
            callback(message, self._ack, noop)

    def stop_consuming(self):
        self.stopped = True

    def _ack(self):
        self.acks += 1


# Entrada de control falsa, como la del clasificador: entrega al callback lo que le mandan.
class FakeControlInput:

    def __init__(self):
        self.callback = None
        self.ready = threading.Event()
        self.released = threading.Event()

    def start_consuming(self, callback):
        self.callback = callback
        self.ready.set()
        self.released.wait()

    def stop_consuming(self):
        self.released.set()

    def deliver(self, message):
        self.callback(message, noop, noop)


# Cola de texto falsa: entrega los eventos en orden y ejecuta lo que se le pidio correr en su hilo entre mensaje y mensaje.
class FakeTextQueue:

    def __init__(self, events, control_input):
        self.events = list(events)
        self.control_input = control_input
        self.pending = []
        self.acks = 0

    def start_consuming(self, callback):
        self.control_input.ready.wait(timeout=5)
        for event in self.events:
            self._run_pending()
            callback(event, self._ack, noop)
        self._run_pending()

    def stop_consuming(self):
        pass

    def run_in_consumer_thread(self, callback):
        self.pending.append(callback)

    def _run_pending(self):
        while self.pending:
            self.pending.pop(0)()

    def _ack(self):
        self.acks += 1


# Salida de control falsa: entrega cada mensaje a todas las entradas de control.
class FakeControlOutput:

    def __init__(self, control_inputs):
        self.control_inputs = control_inputs
        self.sent = []

    def send(self, message):
        self.sent.append(message)
        for control_input in self.control_inputs:
            control_input.deliver(message)

    def close(self):
        pass
