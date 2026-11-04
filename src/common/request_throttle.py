import math
import time


class RequestThrottle:

    def __init__(self, interval):
        if not math.isfinite(interval) or interval < 0:
            raise ValueError("El intervalo de descarga tiene que ser finito y no negativo")
        self._interval = interval
        self._next_request = 0

    def wait(self, stop_event):
        delay = max(0, self._next_request - time.monotonic())
        if delay > 0 and stop_event.wait(delay):
            return False
        if stop_event.is_set():
            return False
        self._next_request = time.monotonic() + self._interval
        return True
