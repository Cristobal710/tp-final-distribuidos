import logging
import socket
import threading
import time

MAX_DATAGRAM_BYTES = 1024
MIN_WAIT_SECONDS = 0.01
REFRESH_SECONDS = 5.0


class UdpTransport:

    def __init__(self, port, host="", resolve=socket.gethostbyname, clock=time.monotonic,
                 refresh_seconds=REFRESH_SECONDS):
        self._port = port
        self._resolve = resolve
        self._clock = clock
        self._refresh_seconds = refresh_seconds
        self._addresses = {}
        self._resolved_at = {}
        self._resolving = set()
        self._lock = threading.Lock()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind((host, port))

    def send(self, name, payload):
        self._refresh(name)
        with self._lock:
            address = self._addresses.get(name)
        if address is None:
            return
        try:
            self._socket.sendto(payload, (address, self._port))
        except OSError as e:
            logging.debug("No se pudo mandar a %s: %s", name, e)

    def reply(self, address, payload):
        try:
            self._socket.sendto(payload, address)
        except OSError as e:
            logging.debug("No se pudo responder a %s: %s", address, e)

    def receive(self, timeout):
        self._socket.settimeout(max(timeout, MIN_WAIT_SECONDS))
        try:
            return self._socket.recvfrom(MAX_DATAGRAM_BYTES)
        except OSError:
            return None

    def close(self):
        self._socket.close()

    def _refresh(self, name):
        now = self._clock()
        with self._lock:
            if name in self._resolving:
                return
            if name in self._resolved_at and now - self._resolved_at[name] < self._refresh_seconds:
                return
            self._resolving.add(name)
        threading.Thread(target=self._resolve_in_background, args=(name,), daemon=True).start()

    def _resolve_in_background(self, name):
        try:
            address = self._resolve(name)
        except OSError as e:
            logging.debug("No se pudo resolver %s: %s", name, e)
            address = None
        with self._lock:
            self._resolving.discard(name)
            self._resolved_at[name] = self._clock()
            if address is not None:
                self._addresses[name] = address
