import socket
import threading

PING = "PING"
PONG = "PONG"
OK = "ok"
RECOVERING = "recuperando"
MAX_DATAGRAM_BYTES = 1024
POLL_SECONDS = 0.5


def ping_message(token):
    return f"{PING} {token}".encode()


def pong_reply(message, state):
    parts = message.decode(errors="replace").split()
    if len(parts) != 2 or parts[0] != PING:
        return None
    return f"{PONG} {parts[1]} {state}".encode()


def parse_pong(message):
    parts = message.decode(errors="replace").split()
    if len(parts) != 3 or parts[0] != PONG:
        return None
    return parts[1], parts[2]


class HeartbeatServer:

    def __init__(self, port, host=""):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind((host, port))
        self._socket.settimeout(POLL_SECONDS)
        self._state = OK
        self._stopping = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    @property
    def port(self):
        return self._socket.getsockname()[1]

    @property
    def state(self):
        return self._state

    def set_state(self, state):
        self._state = state

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stopping.set()
        if self._thread.is_alive():
            self._thread.join()
        self._socket.close()

    def _serve(self):
        while not self._stopping.is_set():
            try:
                message, address = self._socket.recvfrom(MAX_DATAGRAM_BYTES)
            except TimeoutError:
                continue
            except OSError:
                if self._stopping.is_set():
                    return
                continue
            reply = pong_reply(message, self._state)
            if reply is None:
                continue
            try:
                self._socket.sendto(reply, address)
            except OSError:
                pass
