import http.client
import logging
import socket
import threading
from urllib.parse import quote

RESTARTED = 204


class UnixHTTPConnection(http.client.HTTPConnection):

    def __init__(self, path, timeout):
        super().__init__("localhost", timeout=timeout)
        self._path = path

    def connect(self):
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self.timeout)
        try:
            connection.connect(self._path)
        except OSError:
            connection.close()
            raise
        self.sock = connection


class DockerRestarter:

    def __init__(self, socket_path, timeout=30, connect=None):
        self._connect = connect or (lambda: UnixHTTPConnection(socket_path, timeout))

    def restart(self, name):
        connection = self._connect()
        try:
            connection.request("POST", f"/containers/{quote(name, safe='')}/restart?t=0")
            response = connection.getresponse()
            body = response.read()
        finally:
            connection.close()
        if response.status != RESTARTED:
            raise RuntimeError(f"Docker respondio {response.status} al reiniciar {name}: {body[:200]!r}")


class BackgroundRestarter:

    def __init__(self, restart):
        self._restart = restart
        self._running = set()
        self._lock = threading.Lock()

    def __call__(self, name):
        with self._lock:
            if name in self._running:
                return None
            self._running.add(name)
        thread = threading.Thread(target=self._run, args=(name,), daemon=True)
        thread.start()
        return thread

    def _run(self, name):
        try:
            self._restart(name)
            logging.info("%s reiniciado", name)
        except Exception:
            logging.exception("No se pudo reiniciar %s", name)
        finally:
            with self._lock:
                self._running.discard(name)
