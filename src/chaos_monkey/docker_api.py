import http.client
import json
import socket
from urllib.parse import quote

OK = 200
NO_CONTENT = 204


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


class DockerClient:

    def __init__(self, socket_path, timeout=30, connect=None):
        self._connect = connect or (lambda: UnixHTTPConnection(socket_path, timeout))

    def running(self):
        status, body = self._request("GET", "/containers/json")
        if status != OK:
            raise RuntimeError(f"Docker respondio {status} al listar contenedores: {body[:200]!r}")
        return [name.lstrip("/") for container in json.loads(body) for name in container.get("Names", [])]

    def kill(self, name):
        status, body = self._request("POST", f"/containers/{quote(name, safe='')}/kill")
        if status != NO_CONTENT:
            raise RuntimeError(f"Docker respondio {status} al matar {name}: {body[:200]!r}")

    def _request(self, method, path):
        connection = self._connect()
        try:
            connection.request(method, path)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()
