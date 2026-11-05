import fcntl
import hashlib
import logging
import os
import tempfile
from contextlib import contextmanager, suppress
from pathlib import Path


class ImageCache:

    def __init__(self, directory):
        self._directory = Path(directory)
        try:
            self._directory.mkdir(parents=True, exist_ok=True)
        except OSError:
            logging.warning("No se pudo inicializar la cache de imagenes", exc_info=True)

    @contextmanager
    def _locked(self):
        self._directory.mkdir(parents=True, exist_ok=True)
        with (self._directory / "cache.lock").open("ab") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def _path(self, url):
        return self._directory / (hashlib.sha256(url.encode("utf-8")).hexdigest() + ".img")

    def get(self, url):
        try:
            with self._locked():
                path = self._path(url)
                data = path.read_bytes()
                return data or None
        except FileNotFoundError:
            return None
        except OSError:
            logging.warning("No se pudo leer la cache de imagenes", exc_info=True)
        return None

    def put(self, url, data):
        if not data:
            return
        temporary = None
        try:
            with self._locked():
                path = self._path(url)
                with tempfile.NamedTemporaryFile(dir=self._directory, prefix=".image-", suffix=".tmp",
                                                 delete=False) as file:
                    temporary = Path(file.name)
                    file.write(data)
                os.replace(temporary, path)
        except OSError:
            logging.warning("No se pudo guardar la imagen en cache", exc_info=True)
        finally:
            if temporary is not None:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)
