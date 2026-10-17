import logging
import os
import queue
import threading

import kagglehub
from kagglehub.exceptions import KaggleApiHTTPError

NOT_FOUND = 404


# Marca que el descargador pone en la cola cuando ya no hay mas archivos.
class _Done:
    pass


# Baja los archivos del dataset de Kaggle a una carpeta propia del cliente.
class KaggleDataset:

    def __init__(self, output_dir, dataset, file_patterns, files_per_pattern, files_ahead, poll_seconds,
                 stop_timeout_seconds, download=kagglehub.dataset_download):
        self._output_dir = output_dir
        self._dataset = dataset
        self._file_patterns = file_patterns
        self._files_per_pattern = files_per_pattern
        self._files_ahead = files_ahead
        self._poll_seconds = poll_seconds
        self._stop_timeout_seconds = stop_timeout_seconds
        self._download = download

    # Devuelve los archivos de a uno mientras baja el siguiente en otro hilo, y borra cada uno despues de usarlo.
    def files(self):
        ready = queue.Queue(maxsize=self._files_ahead)
        stop = threading.Event()
        downloader = threading.Thread(target=self._download_all, args=(ready, stop), daemon=True)
        downloader.start()
        try:
            while True:
                item = ready.get()
                if isinstance(item, _Done):
                    return
                if isinstance(item, Exception):
                    raise item
                file_path, local_path = item
                try:
                    yield file_path, local_path
                finally:
                    _remove(local_path)
        finally:
            stop.set()
            self._discard(ready)
            downloader.join(timeout=self._stop_timeout_seconds)

    # Baja los archivos de cada patron en orden y los deja en la cola, hasta que no haya mas o se pida parar.
    def _download_all(self, ready, stop):
        try:
            for pattern in self._file_patterns:
                index = 0
                while not stop.is_set() and (self._files_per_pattern == 0 or index < self._files_per_pattern):
                    file_path = pattern.format(index)
                    local_path = self._download_one(file_path)
                    if local_path is None:
                        logging.info(f"No hay más archivos de {pattern}: {index} en total")
                        break
                    if not self._put(ready, (file_path, local_path), stop):
                        _remove(local_path)
                        return
                    index += 1
            self._put(ready, _Done(), stop)
        except Exception as e:
            self._put(ready, e, stop)

    # Baja un archivo y devuelve su ruta local, o None si Kaggle dice que no existe.
    def _download_one(self, file_path):
        try:
            return self._download(self._dataset, path=file_path, output_dir=self._output_dir)
        except KaggleApiHTTPError as e:
            if getattr(e.response, "status_code", None) == NOT_FOUND:
                return None
            raise

    # Pone un item en la cola esperando lugar, salvo que se pida parar mientras espera.
    def _put(self, ready, item, stop):
        while not stop.is_set():
            try:
                ready.put(item, timeout=self._poll_seconds)
                return True
            except queue.Full:
                pass
        return False

    # Vacia la cola y borra los archivos que quedaron sin usar.
    @staticmethod
    def _discard(ready):
        while True:
            try:
                item = ready.get_nowait()
            except queue.Empty:
                return
            if isinstance(item, tuple):
                _remove(item[1])


# Borra un archivo si existe.
def _remove(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
