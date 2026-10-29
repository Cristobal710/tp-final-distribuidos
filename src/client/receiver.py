import hashlib
import json
import threading
from pathlib import Path

from common.protocol import external

# Se encarga de escribir los resultados de las querys en los archivos de resultados
# para comparar contra el expected
class ResultReceiver(threading.Thread):

    def __init__(self, sock, output_dir, on_error):
        super().__init__()
        self._socket = sock
        self._output_dir = Path(output_dir)
        self._on_error = on_error
        self.error = None
        self.finished_queries = set()
        self.rows = {q: 0 for q in range(1, 6)}
        self.messages = {q: 0 for q in range(1, 6)}
        self.all_done = False

    def run(self):
        files = {}
        try:
            self._output_dir.mkdir(parents=True, exist_ok=True)
            for query in self.rows:
                files[query] = open(self._output_dir / f"q{query}.jsonl", "w")
            while True:
                kind, value = external.receive_message(self._socket)
                if kind == external.MsgType.ERROR:
                    raise external.ProtocolError(f"Gateway: {value}")
                if kind == external.MsgType.ALL_DONE:
                    if self.finished_queries != set(self.rows):
                        raise external.ProtocolError("ALL_DONE antes de terminar las cinco consultas")
                    self.all_done = True
                    try:
                        external.receive_raw_message(self._socket)
                    except ConnectionError:
                        return
                    raise external.ProtocolError("Datos adicionales despues de ALL_DONE")
                if not isinstance(value, dict) or type(value.get("query")) is not int or value["query"] not in self.rows:
                    raise external.ProtocolError("Consulta de resultado invalida")
                query = value["query"]
                if query in self.finished_queries:
                    raise external.ProtocolError(f"Datos o fin duplicado despues de terminar Q{query}")
                if kind == external.MsgType.QUERY_DONE:
                    self.finished_queries.add(query)
                elif kind == external.MsgType.RESULTS:
                    rows = value.get("rows")
                    if not isinstance(rows, list):
                        raise external.ProtocolError("Resultado sin filas")
                    for row in rows:
                        expected_length = 3 if query == 2 else 1 if query == 3 else 2
                        if not isinstance(row, list) or len(row) != expected_length:
                            raise external.ProtocolError("Fila de resultado invalida")
                        if query == 2:
                            row = self._save_image(row)
                        files[query].write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
                    files[query].flush()
                    self.rows[query] += len(rows)
                    self.messages[query] += 1
                else:
                    raise external.ProtocolError(f"Tipo de resultado inesperado: {kind}")
        except Exception as error:
            self.error = error
            self._on_error()
        finally:
            for file in files.values():
                file.close()

    def _save_image(self, row):
        name, url, values = row
        if not isinstance(values, list) or any(type(value) is not int or not 0 <= value <= 255 for value in values):
            raise external.ProtocolError("La imagen tiene que ser un array de bytes")
        data = bytes(values)
        digest = hashlib.sha256(data).hexdigest()
        images = self._output_dir / "images"
        images.mkdir(exist_ok=True)
        path = images / f"{digest}.bin"
        if not path.exists():
            path.write_bytes(data)
        return [name, url, digest, len(data)]

    def wait(self, timeout):
        self.join(timeout)
        if self.is_alive():
            self._on_error()
            self.join(5)
            raise TimeoutError("Se agoto el tiempo esperando los resultados")
        if self.error is not None:
            raise self.error
        if not self.all_done:
            raise external.ProtocolError("El cliente no recibio ALL_DONE")
