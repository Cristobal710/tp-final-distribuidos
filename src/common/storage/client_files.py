import os

from common.protocol import external


# Un archivo por cliente donde un nodo va dejando filas ya serializadas hasta que terminan los datos de ese cliente.
class ClientFiles:

    def __init__(self, directory, name):
        os.makedirs(directory, exist_ok=True)
        self._directory = directory
        self._name = name

    # Arma el camino del archivo de un cliente.
    def _path(self, client_id):
        return os.path.join(self._directory, f"{self._name}_{client_id}.jsonl")

    # Agrega filas ya serializadas al final del archivo del cliente.
    def append(self, client_id, serialized_rows):
        with open(self._path(client_id), "ab") as file:
            file.write(b"".join(row + b"\n" for row in serialized_rows))

    # Devuelve las filas del archivo del cliente tal como se guardaron, sin volver a armarlas.
    def read_raw(self, client_id):
        path = self._path(client_id)
        if not os.path.exists(path):
            return
        with open(path, "rb") as file:
            for line in file:
                yield line[:-1] if line.endswith(b"\n") else line

    # Devuelve las filas del archivo del cliente ya deserializadas.
    def read(self, client_id):
        for line in self.read_raw(client_id):
            yield external.deserialize(line)

    # Borra el archivo del cliente.
    def delete(self, client_id):
        try:
            os.remove(self._path(client_id))
        except FileNotFoundError:
            pass
