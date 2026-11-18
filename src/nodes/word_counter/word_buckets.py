from common import partition
from common.protocol import external
from shard_summary import total_counts


# Los archivos donde un shard guarda las palabras de cada cliente, uno por bucket. Cada palabra va siempre al archivo
# que le toca por su hash, asi al contar se puede leer un archivo por vez sin que una palabra quede repartida.
class WordBuckets:

    def __init__(self, files):
        self._files = list(files)

    # Guarda las filas de un mensaje, cada una en el archivo que le toca a su palabra, con una escritura por archivo.
    def append(self, client_id, rows):
        rows_by_bucket = {}
        for row in rows:
            bucket = partition.bucket_of(row[0], len(self._files))
            rows_by_bucket.setdefault(bucket, []).append(external.serialize(row))
        for bucket, serialized_rows in rows_by_bucket.items():
            self._files[bucket].append(client_id, serialized_rows)

    # Devuelve, para cada archivo del cliente, lo contado de sus palabras. Va leyendo de a un archivo por vez.
    def counts_per_file(self, client_id):
        for file in self._files:
            yield total_counts(file.read(client_id))

    # Borra los archivos del cliente.
    def delete(self, client_id):
        for file in self._files:
            file.delete(client_id)
