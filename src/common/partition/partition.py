import hashlib
import zlib

BUCKET_DIGEST_SIZE = 8


# Elige a cual de las instancias le toca una clave, con un hash que da lo mismo en todos los procesos.
def shard_of(key, amount):
    return zlib.crc32(key.encode("utf-8")) % amount


# Elige el archivo de una clave dentro de una instancia, con un hash distinto al de shard_of para que las claves de
# una instancia se repartan entre todos sus archivos.
def bucket_of(key, amount):
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=BUCKET_DIGEST_SIZE).digest()
    return int.from_bytes(digest, "big") % amount
