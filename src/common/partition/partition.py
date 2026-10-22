import zlib


# Elige a cual de las instancias le toca una clave, con un hash que da lo mismo en todos los procesos.
def shard_of(key, amount):
    return zlib.crc32(key.encode("utf-8")) % amount
