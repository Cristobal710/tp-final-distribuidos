import datetime
import json
import struct
from enum import IntEnum

from common import safe_socket

HEADER = struct.Struct(">BI")
MAX_VALUE_SIZE = 64 * 1024 * 1024
EPOCH = datetime.datetime(1970, 1, 1)
EPOCH_UTC = EPOCH.replace(tzinfo=datetime.timezone.utc)
ONE_MILLISECOND = datetime.timedelta(milliseconds=1)


class MsgType(IntEnum):
    ARTICLES = 1
    END_OF_DATA = 2


class ProtocolError(Exception):
    pass


# Arma un mensaje: encabezado (tipo y largo) seguido del valor.
def encode_message(msg_type, value=b""):
    return HEADER.pack(msg_type, len(value)) + value


# Convierte las fechas en milisegundos desde 1970 para poder serializarlas.
def _serializable(value):
    if isinstance(value, datetime.datetime):
        epoch = EPOCH if value.tzinfo is None else EPOCH_UTC
        return (value - epoch) // ONE_MILLISECOND
    raise TypeError(f"No se puede serializar un {type(value).__name__}")


# Serializa un objeto a bytes (hoy JSON en UTF-8, sin espacios y con los acentos).
def serialize(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=_serializable).encode("utf-8")


# Une valores ya serializados en una lista serializada, sin volver a serializarlos.
def serialize_list(serialized_values):
    return b"[" + b",".join(serialized_values) + b"]"


# Une campos ya serializados en un objeto serializado, sin volver a serializarlos.
def serialize_fields(serialized_fields):
    entries = (serialize(name) + b":" + value for name, value in serialized_fields.items())
    return b"{" + b",".join(entries) + b"}"


# Convierte bytes serializados de vuelta en un objeto de Python.
def deserialize(value):
    return json.loads(value)


# Arma el mensaje y lo manda completo.
def send_message(sock, msg_type, value=b""):
    safe_socket.send_all(sock, encode_message(msg_type, value))


# Saca el tipo y el largo del encabezado y rechaza tipos desconocidos o largos excesivos.
def parse_header(header):
    msg_type, size = HEADER.unpack(header)
    try:
        msg_type = MsgType(msg_type)
    except ValueError as e:
        raise ProtocolError(f"Tipo de mensaje desconocido: {msg_type}") from e
    if size > MAX_VALUE_SIZE:
        raise ProtocolError(f"Mensaje de {size} bytes, el máximo es {MAX_VALUE_SIZE}")
    return msg_type, size


# Lee un mensaje completo y devuelve su valor en bytes, sin deserializar.
def receive_raw_message(sock):
    msg_type, size = parse_header(safe_socket.recv_all(sock, HEADER.size))
    value = safe_socket.recv_all(sock, size) if size else b""
    return msg_type, value


# Lee un mensaje completo y devuelve su valor ya deserializado.
def receive_message(sock):
    msg_type, value = receive_raw_message(sock)
    return msg_type, deserialize(value) if value else None
