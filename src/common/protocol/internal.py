from enum import IntEnum
from typing import NamedTuple, Optional

from . import articles, external


# Tipos de mensaje entre nodos.
class MsgType(IntEnum):
    ARTICLES = 1
    EOF = 2
    TEXTS = 3
    BIOS = 4
    RESULTS = 5
    LATE_EOF = 6


# Campos que tiene que traer cada mensaje de control.
REQUIRED_FIELDS = {
    MsgType.EOF: ("gateway_id", "origin", "processed", "total", "sent"),
    MsgType.LATE_EOF: ("gateway_id", "origin", "processed", "sent"),
}


# Fila que va a la cola de texto (Q3, Q4 y Q5).
class Text(NamedTuple):
    url: Optional[str]
    abstract: Optional[str]
    until_2020: Optional[bool]
    language: Optional[str]


# Fila que va a la cola de bio (Q2).
class Bio(NamedTuple):
    name: Optional[str]
    url: Optional[str]
    sections: Optional[str]
    image_url: Optional[str]


# Fila del resultado de Q1.
class Q1Result(NamedTuple):
    url: Optional[str]
    date_modified: Optional[int]


# Arma el mensaje con un batch de articulos de un cliente para publicar en las colas internas.
def encode_articles(client_id, gateway_id, article_list):
    return external.serialize_fields({
        "type": external.serialize(MsgType.ARTICLES),
        "client_id": external.serialize(client_id),
        "gateway_id": external.serialize(gateway_id),
        "columns": external.serialize(list(articles.Article._fields)),
        "rows": external.serialize_list([articles.encode_row(article) for article in article_list]),
    })


# Arma un mensaje con un batch de filas de un tipo, sin nombres de columnas porque el tipo ya fija el orden.
def _encode_rows(msg_type, client_id, gateway_id, rows, extra_fields=None):
    fields = {
        "type": external.serialize(msg_type),
        "client_id": external.serialize(client_id),
        "gateway_id": external.serialize(gateway_id),
    }
    fields.update(extra_fields or {})
    fields["rows"] = external.serialize_list([articles.encode_row(row) for row in rows])
    return external.serialize_fields(fields)


# Arma el mensaje con un batch de filas para la cola de texto.
def encode_texts(client_id, gateway_id, texts):
    return _encode_rows(MsgType.TEXTS, client_id, gateway_id, texts)


# Arma el mensaje con un batch de filas para la cola de bio.
def encode_bios(client_id, gateway_id, bios):
    return _encode_rows(MsgType.BIOS, client_id, gateway_id, bios)


# Arma el mensaje con un batch de resultados de una consulta.
def encode_results(client_id, gateway_id, query, rows):
    return _encode_rows(MsgType.RESULTS, client_id, gateway_id, rows, {"query": external.serialize(query)})


# Arma el EOF de un cliente con lo que proceso quien lo manda, el total esperado y lo que le mando a este destino.
def encode_eof(client_id, gateway_id, origin, processed, total, sent, query=None):
    eof = {
        "type": MsgType.EOF,
        "client_id": client_id,
        "gateway_id": gateway_id,
        "origin": origin,
        "processed": processed,
        "total": total,
        "sent": sent,
    }
    if query is not None:
        eof["query"] = query
    return external.serialize(eof)


# Arma el aviso de un batch que se proceso despues de mandar el EOF de ese cliente.
def encode_late_eof(client_id, gateway_id, origin, processed, sent, query=None):
    late_eof = {
        "type": MsgType.LATE_EOF,
        "client_id": client_id,
        "gateway_id": gateway_id,
        "origin": origin,
        "processed": processed,
        "sent": sent,
    }
    if query is not None:
        late_eof["query"] = query
    return external.serialize(late_eof)


# Valida que cada fila tenga los campos del tipo y las convierte en ese tipo.
def _decode_rows(value, row_type):
    rows = value.get("rows")
    if not isinstance(rows, list):
        raise external.ProtocolError("El mensaje tiene que tener filas")
    for row in rows:
        if not isinstance(row, list) or len(row) != len(row_type._fields):
            raise external.ProtocolError(f"Fila con formato inválido: {row!r:.200}")
    return [row_type(*row) for row in rows]


# Lee un mensaje interno, valida su tipo y sus campos y arma las filas segun el tipo.
def decode(message):
    value = external.deserialize(message)
    if not isinstance(value, dict) or "type" not in value or "client_id" not in value:
        raise external.ProtocolError("Un mensaje interno tiene que tener tipo y client_id")
    try:
        msg_type = MsgType(value["type"])
    except ValueError as e:
        raise external.ProtocolError(f"Tipo de mensaje interno desconocido: {value['type']}") from e
    missing = [field for field in REQUIRED_FIELDS.get(msg_type, ()) if field not in value]
    if missing:
        raise external.ProtocolError(f"Al mensaje le faltan campos: {', '.join(missing)}")
    if msg_type == MsgType.ARTICLES:
        value["articles"] = articles.decode_batch(value)
    elif msg_type == MsgType.TEXTS:
        value["texts"] = _decode_rows(value, Text)
    elif msg_type == MsgType.BIOS:
        value["bios"] = _decode_rows(value, Bio)
    return msg_type, value
