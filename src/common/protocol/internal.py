from enum import IntEnum

from . import articles, external


# Tipos de mensaje entre nodos.
class MsgType(IntEnum):
    ARTICLES = 1
    EOF = 2


# Arma el mensaje con un batch de articulos de un cliente para publicar en las colas internas.
def encode_articles(client_id, gateway_id, article_list):
    return external.serialize_fields({
        "type": external.serialize(MsgType.ARTICLES),
        "client_id": external.serialize(client_id),
        "gateway_id": external.serialize(gateway_id),
        "columns": external.serialize(list(articles.Article._fields)),
        "rows": external.serialize_list([articles.encode_row(article) for article in article_list]),
    })


# Arma el aviso de fin de datos de un cliente, indicando quien lo manda.
def encode_eof(client_id, origin):
    return external.serialize_fields({
        "type": external.serialize(MsgType.EOF),
        "client_id": external.serialize(client_id),
        "origin": external.serialize(origin),
    })


# Lee un mensaje interno, valida su tipo y, si trae articulos, los devuelve ya armados.
def decode(message):
    value = external.deserialize(message)
    if not isinstance(value, dict) or "type" not in value or "client_id" not in value:
        raise external.ProtocolError("Un mensaje interno tiene que tener tipo y client_id")
    try:
        msg_type = MsgType(value["type"])
    except ValueError as e:
        raise external.ProtocolError(f"Tipo de mensaje interno desconocido: {value['type']}") from e
    if msg_type == MsgType.ARTICLES:
        value["articles"] = articles.decode_batch(value)
    return msg_type, value
