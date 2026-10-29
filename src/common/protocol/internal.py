from enum import IntEnum
from typing import List, NamedTuple, Optional

from . import articles, external


# Tipos de mensaje entre nodos.
class MsgType(IntEnum):
    ARTICLES = 1
    EOF = 2
    TEXTS = 3
    BIOS = 4
    RESULTS = 5
    LATE_EOF = 6
    # Los tipos 7 a 9 son de Q3 y Q4.
    WORDS = 7
    SHARED_WORDS = 8
    TOP_WORDS = 9
    IMAGES = 10
    PARTIAL_AVERAGE = 11
    CANDIDATES = 12
    FINAL_AVERAGE = 13


# Campos que tiene que traer cada mensaje de control.
REQUIRED_FIELDS = {
    MsgType.EOF: ("gateway_id", "origin", "processed", "total", "sent"),
    MsgType.LATE_EOF: ("gateway_id", "origin", "processed", "sent"),
    MsgType.WORDS: ("gateway_id",),
    MsgType.SHARED_WORDS: ("gateway_id",),
    MsgType.TOP_WORDS: ("gateway_id",),
    MsgType.IMAGES: ("gateway_id",),
    MsgType.PARTIAL_AVERAGE: ("gateway_id",),
    MsgType.CANDIDATES: ("gateway_id",),
    MsgType.FINAL_AVERAGE: ("gateway_id",),
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


# Fila que va del filtro de biografias al descargador de imagenes (Q2).
class ImageRequest(NamedTuple):
    name: Optional[str]
    url: Optional[str]
    image_url: Optional[str]


# Los bytes originales de la imagen se transportan como enteros porque el protocolo usa JSON.
class Q2Result(NamedTuple):
    name: Optional[str]
    url: Optional[str]
    image_bytes: List[int]


# Fila del resultado de Q1.
class Q1Result(NamedTuple):
    url: Optional[str]
    date_modified: Optional[int]


# Fila que va de los Text processor a los contadores de palabras (Q3 y Q4): en cuantos articulos de un batch aparece
# la palabra y si aparece en alguno en ingles y en alguno en frances.
class Word(NamedTuple):
    word: str
    articles: int
    english: bool
    french: bool


# Fila que va de los contadores de palabras al Join Q3 y que es una fila del resultado de Q3.
class SharedWord(NamedTuple):
    word: str


# Fila que va de los contadores de palabras al Top 20 final y que es una fila del resultado de Q4.
class WordCount(NamedTuple):
    word: str
    articles: int


# Promedio parcial de articulos hasta 2020
class PartialAverage(NamedTuple):
    total_words: int
    article_count: int

# Promedio final exacto de Q5
class FinalAverage(NamedTuple):
    total_words: int
    article_count: int

# Articulos de 2021 a ser comparados con el FinalAverage Q5
class CandidateArticle(NamedTuple):
    url: Optional[str]
    word_count: int


class Q5Result(NamedTuple):
    url: Optional[str]
    word_count: int


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


def encode_images(client_id, gateway_id, images):
    return _encode_rows(MsgType.IMAGES, client_id, gateway_id, images)


def encode_partial_average(client_id, gateway_id, partial_average):
    return _encode_rows(MsgType.PARTIAL_AVERAGE, client_id, gateway_id, [partial_average])


def encode_candidates(client_id, gateway_id, candidates):
    return _encode_rows(MsgType.CANDIDATES, client_id, gateway_id, candidates)


def encode_average(client_id, gateway_id, average):
    return _encode_rows(MsgType.FINAL_AVERAGE, client_id, gateway_id, [average])


# Arma el mensaje con un batch de resultados de una consulta.
def encode_results(client_id, gateway_id, query, rows):
    return _encode_rows(MsgType.RESULTS, client_id, gateway_id, rows, {"query": external.serialize(query)})


# Arma un mensaje de un tipo con filas ya serializadas, por ejemplo las que junto un BatchBuilder.
def encode_serialized_rows(msg_type, client_id, gateway_id, serialized_rows, extra_fields=None):
    fields = {
        "type": external.serialize(msg_type),
        "client_id": external.serialize(client_id),
        "gateway_id": external.serialize(gateway_id),
    }
    fields.update(extra_fields or {})
    fields["rows"] = external.serialize_list(serialized_rows)
    return external.serialize_fields(fields)


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


def _nonnegative_count(value):
    if type(value) is not int or value < 0:
        raise external.ProtocolError("Los conteos tienen que ser enteros no negativos")


def _decode_average(value, row_type):
    rows = _decode_rows(value, row_type)
    if len(rows) != 1:
        raise external.ProtocolError("El mensaje necesita una unica fila de estadisticas")
    stats = rows[0]
    _nonnegative_count(stats.total_words)
    _nonnegative_count(stats.article_count)
    if stats.article_count == 0 and stats.total_words != 0:
        raise external.ProtocolError("Sin articulos, la suma de palabras tiene que ser cero")
    return stats


# Lee un mensaje interno, valida su tipo y sus campos y arma las filas segun el tipo.
def decode(message):
    value = external.deserialize(message)
    if not isinstance(value, dict) or "type" not in value or "client_id" not in value:
        raise external.ProtocolError("Un mensaje interno tiene que tener tipo y client_id")
    try:
        msg_type = MsgType(value["type"])
    except (ValueError, TypeError) as e:
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
    elif msg_type == MsgType.IMAGES:
        value["images"] = _decode_rows(value, ImageRequest)
    elif msg_type == MsgType.WORDS:
        value["words"] = _decode_rows(value, Word)
    elif msg_type == MsgType.SHARED_WORDS:
        value["shared_words"] = _decode_rows(value, SharedWord)
    elif msg_type == MsgType.TOP_WORDS:
        value["top_words"] = _decode_rows(value, WordCount)
    elif msg_type == MsgType.PARTIAL_AVERAGE:
        value["partial_average"] = _decode_average(value, PartialAverage)
    elif msg_type == MsgType.FINAL_AVERAGE:
        value["average"] = _decode_average(value, FinalAverage)
    elif msg_type == MsgType.CANDIDATES:
        value["candidates"] = _decode_rows(value, CandidateArticle)
        for candidate in value["candidates"]:
            _nonnegative_count(candidate.word_count)
            if candidate.url is not None and not isinstance(candidate.url, str):
                raise external.ProtocolError("La URL tiene que ser un string o null")
    return msg_type, value
