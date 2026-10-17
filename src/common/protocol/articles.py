from typing import NamedTuple, Optional

from . import external


# Las 8 columnas del dataset que usan las consultas.
class Article(NamedTuple):
    name: Optional[str]
    url: Optional[str]
    in_language: Optional[dict]
    date_created: Optional[int]
    date_modified: Optional[int]
    abstract: Optional[str]
    sections: Optional[str]
    image: Optional[dict]


# Serializa una fila como lista de valores, en el orden de sus columnas.
def encode_row(values):
    return external.serialize(list(values))


# Arma un batch con los nombres de las columnas y las filas ya serializadas.
def encode_batch(columns, encoded_rows):
    return external.serialize_fields({
        "columns": external.serialize(list(columns)),
        "rows": external.serialize_list(encoded_rows),
    })


# Devuelve en que posicion de la fila esta cada campo de Article, o falla si falta alguno.
def _positions_of_article_fields(columns):
    missing = [field for field in Article._fields if field not in columns]
    if missing:
        raise external.ProtocolError(f"Al batch le faltan columnas: {', '.join(missing)}")
    return [columns.index(field) for field in Article._fields]


# Convierte un batch ya deserializado en Articles, quedandose solo con sus columnas.
def decode_batch(value):
    if not isinstance(value, dict):
        raise external.ProtocolError("Un batch tiene que tener columnas y filas")
    columns = value.get("columns")
    rows = value.get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise external.ProtocolError("Un batch tiene que tener columnas y filas")
    positions = _positions_of_article_fields(columns)
    articles = []
    for row in rows:
        if not isinstance(row, list) or len(row) != len(columns):
            raise external.ProtocolError(f"Fila con formato inválido: {row!r:.200}")
        articles.append(Article(*(row[i] for i in positions)))
    return articles
