from common.batching import chunk_rows
from common.protocol import external, internal


# Manda filas ya serializadas a una cola, partidas en mensajes que respetan los topes, y cierra con el EOF que dice
# cuantos mensajes salieron. Si se pasa query, los mensajes y el EOF son de esa consulta. Sin topes, todas las filas
# van en un solo mensaje.
class RowSender:

    def __init__(self, origin, max_rows=None, max_bytes=None, query=None):
        self._origin = origin
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._query = query

    # Manda las filas y despues el EOF. received es lo que recibio este nodo de la etapa anterior para ese cliente.
    # Devuelve cuantos mensajes de filas mando.
    def send(self, queue, msg_type, client_id, gateway_id, serialized_rows, received):
        extra_fields = {"query": external.serialize(self._query)} if self._query is not None else None
        sent = 0
        for chunk in self._chunks(serialized_rows):
            queue.send(internal.encode_serialized_rows(msg_type, client_id, gateway_id, chunk, extra_fields))
            sent += 1
        queue.send(internal.encode_eof(client_id, gateway_id, self._origin, received, received, sent, self._query))
        return sent

    # Parte las filas segun los topes, o las junta en un solo grupo si no hay topes.
    def _chunks(self, serialized_rows):
        if self._max_rows is None:
            rows = list(serialized_rows)
            return [rows] if rows else []
        return chunk_rows(serialized_rows, self._max_rows, self._max_bytes)
