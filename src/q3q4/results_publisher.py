from common.protocol import internal
from q3q4.row_sender import RowSender


# Manda a un gateway los resultados de una consulta: abre la cola de resultados de cada gateway la primera vez que la
# usa, manda las filas como resultados de la consulta seguidas de su EOF, y las cierra todas juntas.
class ResultsPublisher:

    def __init__(self, open_queue, origin, query, max_rows=None, max_bytes=None):
        self._open_queue = open_queue
        self._sender = RowSender(origin, max_rows, max_bytes, query)
        self._queues = {}

    # Manda las filas ya serializadas al gateway del cliente, seguidas del EOF.
    # Devuelve cuantos mensajes de filas mando.
    def publish(self, client_id, gateway_id, serialized_rows, received):
        queue = self._queue(gateway_id)
        return self._sender.send(queue, internal.MsgType.RESULTS, client_id, gateway_id, serialized_rows, received)

    # Cierra las colas de resultados que abrio.
    def close(self):
        for queue in self._queues.values():
            queue.close()
        self._queues.clear()

    # Devuelve la cola de resultados del gateway, abriendola la primera vez.
    def _queue(self, gateway_id):
        if gateway_id not in self._queues:
            self._queues[gateway_id] = self._open_queue(gateway_id)
        return self._queues[gateway_id]
