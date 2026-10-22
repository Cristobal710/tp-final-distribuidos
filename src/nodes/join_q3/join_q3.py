import logging

from common.batching import chunk_rows
from common.eof import UpstreamTracker
from common.protocol import external, internal

Q3 = 3


# Junta las palabras compartidas que mandan todos los contadores de palabras y, cuando terminaron todos, las manda
# como resultado de Q3 al gateway del cliente.
class JoinQ3:

    def __init__(self, join_id, input_queue, files, open_results_queue, word_counter_amount, max_rows, max_bytes):
        self._join_id = join_id
        self._origin = f"join_q3_{join_id}"
        self._input_queue = input_queue
        self._files = files
        self._open_results_queue = open_results_queue
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._results_queues = {}
        self._word_counters = UpstreamTracker(word_counter_amount, check_total=False, check_received=True)

    # Consume la cola del Join Q3 hasta que se pida parar.
    def run(self):
        logging.info(f"Join Q3 {self._join_id} consumiendo")
        self._input_queue.start_consuming(self._on_message)
        logging.info(f"Join Q3 {self._join_id} detenido")

    # Pide cortar el consumo, desde el handler de SIGTERM.
    def stop(self):
        self._input_queue.stop_consuming()

    # Cierra las colas de resultados que abrio.
    def close(self):
        for results_queue in self._results_queues.values():
            results_queue.close()
        self._results_queues.clear()

    # Guarda las palabras de cada mensaje, anota los EOF de los contadores y termina al cliente cuando estan todos.
    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error(f"Mensaje inválido descartado: {e}")
            ack()
            return
        client_id = value["client_id"]
        if msg_type == internal.MsgType.SHARED_WORDS:
            self._files.append(client_id, [external.serialize(row) for row in value["rows"]])
            self._word_counters.add_message(client_id)
        elif msg_type == internal.MsgType.EOF:
            self._word_counters.add_eof(value)
        elif msg_type == internal.MsgType.LATE_EOF:
            self._word_counters.add_late_eof(value)
        else:
            logging.warning(f"Mensaje de tipo {msg_type.name} ignorado")
        if self._word_counters.is_complete(client_id):
            self._finish_client(client_id, value["gateway_id"])
        ack()

    # Manda las palabras guardadas del cliente como resultado de Q3, seguidas del EOF, y borra el archivo.
    def _finish_client(self, client_id, gateway_id):
        received = self._word_counters.received(client_id)
        results_queue = self._results_queue(gateway_id)
        sent = 0
        for chunk in chunk_rows(self._files.read_raw(client_id), self._max_rows, self._max_bytes):
            results_queue.send(internal.encode_serialized_rows(
                internal.MsgType.RESULTS, client_id, gateway_id, chunk, {"query": external.serialize(Q3)}
            ))
            sent += 1
        results_queue.send(internal.encode_eof(client_id, gateway_id, self._origin, received, received, sent, Q3))
        self._files.delete(client_id)
        self._word_counters.forget(client_id)
        logging.info(f"Cliente {client_id}: Q3 enviado en {sent} mensajes")

    # Devuelve la cola de resultados del gateway, abriendola la primera vez.
    def _results_queue(self, gateway_id):
        if gateway_id not in self._results_queues:
            self._results_queues[gateway_id] = self._open_results_queue(gateway_id)
        return self._results_queues[gateway_id]
