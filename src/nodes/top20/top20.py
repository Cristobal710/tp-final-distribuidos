import logging

from common.eof import UpstreamTracker
from common.protocol import external, internal

Q4 = 4


# Junta los tops locales de todos los contadores de palabras y, cuando terminaron todos, manda las primeras palabras
# como resultado de Q4 al gateway del cliente.
class Top20:

    def __init__(self, top_id, input_queue, open_results_queue, word_counter_amount, top_size):
        self._top_id = top_id
        self._origin = f"top20_{top_id}"
        self._input_queue = input_queue
        self._open_results_queue = open_results_queue
        self._top_size = top_size
        self._results_queues = {}
        self._candidates = {}
        self._word_counters = UpstreamTracker(word_counter_amount, check_total=False, check_received=True)

    # Consume la cola del Top 20 final hasta que se pida parar.
    def run(self):
        logging.info(f"Top 20 final {self._top_id} consumiendo")
        self._input_queue.start_consuming(self._on_message)
        logging.info(f"Top 20 final {self._top_id} detenido")

    # Pide cortar el consumo, desde el handler de SIGTERM.
    def stop(self):
        self._input_queue.stop_consuming()

    # Cierra las colas de resultados que abrio.
    def close(self):
        for results_queue in self._results_queues.values():
            results_queue.close()
        self._results_queues.clear()

    # Guarda los tops de cada mensaje, anota los EOF de los contadores y termina al cliente cuando estan todos.
    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error(f"Mensaje inválido descartado: {e}")
            ack()
            return
        client_id = value["client_id"]
        if msg_type == internal.MsgType.TOP_WORDS:
            self._candidates.setdefault(client_id, []).extend(value["top_words"])
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

    # Elige las palabras que aparecen en mas articulos, con empate por orden alfabetico, y las manda como resultado de Q4.
    def _finish_client(self, client_id, gateway_id):
        received = self._word_counters.received(client_id)
        candidates = self._candidates.pop(client_id, [])
        top = sorted(candidates, key=lambda candidate: (-candidate.articles, candidate.word))[:self._top_size]
        results_queue = self._results_queue(gateway_id)
        sent = 0
        if top:
            results_queue.send(internal.encode_results(client_id, gateway_id, Q4, top))
            sent = 1
        results_queue.send(internal.encode_eof(client_id, gateway_id, self._origin, received, received, sent, Q4))
        self._word_counters.forget(client_id)
        logging.info(f"Cliente {client_id}: Q4 enviado con {len(top)} palabras de {len(candidates)} candidatas")

    # Devuelve la cola de resultados del gateway, abriendola la primera vez.
    def _results_queue(self, gateway_id):
        if gateway_id not in self._results_queues:
            self._results_queues[gateway_id] = self._open_results_queue(gateway_id)
        return self._results_queues[gateway_id]
