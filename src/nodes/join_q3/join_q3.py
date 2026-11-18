import logging

from common.eof import UpstreamTracker
from common.protocol import external, internal
from q3q4.results_publisher import ResultsPublisher
from q3q4.stage import Stage

Q3 = 3


# Junta las palabras compartidas que mandan todos los contadores de palabras y, cuando terminaron todos, las manda
# como resultado de Q3 al gateway del cliente.
class JoinQ3(Stage):

    def __init__(self, join_id, input_queue, files, open_results_queue, word_counter_amount, max_rows, max_bytes):
        origin = f"join_q3_{join_id}"
        upstream = UpstreamTracker(word_counter_amount, check_total=False, check_received=True)
        super().__init__(f"Join Q3 {join_id}", input_queue, upstream, internal.MsgType.SHARED_WORDS)
        self._files = files
        self._results = ResultsPublisher(open_results_queue, origin, Q3, max_rows, max_bytes)

    # Cierra las colas de resultados que abrio.
    def close(self):
        self._results.close()

    # Guarda en disco las palabras compartidas que manda un contador.
    def _on_data(self, client_id, value):
        self._files.append(client_id, [external.serialize(row) for row in value["rows"]])

    # Manda las palabras guardadas del cliente como resultado de Q3, seguidas del EOF, y borra el archivo.
    def _finish_client(self, client_id, gateway_id, received):
        sent = self._results.publish(client_id, gateway_id, self._files.read_raw(client_id), received)
        self._files.delete(client_id)
        logging.info(f"Cliente {client_id}: Q3 enviado en {sent} mensajes")
