import logging

from common.eof import UpstreamTracker
from common.protocol import articles, internal
from q3q4.ranking import best_words
from q3q4.results_publisher import ResultsPublisher
from q3q4.stage import Stage

Q4 = 4


# Junta los tops locales de todos los contadores de palabras y, cuando terminaron todos, manda las primeras palabras
# como resultado de Q4 al gateway del cliente.
class Top20(Stage):

    def __init__(self, top_id, input_queue, open_results_queue, word_counter_amount, top_size):
        origin = f"top20_{top_id}"
        upstream = UpstreamTracker(word_counter_amount, check_total=False, check_received=True)
        super().__init__(f"Top 20 final {top_id}", input_queue, upstream, internal.MsgType.TOP_WORDS)
        self._top_size = top_size
        self._results = ResultsPublisher(open_results_queue, origin, Q4)
        self._candidates = {}

    # Cierra las colas de resultados que abrio.
    def close(self):
        self._results.close()

    # Guarda los tops locales que manda un contador.
    def _on_data(self, client_id, value):
        self._candidates.setdefault(client_id, []).extend(value["top_words"])

    # Elige las palabras que aparecen en mas articulos, con empate por orden alfabetico, y las manda como resultado de Q4.
    def _finish_client(self, client_id, gateway_id, received):
        candidates = self._candidates.pop(client_id, [])
        top = best_words(candidates, self._top_size)
        self._results.publish(client_id, gateway_id, [articles.encode_row(row) for row in top], received)
        logging.info(f"Cliente {client_id}: Q4 enviado con {len(top)} palabras de {len(candidates)} candidatas")
