import logging

from common import partition
from common.eof import UpstreamTracker
from common.protocol import articles, internal
from q3q4.row_sender import RowSender
from q3q4.stage import Stage
from shard_summary import ShardSummary
from word_buckets import WordBuckets


# Cuenta las palabras de su shard: guarda en disco lo que le llega de los Text processor, repartido en varios archivos
# por palabra, y cuando terminaron los datos de un cliente manda las palabras compartidas al Join Q3 y su top local al
# Top 20 final.
class WordCounter(Stage):

    def __init__(self, counter_id, input_queue, buckets, join_queues, top_queues, text_processor_amount,
                 top_size, max_rows, max_bytes):
        origin = f"word_counter_{counter_id}"
        upstream = UpstreamTracker(text_processor_amount, check_total=True, check_received=True)
        super().__init__(f"Contador de palabras {counter_id}", input_queue, upstream, internal.MsgType.WORDS)
        self._buckets = WordBuckets(buckets)
        self._join_queues = join_queues
        self._top_queues = top_queues
        self._top_size = top_size
        self._sender = RowSender(origin, max_rows, max_bytes)

    # Cierra las colas de salida.
    def close(self):
        for queue in self._join_queues + self._top_queues:
            queue.close()

    # Guarda las palabras que llegan de un Text processor.
    def _on_data(self, client_id, value):
        self._buckets.append(client_id, value["rows"])

    # Manda las palabras compartidas y el top local del cliente, armados de a un archivo por vez, y borra los archivos.
    def _finish_client(self, client_id, gateway_id, received):
        summary = ShardSummary(self._top_size)
        shared_words = summary.shared_words(self._buckets.counts_per_file(client_id))
        shared = (articles.encode_row(internal.SharedWord(word)) for word in shared_words)
        index = partition.shard_of(client_id, len(self._join_queues))
        self._sender.send(self._join_queues[index], internal.MsgType.SHARED_WORDS, client_id, gateway_id, shared, received)
        top = [articles.encode_row(internal.WordCount(word, article_count)) for word, article_count in summary.top]
        index = partition.shard_of(client_id, len(self._top_queues))
        self._sender.send(self._top_queues[index], internal.MsgType.TOP_WORDS, client_id, gateway_id, top, received)
        self._buckets.delete(client_id)
        logging.info(f"Cliente {client_id}: palabras compartidas y top local enviados")
