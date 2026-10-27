import heapq
import itertools
import logging

from common import partition
from common.batching import chunk_rows
from common.eof import UpstreamTracker
from common.protocol import articles, external, internal


# Suma las filas de palabras guardadas: en cuantos articulos aparece cada una y si aparece en ingles y en frances.
def total_counts(rows):
    counts = {}
    for word, article_count, english, french in rows:
        entry = counts.get(word)
        if entry is None:
            counts[word] = [article_count, english, french]
        else:
            entry[0] += article_count
            entry[1] = entry[1] or english
            entry[2] = entry[2] or french
    return counts


# Cuenta las palabras de su shard: guarda en disco lo que le llega de los Text processor, repartido en varios archivos
# por palabra, y cuando terminaron los datos de un cliente manda las palabras compartidas al Join Q3 y su top local al
# Top 20 final.
class WordCounter:

    def __init__(self, counter_id, input_queue, buckets, join_queues, top_queues, text_processor_amount,
                 top_size, max_rows, max_bytes):
        self._counter_id = counter_id
        self._origin = f"word_counter_{counter_id}"
        self._input_queue = input_queue
        self._buckets = list(buckets)
        self._join_queues = join_queues
        self._top_queues = top_queues
        self._top_size = top_size
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._text_processors = UpstreamTracker(text_processor_amount, check_total=True, check_received=True)

    # Consume la cola de palabras de este shard hasta que se pida parar.
    def run(self):
        logging.info(f"Contador de palabras {self._counter_id} consumiendo")
        self._input_queue.start_consuming(self._on_message)
        logging.info(f"Contador de palabras {self._counter_id} detenido")

    # Pide cortar el consumo, desde el handler de SIGTERM.
    def stop(self):
        self._input_queue.stop_consuming()

    # Cierra las colas de salida.
    def close(self):
        for queue in self._join_queues + self._top_queues:
            queue.close()

    # Guarda las palabras de cada mensaje, anota los EOF de los Text processor y termina al cliente cuando esta todo.
    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error(f"Mensaje inválido descartado: {e}")
            ack()
            return
        client_id = value["client_id"]
        if msg_type == internal.MsgType.WORDS:
            self._store(client_id, value["rows"])
            self._text_processors.add_message(client_id)
        elif msg_type == internal.MsgType.EOF:
            self._text_processors.add_eof(value)
        elif msg_type == internal.MsgType.LATE_EOF:
            self._text_processors.add_late_eof(value)
        else:
            logging.warning(f"Mensaje de tipo {msg_type.name} ignorado")
        if self._text_processors.is_complete(client_id):
            self._finish_client(client_id, value["gateway_id"])
        ack()

    # Guarda las filas de un mensaje, cada una en el archivo que le toca a su palabra, con una escritura por archivo.
    def _store(self, client_id, rows):
        rows_by_bucket = {}
        for row in rows:
            bucket = partition.bucket_of(row[0], len(self._buckets))
            rows_by_bucket.setdefault(bucket, []).append(external.serialize(row))
        for bucket, serialized_rows in rows_by_bucket.items():
            self._buckets[bucket].append(client_id, serialized_rows)

    # Manda las palabras compartidas y el top local del cliente, armados de a un archivo por vez, y borra los archivos.
    def _finish_client(self, client_id, gateway_id):
        received = self._text_processors.received(client_id)
        top = []
        index = partition.shard_of(client_id, len(self._join_queues))
        shared = self._shared_rows(client_id, top)
        self._send_rows(self._join_queues[index], internal.MsgType.SHARED_WORDS, client_id, gateway_id, shared, received)
        top_rows = [articles.encode_row(internal.WordCount(word, article_count)) for word, article_count in top]
        index = partition.shard_of(client_id, len(self._top_queues))
        self._send_rows(self._top_queues[index], internal.MsgType.TOP_WORDS, client_id, gateway_id, top_rows, received)
        for bucket in self._buckets:
            bucket.delete(client_id)
        self._text_processors.forget(client_id)
        logging.info(f"Cliente {client_id}: palabras compartidas y top local enviados")

    # Recorre los archivos del cliente de a uno: devuelve las palabras compartidas de cada uno y va dejando en top las
    # palabras con mas articulos, asi en memoria hay un solo archivo a la vez.
    def _shared_rows(self, client_id, top):
        for bucket in self._buckets:
            counts = total_counts(bucket.read(client_id))
            for word, (article_count, english, french) in counts.items():
                if english and french:
                    yield articles.encode_row(internal.SharedWord(word))
            candidates = itertools.chain(top, ((word, entry[0]) for word, entry in counts.items()))
            top[:] = heapq.nsmallest(self._top_size, candidates, key=lambda item: (-item[1], item[0]))

    # Manda las filas a una cola en mensajes del tipo dado, seguidas del EOF con lo que se mando.
    def _send_rows(self, queue, msg_type, client_id, gateway_id, rows, received):
        sent = 0
        for chunk in chunk_rows(rows, self._max_rows, self._max_bytes):
            queue.send(internal.encode_serialized_rows(msg_type, client_id, gateway_id, chunk))
            sent += 1
        queue.send(internal.encode_eof(client_id, gateway_id, self._origin, received, received, sent))
