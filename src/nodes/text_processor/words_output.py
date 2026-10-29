from common import partition
from common.batching import BatchBuilder
from common.protocol import articles, internal

ENGLISH = "en"
FRENCH = "fr"


# Salida de Q3 y Q4 del Text processor: junta las palabras de cada batch y las manda al contador de palabras que le toca.
class WordsOutput:

    def __init__(self, queue_prefix, shard_queues, max_rows, max_bytes):
        self._names = [f"{queue_prefix}.{shard}" for shard in range(len(shard_queues))]
        self._queues = list(shard_queues)
        self._max_rows = max_rows
        self._max_bytes = max_bytes

    # Cuenta en cuantos articulos del batch aparece cada palabra y en que idiomas, y manda cada palabra a su shard
    # segun su hash. Devuelve cuantos mensajes le mando a cada cola.
    def send_batch(self, client_id, gateway_id, texts):
        counts = {}
        for text in texts:
            for word in set(text.words):
                entry = counts.setdefault(word, [0, False, False])
                entry[0] += 1
                if text.language == ENGLISH:
                    entry[1] = True
                elif text.language == FRENCH:
                    entry[2] = True
        builders = [BatchBuilder(self._max_rows, self._max_bytes) for _ in self._queues]
        sent = {}
        for word, (article_count, english, french) in counts.items():
            shard = partition.shard_of(word, len(self._queues))
            row = articles.encode_row(internal.Word(word, article_count, english, french))
            closed = builders[shard].add(row)
            if closed is not None:
                self._send(client_id, gateway_id, shard, closed, sent)
        for shard, builder in enumerate(builders):
            last = builder.flush()
            if last is not None:
                self._send(client_id, gateway_id, shard, last, sent)
        return sent

    # Manda el aviso de fin a todos los shards, armado con lo que se le mando a cada uno.
    def send_eofs(self, client_id, sent, encode):
        for name, queue in zip(self._names, self._queues):
            queue.send(encode(sent.get(name, 0)))

    # Cierra las colas de los shards.
    def close(self):
        for queue in self._queues:
            queue.close()

    # Manda un mensaje de palabras a un shard y lo cuenta.
    def _send(self, client_id, gateway_id, shard, rows, sent):
        message = internal.encode_serialized_rows(internal.MsgType.WORDS, client_id, gateway_id, rows)
        self._queues[shard].send(message)
        sent[self._names[shard]] = sent.get(self._names[shard], 0) + 1
