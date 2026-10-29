import logging

from common import partition
from common.batching import chunk_rows
from common.protocol import articles, internal


def distinct_word_count(words):
    return len(set(words))


class Q5Output:

    def __init__(self, average_prefix, average_queues, comparison_prefix, comparison_queues, max_rows, max_bytes):
        if not average_queues or not comparison_queues or max_rows < 1 or max_bytes < 1:
            raise ValueError("Q5 necesita colas y limites positivos")
        self._averages = list(average_queues)
        self._comparators = list(comparison_queues)
        self._average_names = [f"{average_prefix}.{i}" for i in range(len(self._averages))]
        self._comparison_names = [f"{comparison_prefix}.{i}" for i in range(len(self._comparators))]
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._next_comparator = 0

    def send_batch(self, client_id, gateway_id, texts):
        total_words, article_count = 0, 0
        candidates = []
        for text in texts:
            count = distinct_word_count(text.words)
            if text.until_2020 is True:
                total_words += count
                article_count += 1
            elif text.until_2020 is False:
                candidates.append(articles.encode_row(internal.CandidateArticle(text.url, count)))
        sent = {}
        if article_count:
            shard = partition.shard_of(client_id, len(self._averages))
            message = internal.encode_partial_average(
                client_id, gateway_id, internal.PartialAverage(total_words, article_count),
            )
            if len(message) > self._max_bytes:
                raise ValueError("El mensaje de estadisticas de Q5 supera MAX_BATCH_BYTES")
            self._averages[shard].send(message)
            sent[self._average_names[shard]] = 1
        for rows in chunk_rows(candidates, self._max_rows, self._max_bytes):
            self._send_candidates(client_id, gateway_id, rows, sent)
        return sent

    def _send_candidates(self, client_id, gateway_id, rows, sent):
        message = internal.encode_serialized_rows(internal.MsgType.CANDIDATES, client_id, gateway_id, rows)
        if len(message) > self._max_bytes:
            if len(rows) == 1:
                logging.warning("Cliente %s: candidato Q5 demasiado grande descartado", client_id)
                return
            middle = len(rows) // 2
            self._send_candidates(client_id, gateway_id, rows[:middle], sent)
            self._send_candidates(client_id, gateway_id, rows[middle:], sent)
            return
        target = self._next_comparator
        self._comparators[target].send(message)
        name = self._comparison_names[target]
        sent[name] = sent.get(name, 0) + 1
        self._next_comparator = (target + 1) % len(self._comparators)

    def send_eofs(self, client_id, sent, encode):
        shard = partition.shard_of(client_id, len(self._averages))
        self._averages[shard].send(encode(sent.get(self._average_names[shard], 0)))
        for name, queue in zip(self._comparison_names, self._comparators):
            queue.send(encode(sent.get(name, 0)))

    def close(self):
        for queue in self._averages + self._comparators:
            queue.close()
