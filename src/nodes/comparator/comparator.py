import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass

from common.batching import chunk_rows
from common.eof import UpstreamTracker
from common.protocol import external, internal

Q5 = 5
RECENT_CLIENTS = 1024
STOP_POLL_SECONDS = 1


def exceeds_average(word_count, baseline):
    return baseline.article_count > 0 and word_count * baseline.article_count > baseline.total_words


@dataclass
class _PendingDrain:
    source: object
    chunks: object
    acknowledgements: list

    def close(self):
        self.chunks.close()
        self.source.close()


class Comparator:

    def __init__(self, comparator_id, input_queue, average_input, files, open_results_queue,
                 text_processor_amount, max_rows, max_bytes, drain_rows=1000):
        if min(text_processor_amount, max_rows, max_bytes, drain_rows) < 1:
            raise ValueError("Las cantidades y los limites tienen que ser positivos")
        self._origin = f"comparator_{comparator_id}"
        self._input_queue = input_queue
        self._average_input = average_input
        self._files = files
        self._open_results_queue = open_results_queue
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._drain_rows = drain_rows
        self._upstream = UpstreamTracker(text_processor_amount, check_total=True, check_received=True)
        self._baselines = {}
        self._drains = {}
        self._sent = {}
        self._results_queues = {}
        self._finished = OrderedDict()
        self._stopping = False
        self._average_thread = threading.Thread(target=self._consume_averages)

    def run(self):
        logging.info("%s consumiendo candidatos Q5", self._origin)
        self._average_thread.start()
        try:
            if not self._stopping:
                self._input_queue.start_consuming(self._on_message)
        finally:
            self._stopping = True
            while self._average_thread.is_alive():
                self._average_input.stop_consuming()
                self._average_thread.join(timeout=STOP_POLL_SECONDS)

    def stop(self):
        self._stopping = True
        self._input_queue.stop_consuming()

    def close(self):
        self._stopping = True
        for drain in self._drains.values():
            drain.close()
        self._drains.clear()
        for queue in self._results_queues.values():
            try:
                queue.close()
            except Exception:
                logging.exception("No se pudo cerrar una salida de %s", self._origin)
        self._results_queues.clear()

    def _consume_averages(self):
        try:
            if not self._stopping:
                self._average_input.start_consuming(self._on_average_message)
        except Exception:
            logging.exception("Se corto la entrada de promedios de %s", self._origin)
            self.stop()

    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error("Mensaje Q5 invalido descartado: %s", e)
            ack()
            return
        client_id = value["client_id"]
        if client_id in self._finished:
            ack()
            return
        if msg_type == internal.MsgType.CANDIDATES:
            self._process_articles(client_id, value["gateway_id"], value["candidates"])
        elif msg_type == internal.MsgType.EOF:
            self._upstream.add_eof(value)
        elif msg_type == internal.MsgType.LATE_EOF:
            self._upstream.add_late_eof(value)
        else:
            logging.warning("Mensaje %s ignorado por %s", msg_type.name, self._origin)
            ack()
            return
        self._try_finish_client(client_id, value["gateway_id"])
        ack()

    def _process_articles(self, client_id, gateway_id, articles):
        if client_id in self._baselines:
            self._publish_matches(client_id, gateway_id, articles)
        else:
            self._files.append(client_id, (external.serialize(article) for article in articles))
        self._upstream.add_message(client_id)

    def _on_average_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
            if msg_type != internal.MsgType.FINAL_AVERAGE:
                raise external.ProtocolError("Se esperaba FINAL_AVERAGE")
        except external.ProtocolError as e:
            logging.error("Promedio Q5 invalido descartado: %s", e)
            ack()
            return
        self._input_queue.run_in_consumer_thread(
            lambda: self._accept_average(value["client_id"], value["gateway_id"], value["average"], ack),
        )

    def _ack_average(self, ack):
        self._average_input.run_in_consumer_thread(ack)

    def _accept_average(self, client_id, gateway_id, baseline, ack):
        if self._stopping:
            return
        if client_id in self._finished:
            self._ack_average(ack)
            return
        if client_id in self._baselines:
            if self._baselines[client_id] != baseline:
                logging.error("Cliente %s: promedio Q5 contradictorio descartado", client_id)
                self._ack_average(ack)
            elif client_id in self._drains:
                self._drains[client_id].acknowledgements.append(ack)
            else:
                self._ack_average(ack)
            return
        self._baselines[client_id] = baseline
        source = self._files.read_raw(client_id)
        self._drains[client_id] = _PendingDrain(
            source, chunk_rows(source, self._drain_rows, self._max_bytes), [ack],
        )
        self._input_queue.run_in_consumer_thread(lambda: self._drain_pending(client_id, gateway_id))

    def _drain_pending(self, client_id, gateway_id):
        if self._stopping:
            return
        drain = self._drains[client_id]
        try:
            rows = next(drain.chunks)
        except StopIteration:
            drain.close()
            self._files.delete(client_id)
            del self._drains[client_id]
            for ack in drain.acknowledgements:
                self._ack_average(ack)
            self._try_finish_client(client_id, gateway_id)
            return
        candidates = (internal.CandidateArticle(*external.deserialize(row)) for row in rows)
        self._publish_matches(client_id, gateway_id, candidates)
        self._input_queue.run_in_consumer_thread(lambda: self._drain_pending(client_id, gateway_id))

    def _publish_matches(self, client_id, gateway_id, articles):
        baseline = self._baselines[client_id]
        rows = (
            external.serialize(internal.Q5Result(article.url, article.word_count))
            for article in articles if exceeds_average(article.word_count, baseline)
        )
        for chunk in chunk_rows(rows, self._max_rows, self._max_bytes):
            self._publish_rows(client_id, gateway_id, chunk)

    def _publish_rows(self, client_id, gateway_id, rows):
        message = internal.encode_serialized_rows(
            internal.MsgType.RESULTS, client_id, gateway_id, rows, {"query": external.serialize(Q5)},
        )
        if len(message) > self._max_bytes:
            if len(rows) == 1:
                logging.warning("Cliente %s: resultado Q5 demasiado grande descartado", client_id)
                return
            middle = len(rows) // 2
            self._publish_rows(client_id, gateway_id, rows[:middle])
            self._publish_rows(client_id, gateway_id, rows[middle:])
            return
        self._results_queue(gateway_id).send(message)
        self._sent[client_id] = self._sent.get(client_id, 0) + 1

    def _try_finish_client(self, client_id, gateway_id):
        if (client_id in self._baselines and client_id not in self._drains
                and self._upstream.is_complete(client_id)):
            self._finish_client(client_id, gateway_id)

    def _finish_client(self, client_id, gateway_id):
        self._results_queue(gateway_id).send(internal.encode_eof(
            client_id, gateway_id, self._origin, self._upstream.received(client_id),
            self._upstream.total_sent(client_id), self._sent.get(client_id, 0), Q5,
        ))
        self._files.delete(client_id)
        self._baselines.pop(client_id, None)
        self._sent.pop(client_id, None)
        self._upstream.forget(client_id)
        self._finished[client_id] = None
        if len(self._finished) > RECENT_CLIENTS:
            self._finished.popitem(last=False)

    def _results_queue(self, gateway_id):
        if gateway_id not in self._results_queues:
            self._results_queues[gateway_id] = self._open_results_queue(gateway_id)
        return self._results_queues[gateway_id]
