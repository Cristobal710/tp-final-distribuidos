import logging
from collections import OrderedDict

from common.eof import UpstreamTracker
from common.protocol import external, internal

RECENT_CLIENTS = 1024

# Junta resultados parciales de los articulos para calcular el promedio, se comunica con el comparator una vez que recibe el EOF
class Average:

    def __init__(self, average_id, input_queue, comparator_queues, text_processor_amount):
        if not comparator_queues or text_processor_amount < 1:
            raise ValueError("Se necesitan comparadores y Text processors")
        self._origin = f"average_{average_id}"
        self._input_queue = input_queue
        self._comparator_queues = list(comparator_queues)
        self._upstream = UpstreamTracker(text_processor_amount, check_total=True, check_received=True)
        self._totals = {}
        self._finished = OrderedDict()
        self._stopping = False

    def run(self):
        logging.info("%s consumiendo estadisticas de Q5", self._origin)
        if not self._stopping:
            self._input_queue.start_consuming(self._on_message)

    def stop(self):
        self._stopping = True
        self._input_queue.stop_consuming()

    def close(self):
        for queue in self._comparator_queues:
            try:
                queue.close()
            except Exception:
                logging.exception("No se pudo cerrar una salida de %s", self._origin)

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
        if msg_type == internal.MsgType.PARTIAL_AVERAGE:
            self._process_partial_average(client_id, value["partial_average"])
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

    def _process_partial_average(self, client_id, partial_average):
        previous = self._totals.get(client_id, internal.PartialAverage(0, 0))
        self._totals[client_id] = internal.PartialAverage(
            previous.total_words + partial_average.total_words, previous.article_count + partial_average.article_count,
        )
        self._upstream.add_message(client_id)

    def _try_finish_client(self, client_id, gateway_id):
        if self._upstream.is_complete(client_id):
            self._finish_client(client_id, gateway_id)

    def _finish_client(self, client_id, gateway_id):
        stats = self._totals.get(client_id, internal.PartialAverage(0, 0))
        message = internal.encode_average(client_id, gateway_id, internal.FinalAverage(*stats))
        for queue in self._comparator_queues:
            queue.send(message)
        self._totals.pop(client_id, None)
        self._upstream.forget(client_id)
        self._finished[client_id] = None
        if len(self._finished) > RECENT_CLIENTS:
            self._finished.popitem(last=False)
        logging.info("Cliente %s: promedio Q5 final %s/%s", client_id, stats.total_words, stats.article_count)
