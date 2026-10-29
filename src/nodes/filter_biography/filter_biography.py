import logging
import threading

from common.batching.row_chunks import chunk_rows
from common.eof import UpstreamTracker
from common.protocol import articles, external, internal

STOP_POLL_SECONDS = 1

def is_biography(sections):
    if not isinstance(sections, str):
        return False
    sections = sections.casefold()
    return "biography" in sections or "biographie" in sections


class FilterBiography:

    def __init__(self, filter_id, bio_queue, control_input, control_output, image_queue,
                 classifier_amount, max_rows, max_bytes, in_flight_messages=1):
        if classifier_amount < 1 or max_rows < 1 or max_bytes < 1 or in_flight_messages < 1:
            raise ValueError("Las cantidades y los limites tienen que ser positivos")
        self._origin = f"filter_biography_{filter_id}"
        self._bio_queue = bio_queue
        self._control_input = control_input
        self._control_output = control_output
        self._image_queue = image_queue
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._in_flight_messages = in_flight_messages
        self._classifiers = UpstreamTracker(classifier_amount, check_total=True, check_received=False)
        self._processed = {}
        self._sent = {}
        self._finished = {}
        self._processed_messages = 0
        self._stopping = False
        self._control_thread = threading.Thread(target=self._consume_control)

    def run(self):
        logging.info("%s consumiendo biografias", self._origin)
        self._control_thread.start()
        try:
            if not self._stopping:
                self._bio_queue.start_consuming(self._on_message)
        finally:
            self._stopping = True
            while self._control_thread.is_alive():
                self._control_input.stop_consuming()
                self._control_thread.join(timeout=STOP_POLL_SECONDS)

    def stop(self):
        self._stopping = True
        self._bio_queue.stop_consuming()

    def close(self):
        self._image_queue.close()

    def _consume_control(self):
        try:
            if not self._stopping:
                self._control_input.start_consuming(self._on_control_message)
        except Exception:
            logging.exception("Se corto el control de %s", self._origin)
            self.stop()

    def _on_control_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error("Mensaje de control invalido descartado: %s", e)
            ack()
            return
        if msg_type in (internal.MsgType.EOF, internal.MsgType.LATE_EOF):
            self._bio_queue.run_in_consumer_thread(lambda: self._on_upstream(msg_type, value))
        ack()

    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
            if "gateway_id" not in value:
                raise external.ProtocolError("El mensaje necesita gateway_id")
        except external.ProtocolError as e:
            logging.error("Mensaje invalido descartado: %s", e)
            ack()
            return
        if msg_type == internal.MsgType.BIOS:
            self._process_bios(value["client_id"], value["gateway_id"], value["bios"])
        elif msg_type in (internal.MsgType.EOF, internal.MsgType.LATE_EOF):
            self._control_output.send(message)
        self._processed_messages += 1
        self._forget_finished_clients()
        ack()

    def _process_bios(self, client_id, gateway_id, bios):
        rows = (
            articles.encode_row(internal.ImageRequest(bio.name, bio.url, bio.image_url))
            for bio in bios if is_biography(bio.sections)
        )
        sent = 0
        for chunk in chunk_rows(rows, self._max_rows, self._max_bytes):
            sent += self._publish_images(client_id, gateway_id, chunk)
        if client_id in self._finished:
            self._image_queue.send(internal.encode_late_eof(client_id, gateway_id, self._origin, 1, sent))
        else:
            self._processed[client_id] = self._processed.get(client_id, 0) + 1
            self._sent[client_id] = self._sent.get(client_id, 0) + sent
        logging.info("Cliente %s: %s bios procesadas, %s mensajes de imagenes", client_id, len(bios), sent)

    def _publish_images(self, client_id, gateway_id, rows):
        message = internal.encode_serialized_rows(internal.MsgType.IMAGES, client_id, gateway_id, rows)
        if len(message) <= self._max_bytes:
            self._image_queue.send(message)
            return 1
        if len(rows) == 1:
            logging.warning("Cliente %s: pedido de imagen demasiado grande descartado", client_id)
            return 0
        middle = len(rows) // 2
        return (self._publish_images(client_id, gateway_id, rows[:middle])
                + self._publish_images(client_id, gateway_id, rows[middle:]))

    def _on_upstream(self, msg_type, value):
        client_id = value["client_id"]
        if client_id in self._finished:
            return
        if msg_type == internal.MsgType.EOF:
            self._classifiers.add_eof(value)
        else:
            self._classifiers.add_late_eof(value)
        if self._classifiers.is_complete(client_id):
            self._finish_client(client_id, value["gateway_id"])

    def _finish_client(self, client_id, gateway_id):
        self._image_queue.send(internal.encode_eof(
            client_id, gateway_id, self._origin, self._processed.get(client_id, 0),
            self._classifiers.total_sent(client_id), self._sent.get(client_id, 0),
        ))
        self._processed.pop(client_id, None)
        self._sent.pop(client_id, None)
        self._classifiers.forget(client_id)
        self._finished[client_id] = self._processed_messages

    def _forget_finished_clients(self):
        for client_id, finished_at in list(self._finished.items()):
            if self._processed_messages - finished_at >= self._in_flight_messages:
                del self._finished[client_id]
