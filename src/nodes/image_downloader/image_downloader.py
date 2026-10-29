import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from common.eof import UpstreamTracker
from common.protocol import external, internal
from download import DownloadCancelled, DownloadError

Q2 = 2
STOP_POLL_SECONDS = 1

@dataclass
class _ImageBatch:
    client_id: str
    gateway_id: int
    images: object
    ack: object
    current: object = None
    sent: int = 0


class ImageDownloader:

    def __init__(self, downloader_id, input_queue, control_input, control_output, open_results_queue,
                 download_image, filter_amount, max_result_bytes, in_flight_messages=1):
        if filter_amount < 1 or max_result_bytes < 1 or in_flight_messages < 1:
            raise ValueError("Las cantidades y los limites tienen que ser positivos")
        self._origin = f"image_downloader_{downloader_id}"
        self._input_queue = input_queue
        self._control_input = control_input
        self._control_output = control_output
        self._open_results_queue = open_results_queue
        self._download_image = download_image
        self._max_result_bytes = max_result_bytes
        self._in_flight_messages = in_flight_messages
        self._filters = UpstreamTracker(filter_amount, check_total=True, check_received=False)
        self._results_queues = {}
        self._processed = {}
        self._sent = {}
        self._finished = {}
        self._processed_messages = 0
        self._active = None
        self._stopping = False
        self._cancelled = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=self._origin)
        self._control_thread = threading.Thread(target=self._consume_control)

    def run(self):
        logging.info("%s consumiendo pedidos de imagenes", self._origin)
        self._control_thread.start()
        try:
            if not self._stopping:
                self._input_queue.start_consuming(self._on_message)
        finally:
            self._stopping = True
            self._cancelled.set()
            while self._control_thread.is_alive():
                self._control_input.stop_consuming()
                self._control_thread.join(timeout=STOP_POLL_SECONDS)

    def stop(self):
        self._stopping = True
        self._cancelled.set()
        self._input_queue.stop_consuming()

    def close(self):
        self._stopping = True
        self._cancelled.set()
        self._executor.shutdown(wait=True, cancel_futures=True)
        for publisher in self._results_queues.values():
            publisher.close()
        self._results_queues.clear()

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
            self._input_queue.run_in_consumer_thread(lambda: self._on_upstream(msg_type, value))
        ack()

    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error("Mensaje invalido descartado: %s", e)
            ack()
            return
        if msg_type == internal.MsgType.IMAGES:
            self._process_images(value["client_id"], value["gateway_id"], value["images"], ack)
            return
        if msg_type in (internal.MsgType.EOF, internal.MsgType.LATE_EOF):
            self._control_output.send(message)
        self._processed_messages += 1
        self._forget_finished_clients()
        ack()

    def _process_images(self, client_id, gateway_id, images, ack):
        if self._active is not None:
            raise RuntimeError("El descargador requiere prefetch_count=1")
        self._active = _ImageBatch(client_id, gateway_id, iter(images), ack)
        self._download_next()

    def _download_next(self):
        if self._stopping:
            return
        try:
            self._active.current = next(self._active.images)
        except StopIteration:
            self._complete_batch()
            return
        future = self._executor.submit(
            self._download_image, self._active.current.image_url, stop_event=self._cancelled,
        )
        future.add_done_callback(self._schedule_download_complete)

    def _schedule_download_complete(self, future):
        if self._stopping:
            return
        try:
            self._input_queue.run_in_consumer_thread(lambda: self._on_download_complete(future))
        except Exception:
            logging.exception("No se pudo entregar la descarga al consumidor")
            self.stop()

    def _on_download_complete(self, future):
        if self._stopping:
            return
        batch = self._active
        try:
            image_bytes = future.result()
        except DownloadCancelled:
            self.stop()
            return
        except DownloadError as e:
            logging.warning("Cliente %s: imagen %s descartada: %s", batch.client_id, batch.current.image_url, e)
        else:
            # Un fallo de publicacion debe salir del consumidor sin hacer ack del batch.
            self._publish_result(batch.client_id, batch.gateway_id, batch.current, image_bytes)
        self._download_next()

    def _publish_result(self, client_id, gateway_id, image, image_bytes):
        result = internal.Q2Result(image.name, image.url, list(image_bytes))
        message = internal.encode_results(client_id, gateway_id, Q2, [result])
        if len(message) > self._max_result_bytes:
            logging.warning("Cliente %s: resultado de imagen demasiado grande descartado", client_id)
            return
        self._results_queue(gateway_id).send(message)
        self._active.sent += 1

    def _complete_batch(self):
        batch = self._active
        self._processed_messages += 1
        if batch.client_id in self._finished:
            self._results_queue(batch.gateway_id).send(internal.encode_late_eof(
                batch.client_id, batch.gateway_id, self._origin, 1, batch.sent, Q2,
            ))
        else:
            self._processed[batch.client_id] = self._processed.get(batch.client_id, 0) + 1
            self._sent[batch.client_id] = self._sent.get(batch.client_id, 0) + batch.sent
        self._active = None
        if self._filters.is_complete(batch.client_id):
            self._finish_client(batch.client_id, batch.gateway_id)
        self._forget_finished_clients()
        batch.ack()

    def _on_upstream(self, msg_type, value):
        client_id = value["client_id"]
        if client_id in self._finished:
            return
        if msg_type == internal.MsgType.EOF:
            self._filters.add_eof(value)
        else:
            self._filters.add_late_eof(value)
        if self._filters.is_complete(client_id) and (self._active is None or self._active.client_id != client_id):
            self._finish_client(client_id, value["gateway_id"])

    def _finish_client(self, client_id, gateway_id):
        self._results_queue(gateway_id).send(internal.encode_eof(
            client_id, gateway_id, self._origin, self._processed.get(client_id, 0),
            self._filters.total_sent(client_id), self._sent.get(client_id, 0), Q2,
        ))
        self._processed.pop(client_id, None)
        self._sent.pop(client_id, None)
        self._filters.forget(client_id)
        self._finished[client_id] = self._processed_messages

    def _forget_finished_clients(self):
        for client_id, finished_at in list(self._finished.items()):
            if self._processed_messages - finished_at >= self._in_flight_messages:
                del self._finished[client_id]

    def _results_queue(self, gateway_id):
        if gateway_id not in self._results_queues:
            self._results_queues[gateway_id] = self._open_results_queue(gateway_id)
        return self._results_queues[gateway_id]
