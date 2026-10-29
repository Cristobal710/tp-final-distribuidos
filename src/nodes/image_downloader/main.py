import logging
import math
import os
import signal
import sys
from functools import partial

from common.middleware import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import PREFETCH_COUNT
from common.protocol.external import MAX_VALUE_SIZE
from common.publisher import ReconnectingPublisher
from download import download_image
from image_downloader import ImageDownloader


def close_all(connections):
    for connection in reversed(connections):
        try:
            connection.close()
        except Exception:
            logging.exception("No se pudo cerrar una conexion")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    downloader_id = int(os.environ["IMAGE_DOWNLOADER_ID"])
    downloader_amount = int(os.environ["IMAGE_DOWNLOADER_AMOUNT"])
    filter_amount = int(os.environ["FILTER_BIOGRAPHY_AMOUNT"])
    timeout = float(os.environ["DOWNLOAD_TIMEOUT_SECONDS"])
    attempts = int(os.environ["DOWNLOAD_ATTEMPTS"])
    retry_delay = float(os.environ["DOWNLOAD_RETRY_SECONDS"])
    max_image_bytes = int(os.environ["MAX_IMAGE_BYTES"])
    max_result_bytes = int(os.environ["MAX_RESULT_BYTES"])
    idle_seconds = float(os.environ["PUBLISHER_IDLE_SECONDS"])
    if (not 0 <= downloader_id < downloader_amount or min(filter_amount, attempts, max_image_bytes) < 1
            or not 0 < max_result_bytes <= MAX_VALUE_SIZE or not math.isfinite(timeout) or timeout <= 0
            or not math.isfinite(retry_delay) or retry_delay < 0):
        raise ValueError("Configuracion invalida del descargador de imagenes")
    host = os.environ["RABBITMQ_HOST"]
    exchange = os.environ["CONTROL_EXCHANGE"]
    results_prefix = os.environ["RESULTS_QUEUE_PREFIX"]

    def open_results_queue(gateway_id):
        return ReconnectingPublisher(
            lambda: MessageMiddlewareQueueRabbitMQ(host, f"{results_prefix}.{gateway_id}"), idle_seconds,
        )

    connections = []
    try:
        input_queue = MessageMiddlewareQueueRabbitMQ(host, os.environ["IMAGES_QUEUE"])
        connections.append(input_queue)
        control_input = MessageMiddlewareExchangeRabbitMQ(host, exchange, [f"{exchange}.{downloader_id}"])
        connections.append(control_input)
        control_output = ReconnectingPublisher(
            lambda: MessageMiddlewareExchangeRabbitMQ(
                host, exchange, [f"{exchange}.{i}" for i in range(downloader_amount)],
            ), idle_seconds,
        )
        connections.append(control_output)
        worker = ImageDownloader(
            downloader_id, input_queue, control_input, control_output, open_results_queue,
            partial(download_image, timeout=timeout, attempts=attempts, max_bytes=max_image_bytes,
                    retry_delay=retry_delay),
            filter_amount, max_result_bytes, PREFETCH_COUNT,
        )
        connections.append(worker)
        signal.signal(signal.SIGTERM, lambda signum, frame: worker.stop())
        worker.run()
    finally:
        close_all(connections)
    return 0


if __name__ == "__main__":
    sys.exit(main())
