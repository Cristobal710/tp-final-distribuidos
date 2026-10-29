import logging
import os
import signal
import sys

from common.middleware import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import PREFETCH_COUNT
from common.publisher import ReconnectingPublisher
from filter_biography import FilterBiography


def close_all(connections):
    for connection in reversed(connections):
        try:
            connection.close()
        except Exception:
            logging.exception("No se pudo cerrar una conexion")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    filter_id = int(os.environ["FILTER_BIOGRAPHY_ID"])
    filter_amount = int(os.environ["FILTER_BIOGRAPHY_AMOUNT"])
    classifier_amount = int(os.environ["CLASSIFIER_AMOUNT"])
    max_rows = int(os.environ["MAX_BATCH_ROWS"])
    max_bytes = int(os.environ["MAX_BATCH_BYTES"])
    idle_seconds = float(os.environ["PUBLISHER_IDLE_SECONDS"])
    if not 0 <= filter_id < filter_amount or min(classifier_amount, max_rows, max_bytes) < 1:
        raise ValueError("Configuracion invalida del filtro de biografias")
    host = os.environ["RABBITMQ_HOST"]
    exchange = os.environ["CONTROL_EXCHANGE"]
    connections = []
    try:
        bio_queue = MessageMiddlewareQueueRabbitMQ(host, os.environ["BIO_QUEUE"])
        connections.append(bio_queue)
        control_input = MessageMiddlewareExchangeRabbitMQ(host, exchange, [f"{exchange}.{filter_id}"])
        connections.append(control_input)
        control_output = ReconnectingPublisher(
            lambda: MessageMiddlewareExchangeRabbitMQ(host, exchange, [f"{exchange}.{i}" for i in range(filter_amount)]),
            idle_seconds,
        )
        connections.append(control_output)
        image_queue = ReconnectingPublisher(
            lambda: MessageMiddlewareQueueRabbitMQ(host, os.environ["IMAGES_QUEUE"]), idle_seconds,
        )
        connections.append(image_queue)
        worker = FilterBiography(
            filter_id, bio_queue, control_input, control_output, image_queue, classifier_amount,
            max_rows, max_bytes, PREFETCH_COUNT,
        )
        signal.signal(signal.SIGTERM, lambda signum, frame: worker.stop())
        worker.run()
    finally:
        close_all(connections)
    return 0


if __name__ == "__main__":
    sys.exit(main())
