import logging
import os
import signal
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.publisher import ReconnectingPublisher
from server import Server
from results import Results

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
GATEWAY_ID = int(os.environ["GATEWAY_ID"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
CLASSIFICATION_QUEUE = os.environ["CLASSIFICATION_QUEUE"]
RESULTS_QUEUE_PREFIX = os.environ["RESULTS_QUEUE_PREFIX"]


# Abre una conexion propia por cliente a la cola de clasificacion.
def open_classification_queue():
    return ReconnectingPublisher(
        lambda: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, CLASSIFICATION_QUEUE),
        float(os.environ.get("PUBLISHER_IDLE_SECONDS", "30")),
    )


# Levanta el gateway y lo apaga limpio ante SIGTERM.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    results = Results(
        lambda: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{RESULTS_QUEUE_PREFIX}.{GATEWAY_ID}"),
        {1: int(os.environ["CLASSIFIER_AMOUNT"]), 2: int(os.environ["IMAGE_DOWNLOADER_AMOUNT"]),
         3: 1, 4: 1, 5: int(os.environ["COMPARATOR_AMOUNT"])},
    )
    server = Server(SERVER_HOST, SERVER_PORT, GATEWAY_ID, open_classification_queue, results)
    signal.signal(signal.SIGTERM, lambda signum, frame: server.stop())
    server.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
