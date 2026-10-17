import logging
import os
import signal
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from server import Server

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
GATEWAY_ID = int(os.environ["GATEWAY_ID"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
CLASSIFICATION_QUEUE = os.environ["CLASSIFICATION_QUEUE"]


# Abre una conexion propia a la cola de clasificacion, una por cada cliente.
def open_classification_queue():
    return MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, CLASSIFICATION_QUEUE)


# Levanta el gateway y lo apaga limpio ante SIGTERM.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    server = Server(SERVER_HOST, SERVER_PORT, GATEWAY_ID, open_classification_queue)
    signal.signal(signal.SIGTERM, lambda signum, frame: server.stop())
    server.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
