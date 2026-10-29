import logging
import os
import signal
import sys

from classifier import Classifier
from common.middleware import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import PREFETCH_COUNT
from common.publisher import ReconnectingPublisher

CLASSIFIER_ID = int(os.environ["CLASSIFIER_ID"])
CLASSIFIER_AMOUNT = int(os.environ["CLASSIFIER_AMOUNT"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
CLASSIFICATION_QUEUE = os.environ["CLASSIFICATION_QUEUE"]
CONTROL_EXCHANGE = os.environ["CONTROL_EXCHANGE"]
TEXT_QUEUE = os.environ["TEXT_QUEUE"]
BIO_QUEUE = os.environ["BIO_QUEUE"]
RESULTS_QUEUE_PREFIX = os.environ["RESULTS_QUEUE_PREFIX"]
IDLE_SECONDS = float(os.environ["PUBLISHER_IDLE_SECONDS"])


# Abre la cola de resultados de un gateway.
def open_results_queue(gateway_id):
    return publisher_of(f"{RESULTS_QUEUE_PREFIX}.{gateway_id}")


def publisher_of(queue_name):
    return ReconnectingPublisher(lambda: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, queue_name), IDLE_SECONDS)


# Cierra cada conexion aunque falle alguna.
def close_all(connections):
    for connection in connections:
        try:
            connection.close()
        except Exception as e:
            logging.error(f"Error cerrando una conexión: {e}")


# Levanta el clasificador con sus colas y el exchange de control y lo apaga limpio ante SIGTERM.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    classification_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, CLASSIFICATION_QUEUE)
    control_input = MessageMiddlewareExchangeRabbitMQ(
        RABBITMQ_HOST, CONTROL_EXCHANGE, [f"{CONTROL_EXCHANGE}.{CLASSIFIER_ID}"]
    )
    control_output = ReconnectingPublisher(
        lambda: MessageMiddlewareExchangeRabbitMQ(
            RABBITMQ_HOST, CONTROL_EXCHANGE, [f"{CONTROL_EXCHANGE}.{i}" for i in range(CLASSIFIER_AMOUNT)]
        ), IDLE_SECONDS,
    )
    text_queue = publisher_of(TEXT_QUEUE)
    bio_queue = publisher_of(BIO_QUEUE)
    classifier = Classifier(
        CLASSIFIER_ID, classification_queue, control_input, control_output,
        text_queue, bio_queue, open_results_queue, PREFETCH_COUNT,
    )
    signal.signal(signal.SIGTERM, lambda signum, frame: classifier.stop())
    try:
        classifier.run()
    finally:
        close_all([classifier, bio_queue, text_queue, control_output, control_input, classification_queue])
    return 0


if __name__ == "__main__":
    sys.exit(main())
