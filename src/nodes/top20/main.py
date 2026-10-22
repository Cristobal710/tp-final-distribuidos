import logging
import os
import signal
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.publisher import ReconnectingPublisher
from top20 import Top20

TOP20_ID = int(os.environ["TOP20_ID"])
WORD_COUNTER_AMOUNT = int(os.environ["WORD_COUNTER_AMOUNT"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
TOP20_QUEUE_PREFIX = os.environ["TOP20_QUEUE_PREFIX"]
RESULTS_QUEUE_PREFIX = os.environ["RESULTS_QUEUE_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
IDLE_SECONDS = float(os.environ["PUBLISHER_IDLE_SECONDS"])


# Arma el publicador de la cola de resultados de un gateway.
def open_results_queue(gateway_id):
    return ReconnectingPublisher(
        lambda: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{RESULTS_QUEUE_PREFIX}.{gateway_id}"), IDLE_SECONDS
    )


# Levanta el Top 20 final y lo apaga limpio ante SIGTERM.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    input_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{TOP20_QUEUE_PREFIX}.{TOP20_ID}")
    top = Top20(TOP20_ID, input_queue, open_results_queue, WORD_COUNTER_AMOUNT, TOP_SIZE)
    signal.signal(signal.SIGTERM, lambda signum, frame: top.stop())
    try:
        top.run()
    finally:
        top.close()
        try:
            input_queue.close()
        except Exception as e:
            logging.error(f"Error cerrando una conexión: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
