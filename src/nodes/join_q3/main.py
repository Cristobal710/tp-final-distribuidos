import logging
import os
import signal
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.publisher import ReconnectingPublisher
from common.storage import ClientFiles
from join_q3 import JoinQ3

JOIN_Q3_ID = int(os.environ["JOIN_Q3_ID"])
WORD_COUNTER_AMOUNT = int(os.environ["WORD_COUNTER_AMOUNT"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
JOIN_Q3_QUEUE_PREFIX = os.environ["JOIN_Q3_QUEUE_PREFIX"]
RESULTS_QUEUE_PREFIX = os.environ["RESULTS_QUEUE_PREFIX"]
DATA_DIR = os.environ["DATA_DIR"]
MAX_BATCH_ROWS = int(os.environ["MAX_BATCH_ROWS"])
MAX_BATCH_BYTES = int(os.environ["MAX_BATCH_BYTES"])
IDLE_SECONDS = float(os.environ["PUBLISHER_IDLE_SECONDS"])


# Arma el publicador de la cola de resultados de un gateway.
def open_results_queue(gateway_id):
    return ReconnectingPublisher(
        lambda: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{RESULTS_QUEUE_PREFIX}.{gateway_id}"), IDLE_SECONDS
    )


# Levanta el Join Q3 y lo apaga limpio ante SIGTERM.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    input_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{JOIN_Q3_QUEUE_PREFIX}.{JOIN_Q3_ID}")
    join = JoinQ3(
        JOIN_Q3_ID, input_queue, ClientFiles(DATA_DIR, f"join_q3_{JOIN_Q3_ID}"), open_results_queue,
        WORD_COUNTER_AMOUNT, MAX_BATCH_ROWS, MAX_BATCH_BYTES,
    )
    signal.signal(signal.SIGTERM, lambda signum, frame: join.stop())
    try:
        join.run()
    finally:
        join.close()
        try:
            input_queue.close()
        except Exception as e:
            logging.error(f"Error cerrando una conexión: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
