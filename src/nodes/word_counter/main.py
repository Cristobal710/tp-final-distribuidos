import logging
import os
import signal
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.publisher import ReconnectingPublisher
from common.storage import ClientFiles
from word_counter import WordCounter

WORD_COUNTER_ID = int(os.environ["WORD_COUNTER_ID"])
TEXT_PROCESSOR_AMOUNT = int(os.environ["TEXT_PROCESSOR_AMOUNT"])
JOIN_Q3_AMOUNT = int(os.environ["JOIN_Q3_AMOUNT"])
TOP20_AMOUNT = int(os.environ["TOP20_AMOUNT"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
WORDS_QUEUE_PREFIX = os.environ["WORDS_QUEUE_PREFIX"]
JOIN_Q3_QUEUE_PREFIX = os.environ["JOIN_Q3_QUEUE_PREFIX"]
TOP20_QUEUE_PREFIX = os.environ["TOP20_QUEUE_PREFIX"]
DATA_DIR = os.environ["DATA_DIR"]
WORD_COUNTER_BUCKETS = int(os.environ["WORD_COUNTER_BUCKETS"])
TOP_SIZE = int(os.environ["TOP_SIZE"])
MAX_BATCH_ROWS = int(os.environ["MAX_BATCH_ROWS"])
MAX_BATCH_BYTES = int(os.environ["MAX_BATCH_BYTES"])
IDLE_SECONDS = float(os.environ["PUBLISHER_IDLE_SECONDS"])


# Arma un publicador que abre la cola de salida recien cuando la usa.
def publisher_of(queue_name):
    return ReconnectingPublisher(lambda: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, queue_name), IDLE_SECONDS)


# Levanta el contador de palabras de un shard y lo apaga limpio ante SIGTERM.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    input_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{WORDS_QUEUE_PREFIX}.{WORD_COUNTER_ID}")
    buckets = [
        ClientFiles(DATA_DIR, f"word_counter_{WORD_COUNTER_ID}_{bucket}") for bucket in range(WORD_COUNTER_BUCKETS)
    ]
    counter = WordCounter(
        WORD_COUNTER_ID, input_queue, buckets,
        [publisher_of(f"{JOIN_Q3_QUEUE_PREFIX}.{i}") for i in range(JOIN_Q3_AMOUNT)],
        [publisher_of(f"{TOP20_QUEUE_PREFIX}.{i}") for i in range(TOP20_AMOUNT)],
        TEXT_PROCESSOR_AMOUNT, TOP_SIZE, MAX_BATCH_ROWS, MAX_BATCH_BYTES,
    )
    signal.signal(signal.SIGTERM, lambda signum, frame: counter.stop())
    try:
        counter.run()
    finally:
        counter.close()
        try:
            input_queue.close()
        except Exception as e:
            logging.error(f"Error cerrando una conexión: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
