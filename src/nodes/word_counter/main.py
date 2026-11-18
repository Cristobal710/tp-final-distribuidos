import os
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.storage import ClientFiles
from q3q4.bootstrap import publisher_of, run_stage, start_node
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


# Levanta el contador de palabras de un shard y lo apaga limpio ante SIGTERM.
def main():
    start_node()
    input_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{WORDS_QUEUE_PREFIX}.{WORD_COUNTER_ID}")
    buckets = [
        ClientFiles(DATA_DIR, f"word_counter_{WORD_COUNTER_ID}_{bucket}") for bucket in range(WORD_COUNTER_BUCKETS)
    ]
    counter = WordCounter(
        WORD_COUNTER_ID, input_queue, buckets,
        [publisher_of(RABBITMQ_HOST, f"{JOIN_Q3_QUEUE_PREFIX}.{i}", IDLE_SECONDS) for i in range(JOIN_Q3_AMOUNT)],
        [publisher_of(RABBITMQ_HOST, f"{TOP20_QUEUE_PREFIX}.{i}", IDLE_SECONDS) for i in range(TOP20_AMOUNT)],
        TEXT_PROCESSOR_AMOUNT, TOP_SIZE, MAX_BATCH_ROWS, MAX_BATCH_BYTES,
    )
    return run_stage(counter, input_queue)


if __name__ == "__main__":
    sys.exit(main())
