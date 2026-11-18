import os
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.storage import ClientFiles
from join_q3 import JoinQ3
from q3q4.bootstrap import publisher_of, run_stage, start_node

JOIN_Q3_ID = int(os.environ["JOIN_Q3_ID"])
WORD_COUNTER_AMOUNT = int(os.environ["WORD_COUNTER_AMOUNT"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
JOIN_Q3_QUEUE_PREFIX = os.environ["JOIN_Q3_QUEUE_PREFIX"]
RESULTS_QUEUE_PREFIX = os.environ["RESULTS_QUEUE_PREFIX"]
DATA_DIR = os.environ["DATA_DIR"]
MAX_BATCH_ROWS = int(os.environ["MAX_BATCH_ROWS"])
MAX_BATCH_BYTES = int(os.environ["MAX_BATCH_BYTES"])
IDLE_SECONDS = float(os.environ["PUBLISHER_IDLE_SECONDS"])


# Levanta el Join Q3 y lo apaga limpio ante SIGTERM.
def main():
    start_node()
    input_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{JOIN_Q3_QUEUE_PREFIX}.{JOIN_Q3_ID}")
    join = JoinQ3(
        JOIN_Q3_ID, input_queue, ClientFiles(DATA_DIR, f"join_q3_{JOIN_Q3_ID}"),
        lambda gateway_id: publisher_of(RABBITMQ_HOST, f"{RESULTS_QUEUE_PREFIX}.{gateway_id}", IDLE_SECONDS),
        WORD_COUNTER_AMOUNT, MAX_BATCH_ROWS, MAX_BATCH_BYTES,
    )
    return run_stage(join, input_queue)


if __name__ == "__main__":
    sys.exit(main())
