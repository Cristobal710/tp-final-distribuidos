import os
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q3q4.bootstrap import publisher_of, run_stage, start_node
from top20 import Top20

TOP20_ID = int(os.environ["TOP20_ID"])
WORD_COUNTER_AMOUNT = int(os.environ["WORD_COUNTER_AMOUNT"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
TOP20_QUEUE_PREFIX = os.environ["TOP20_QUEUE_PREFIX"]
RESULTS_QUEUE_PREFIX = os.environ["RESULTS_QUEUE_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
IDLE_SECONDS = float(os.environ["PUBLISHER_IDLE_SECONDS"])


# Levanta el Top 20 final y lo apaga limpio ante SIGTERM.
def main():
    start_node()
    input_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{TOP20_QUEUE_PREFIX}.{TOP20_ID}")
    top = Top20(
        TOP20_ID, input_queue,
        lambda gateway_id: publisher_of(RABBITMQ_HOST, f"{RESULTS_QUEUE_PREFIX}.{gateway_id}", IDLE_SECONDS),
        WORD_COUNTER_AMOUNT, TOP_SIZE,
    )
    return run_stage(top, input_queue)


if __name__ == "__main__":
    sys.exit(main())
