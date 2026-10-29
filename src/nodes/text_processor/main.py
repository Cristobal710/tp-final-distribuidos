import logging
import os
import signal
import sys

from nltk.corpus import stopwords

from common.middleware import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import PREFETCH_COUNT
from common.publisher import ReconnectingPublisher
from text_processor import TextProcessor
from words_output import WordsOutput
from q5_output import Q5Output

TEXT_PROCESSOR_ID = int(os.environ["TEXT_PROCESSOR_ID"])
TEXT_PROCESSOR_AMOUNT = int(os.environ["TEXT_PROCESSOR_AMOUNT"])
CLASSIFIER_AMOUNT = int(os.environ["CLASSIFIER_AMOUNT"])
WORD_COUNTER_AMOUNT = int(os.environ["WORD_COUNTER_AMOUNT"])
AVERAGE_AMOUNT = int(os.environ["AVERAGE_AMOUNT"])
COMPARATOR_AMOUNT = int(os.environ["COMPARATOR_AMOUNT"])
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
TEXT_QUEUE = os.environ["TEXT_QUEUE"]
CONTROL_EXCHANGE = os.environ["CONTROL_EXCHANGE"]
WORDS_QUEUE_PREFIX = os.environ["WORDS_QUEUE_PREFIX"]
AVERAGE_QUEUE_PREFIX = os.environ["AVERAGE_QUEUE_PREFIX"]
COMPARISON_QUEUE_PREFIX = os.environ["COMPARISON_QUEUE_PREFIX"]
MAX_BATCH_ROWS = int(os.environ["MAX_BATCH_ROWS"])
MAX_BATCH_BYTES = int(os.environ["MAX_BATCH_BYTES"])
IDLE_SECONDS = float(os.environ["PUBLISHER_IDLE_SECONDS"])


# Devuelve las stopwords en ingles y en frances, que se sacan de todos los textos.
def load_stop_words():
    return set(stopwords.words("english")) | set(stopwords.words("french"))


# Cierra cada conexion aunque falle alguna.
def close_all(connections):
    for connection in connections:
        try:
            connection.close()
        except Exception as e:
            logging.error(f"Error cerrando una conexión: {e}")


# Levanta el Text processor con sus colas, el control y las salidas de Q3, Q4 y Q5.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    text_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, TEXT_QUEUE)
    control_input = MessageMiddlewareExchangeRabbitMQ(
        RABBITMQ_HOST, CONTROL_EXCHANGE, [f"{CONTROL_EXCHANGE}.{TEXT_PROCESSOR_ID}"]
    )
    control_output = ReconnectingPublisher(
        lambda: MessageMiddlewareExchangeRabbitMQ(
            RABBITMQ_HOST, CONTROL_EXCHANGE, [f"{CONTROL_EXCHANGE}.{i}" for i in range(TEXT_PROCESSOR_AMOUNT)]
        ),
        IDLE_SECONDS,
    )
    shard_queues = [
        ReconnectingPublisher(
            lambda shard=shard: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{WORDS_QUEUE_PREFIX}.{shard}"),
            IDLE_SECONDS,
        )
        for shard in range(WORD_COUNTER_AMOUNT)
    ]
    words_output = WordsOutput(WORDS_QUEUE_PREFIX, shard_queues, MAX_BATCH_ROWS, MAX_BATCH_BYTES)
    average_queues = [
        ReconnectingPublisher(
            lambda shard=shard: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{AVERAGE_QUEUE_PREFIX}.{shard}"),
            IDLE_SECONDS,
        )
        for shard in range(AVERAGE_AMOUNT)
    ]
    comparison_queues = [
        ReconnectingPublisher(
            lambda target=target: MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{COMPARISON_QUEUE_PREFIX}.{target}"),
            IDLE_SECONDS,
        )
        for target in range(COMPARATOR_AMOUNT)
    ]
    q5_output = Q5Output(
        AVERAGE_QUEUE_PREFIX, average_queues, COMPARISON_QUEUE_PREFIX, comparison_queues,
        MAX_BATCH_ROWS, MAX_BATCH_BYTES,
    )
    processor = TextProcessor(
        TEXT_PROCESSOR_ID, text_queue, control_input, control_output, [words_output, q5_output],
        CLASSIFIER_AMOUNT, load_stop_words(), PREFETCH_COUNT,
    )
    signal.signal(signal.SIGTERM, lambda signum, frame: processor.stop())
    try:
        processor.run()
    finally:
        processor.close()
        close_all([control_output, control_input, text_queue])
    return 0


if __name__ == "__main__":
    sys.exit(main())
