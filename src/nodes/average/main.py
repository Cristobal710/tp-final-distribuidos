import logging
import os
import signal
import sys

from average import Average
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.publisher import ReconnectingPublisher


def close_all(connections):
    for connection in reversed(connections):
        try:
            connection.close()
        except Exception:
            logging.exception("No se pudo cerrar una conexion")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    average_id = int(os.environ["AVERAGE_ID"])
    average_amount = int(os.environ["AVERAGE_AMOUNT"])
    comparator_amount = int(os.environ["COMPARATOR_AMOUNT"])
    processor_amount = int(os.environ["TEXT_PROCESSOR_AMOUNT"])
    if not 0 <= average_id < average_amount or min(comparator_amount, processor_amount) < 1:
        raise ValueError("Configuracion invalida de Average")
    host = os.environ["RABBITMQ_HOST"]
    average_prefix = os.environ["AVERAGE_QUEUE_PREFIX"]
    final_prefix = os.environ["FINAL_AVERAGE_QUEUE_PREFIX"]
    idle_seconds = float(os.environ["PUBLISHER_IDLE_SECONDS"])
    connections = []
    try:
        input_queue = MessageMiddlewareQueueRabbitMQ(host, f"{average_prefix}.{average_id}")
        connections.append(input_queue)
        outputs = [
            ReconnectingPublisher(
                lambda target=target: MessageMiddlewareQueueRabbitMQ(host, f"{final_prefix}.{target}"), idle_seconds,
            )
            for target in range(comparator_amount)
        ]
        worker = Average(average_id, input_queue, outputs, processor_amount)
        connections.append(worker)
        signal.signal(signal.SIGTERM, lambda signum, frame: worker.stop())
        worker.run()
    finally:
        close_all(connections)
    return 0


if __name__ == "__main__":
    sys.exit(main())
