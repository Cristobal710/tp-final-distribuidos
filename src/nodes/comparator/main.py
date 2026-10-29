import logging
import os
import signal
import sys

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.protocol.external import MAX_VALUE_SIZE
from common.publisher import ReconnectingPublisher
from common.storage import ClientFiles
from comparator import Comparator


def close_all(connections):
    for connection in reversed(connections):
        try:
            connection.close()
        except Exception:
            logging.exception("No se pudo cerrar una conexion")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    comparator_id = int(os.environ["COMPARATOR_ID"])
    comparator_amount = int(os.environ["COMPARATOR_AMOUNT"])
    processor_amount = int(os.environ["TEXT_PROCESSOR_AMOUNT"])
    max_rows = int(os.environ["MAX_BATCH_ROWS"])
    max_bytes = int(os.environ["MAX_BATCH_BYTES"])
    drain_rows = int(os.environ["DRAIN_BATCH_ROWS"])
    if (not 0 <= comparator_id < comparator_amount or min(processor_amount, max_rows, drain_rows) < 1
            or not 0 < max_bytes <= MAX_VALUE_SIZE):
        raise ValueError("Configuracion invalida de Comparator")
    host = os.environ["RABBITMQ_HOST"]
    comparison_prefix = os.environ["COMPARISON_QUEUE_PREFIX"]
    final_prefix = os.environ["FINAL_AVERAGE_QUEUE_PREFIX"]
    results_prefix = os.environ["RESULTS_QUEUE_PREFIX"]
    idle_seconds = float(os.environ["PUBLISHER_IDLE_SECONDS"])

    def open_results_queue(gateway_id):
        return ReconnectingPublisher(
            lambda: MessageMiddlewareQueueRabbitMQ(host, f"{results_prefix}.{gateway_id}"), idle_seconds,
        )

    connections = []
    try:
        input_queue = MessageMiddlewareQueueRabbitMQ(host, f"{comparison_prefix}.{comparator_id}")
        connections.append(input_queue)
        average_input = MessageMiddlewareQueueRabbitMQ(host, f"{final_prefix}.{comparator_id}")
        connections.append(average_input)
        worker = Comparator(
            comparator_id, input_queue, average_input, ClientFiles(os.environ["DATA_DIR"], f"comparator_{comparator_id}"),
            open_results_queue, processor_amount, max_rows, max_bytes, drain_rows,
        )
        connections.append(worker)
        signal.signal(signal.SIGTERM, lambda signum, frame: worker.stop())
        worker.run()
    finally:
        close_all(connections)
    return 0


if __name__ == "__main__":
    sys.exit(main())
