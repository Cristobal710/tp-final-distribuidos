import logging
import os
import signal

from common.heartbeat import HeartbeatServer
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.publisher import ReconnectingPublisher


# Deja listo el proceso de un nodo: configura el log y levanta el heartbeat. Devuelve el servidor de heartbeat.
def start_node():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    return HeartbeatServer(int(os.environ["HEARTBEAT_PORT"])).start()


# Arma un publicador que abre la cola de salida recien cuando la usa.
def publisher_of(host, queue_name, idle_seconds):
    return ReconnectingPublisher(lambda: MessageMiddlewareQueueRabbitMQ(host, queue_name), idle_seconds)


# Corre el nodo hasta que termine o reciba SIGTERM, y despues cierra sus colas.
def run_stage(stage, input_queue):
    signal.signal(signal.SIGTERM, lambda signum, frame: stage.stop())
    try:
        stage.run()
    finally:
        stage.close()
        try:
            input_queue.close()
        except Exception as e:
            logging.error(f"Error cerrando una conexión: {e}")
    return 0
