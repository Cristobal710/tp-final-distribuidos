import logging
import socket
import threading
import uuid

from common.protocol import articles, external, internal


# Acepta clientes por TCP y publica sus articulos en la cola de clasificacion, con un hilo por cliente.
class Server:

    def __init__(self, host, port, gateway_id, open_classification_queue):
        self._address = (host, port)
        self._gateway_id = gateway_id
        self._open_classification_queue = open_classification_queue
        self._server_socket = None
        self._lock = threading.Lock()
        self._clients = []
        self._stopping = False

    # Escucha conexiones y lanza un hilo por cliente hasta que se pida parar.
    def run(self):
        self._server_socket = socket.create_server(self._address)
        logging.info(f"Escuchando en {self._address[0]}:{self._address[1]}")
        try:
            while True:
                try:
                    client_socket, address = self._server_socket.accept()
                except OSError:
                    if self._stopping:
                        break
                    raise
                handler = threading.Thread(target=self._handle_client, args=(client_socket, address))
                with self._lock:
                    self._clients.append((handler, client_socket))
                handler.start()
        finally:
            self._server_socket.close()
            self._stop_clients()
        logging.info("Gateway detenido")

    # Deja de aceptar clientes, destrabando el accept desde el handler de SIGTERM.
    def stop(self):
        self._stopping = True
        try:
            self._server_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._server_socket.close()

    # Corta los sockets de los clientes y espera a que terminen sus hilos.
    def _stop_clients(self):
        with self._lock:
            clients = list(self._clients)
        for _, client_socket in clients:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        for handler, _ in clients:
            handler.join()

    # Recibe los batches de un cliente, se queda con las columnas necesarias y los publica hasta el fin de datos.
    def _handle_client(self, client_socket, address):
        client_id = uuid.uuid4().hex
        logging.info(f"Cliente {client_id} conectado desde {address[0]}:{address[1]}")
        classification_queue = None
        batches = 0
        with client_socket:
            try:
                classification_queue = self._open_classification_queue()
                while True:
                    msg_type, value = external.receive_message(client_socket)
                    if msg_type == external.MsgType.ARTICLES:
                        self._publish_articles(classification_queue, client_id, articles.decode_batch(value))
                        batches += 1
                    elif msg_type == external.MsgType.END_OF_DATA:
                        self._publish_end_of_data(classification_queue, client_id, batches)
                        return
            except ConnectionError:
                if not self._stopping:
                    logging.error(f"Cliente {client_id}: se cortó la conexión antes del fin de los datos")
            except external.ProtocolError as e:
                logging.error(f"Cliente {client_id}: mensaje inválido: {e}")
            except Exception as e:
                logging.error(f"Cliente {client_id}: no se pudo publicar en clasificacion: {e}")
            finally:
                if classification_queue is not None:
                    classification_queue.close()

    # Publica un batch de articulos del cliente en la cola de clasificacion.
    def _publish_articles(self, classification_queue, client_id, article_list):
        classification_queue.send(internal.encode_articles(client_id, self._gateway_id, article_list))
        logging.info(f"Cliente {client_id}: {len(article_list)} artículos publicados en clasificacion")

    # Publica el fin de datos del cliente en la cola de clasificacion, con la cantidad de batches que publico.
    def _publish_end_of_data(self, classification_queue, client_id, batches):
        origin = f"gateway_{self._gateway_id}"
        classification_queue.send(internal.encode_eof(client_id, self._gateway_id, origin, batches, batches, batches))
        logging.info(f"Cliente {client_id}: EOF publicado en clasificacion ({batches} batches)")
