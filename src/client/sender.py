import logging
import socket
import time

from common.batching import BatchBuilder
from common.protocol import articles, external


# Manda las filas del dataset al gateway en batches por una conexion TCP.
class Client:

    def __init__(self, server_host, server_port, max_batch_rows, max_batch_bytes, connect_attempts,
                 connect_retry_seconds):
        self._server_address = (server_host, server_port)
        self._connect_attempts = connect_attempts
        self._connect_retry_seconds = connect_retry_seconds
        self._max_batch_rows = max_batch_rows
        self._max_batch_bytes = max_batch_bytes
        self._socket = None
        self.stopped = False
        self.sent_rows = 0
        self.sent_batches = 0

    # Abre la conexion con el gateway.
    def connect(self):
        for attempt in range(1, self._connect_attempts + 1):
            try:
                self._socket = socket.create_connection(self._server_address)
                break
            except ConnectionRefusedError:
                if attempt == self._connect_attempts or self.stopped:
                    raise
                logging.info(f"El gateway todavía no acepta conexiones, reintento {attempt}")
                time.sleep(self._connect_retry_seconds)
        logging.info(f"Conectado a {self._server_address[0]}:{self._server_address[1]}")

    # Manda todas las filas en batches y despues el aviso de fin de datos.
    def send_rows(self, rows):
        builder = BatchBuilder(self._max_batch_rows, self._max_batch_bytes)
        columns = None
        for row in rows:
            if self.stopped:
                return
            row_columns = tuple(row)
            if row_columns != columns:
                self._send_batch(columns, builder.flush())
                columns = row_columns
            self._send_batch(columns, builder.add(articles.encode_row(row.values())))
        self._send_batch(columns, builder.flush())
        external.send_message(self._socket, external.MsgType.END_OF_DATA)
        logging.info(f"Fin de los datos: {self.sent_rows} filas en {self.sent_batches} batches")

    # Serializa un batch, lo manda y actualiza los contadores.
    def _send_batch(self, columns, encoded_rows):
        if not encoded_rows:
            return
        external.send_message(
            self._socket, external.MsgType.ARTICLES, articles.encode_batch(columns, encoded_rows)
        )
        self.sent_rows += len(encoded_rows)
        self.sent_batches += 1

    # Corta el envio desde el handler de SIGTERM, destrabando el socket si esta esperando.
    def stop(self):
        self.stopped = True
        if self._socket:
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    # Cierra el socket.
    def close(self):
        if self._socket:
            self._socket.close()
            self._socket = None
