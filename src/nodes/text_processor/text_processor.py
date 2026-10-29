import logging
import threading
from typing import List, NamedTuple, Optional

from common.eof import UpstreamTracker
from common.protocol import external, internal
from words import extract_words

STOP_POLL_SECONDS = 1


# Un texto con sus palabras ya sacadas, que es lo que reciben las salidas de cada consulta.
class ProcessedText(NamedTuple):
    url: Optional[str]
    until_2020: Optional[bool]
    language: Optional[str]
    words: List[str]


# Procesa los textos de los clientes con un hilo de datos y coordina los EOF de los clasificadores con los demas
# Text processor en un hilo de control. Cada consulta que lo usa aporta una salida con estos metodos:
#   send_batch(client_id, gateway_id, texts) -> {cola: mensajes mandados}
#   send_eofs(client_id, sent, encode) -> manda encode(cantidad, query=None) a cada cola
#   close()
class TextProcessor:

    def __init__(self, processor_id, text_queue, control_input, control_output, outputs,
                 classifier_amount, stop_words, in_flight_messages):
        self._processor_id = processor_id
        self._origin = f"text_processor_{processor_id}"
        self._text_queue = text_queue
        self._control_input = control_input
        self._control_output = control_output
        self._outputs = outputs
        self._stop_words = stop_words
        self._in_flight_messages = in_flight_messages
        self._classifiers = UpstreamTracker(classifier_amount, check_total=True, check_received=False)
        self._processed = {}
        self._sent = {}
        self._finished = {}
        self._processed_messages = 0
        self._control_thread = threading.Thread(target=self._consume_control)
        self._stopping = False

    # Arranca el hilo de control y consume la cola de texto hasta que se pida parar, y al final frena el hilo de control.
    def run(self):
        logging.info(f"Text processor {self._processor_id} consumiendo de texto")
        self._control_thread.start()
        try:
            if not self._stopping:
                self._text_queue.start_consuming(self._on_message)
        finally:
            self._stopping = True
            while self._control_thread.is_alive():
                self._control_input.stop_consuming()
                self._control_thread.join(timeout=STOP_POLL_SECONDS)
        logging.info(f"Text processor {self._processor_id} detenido")

    # Pide cortar el consumo de texto, desde el handler de SIGTERM.
    def stop(self):
        self._stopping = True
        self._text_queue.stop_consuming()

    # Cierra las colas de las salidas.
    def close(self):
        for output in self._outputs:
            output.close()

    # Consume los EOF que reenvian los Text processor y, si se corta por un error, frena todo el nodo.
    def _consume_control(self):
        try:
            if not self._stopping:
                self._control_input.start_consuming(self._on_control_message)
        except Exception as e:
            logging.error(f"Se cortó el consumo de control: {e}")
            self.stop()

    # En el hilo de control: al llegar un EOF de un clasificador le pide al hilo de datos que lo anote.
    def _on_control_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error(f"Mensaje de control inválido descartado: {e}")
            ack()
            return
        if msg_type in (internal.MsgType.EOF, internal.MsgType.LATE_EOF):
            self._text_queue.run_in_consumer_thread(lambda: self._on_upstream(msg_type, value))
        ack()

    # En el hilo de datos: procesa cada batch de texto y reenvia los EOF de los clasificadores a todos los Text processor.
    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error(f"Mensaje inválido descartado: {e}")
            ack()
            return
        if msg_type == internal.MsgType.TEXTS:
            self._process_texts(value["client_id"], value["gateway_id"], value["texts"])
        elif msg_type in (internal.MsgType.EOF, internal.MsgType.LATE_EOF):
            self._control_output.send(message)
            logging.info(f"Cliente {value['client_id']}: {msg_type.name} de {value['origin']} reenviado a los Text processor")
        self._processed_messages += 1
        self._forget_finished_clients()
        ack()

    # Saca las palabras de un batch, se las da a cada salida y lo cuenta, o manda un LATE_EOF si el cliente ya habia terminado.
    def _process_texts(self, client_id, gateway_id, texts):
        processed_texts = [
            ProcessedText(text.url, text.until_2020, text.language, extract_words(text.abstract, self._stop_words))
            for text in texts
        ]
        sent = {}
        for output in self._outputs:
            for destination, count in output.send_batch(client_id, gateway_id, processed_texts).items():
                sent[destination] = sent.get(destination, 0) + count
        logging.info(f"Cliente {client_id}: {len(texts)} textos procesados")
        if client_id in self._finished:
            for output in self._outputs:
                output.send_eofs(
                    client_id,
                    sent,
                    lambda count, query=None: internal.encode_late_eof(
                        client_id, gateway_id, self._origin, 1, count, query
                    ),
                )
            logging.info(f"Cliente {client_id}: batch tardío, LATE_EOF enviado")
            return
        self._processed[client_id] = self._processed.get(client_id, 0) + 1
        client_sent = self._sent.setdefault(client_id, {})
        for destination, count in sent.items():
            client_sent[destination] = client_sent.get(destination, 0) + count

    # En el hilo de datos: anota el EOF de un clasificador y, si ya estan todos, termina ese cliente.
    def _on_upstream(self, msg_type, value):
        client_id = value["client_id"]
        if client_id in self._finished:
            logging.warning(f"Cliente {client_id}: {msg_type.name} de {value['origin']} ignorado, el cliente ya terminó")
            return
        if msg_type == internal.MsgType.EOF:
            self._classifiers.add_eof(value)
        else:
            self._classifiers.add_late_eof(value)
        if self._classifiers.is_complete(client_id):
            self._finish_client(client_id, value["gateway_id"])

    # Manda el EOF propio de un cliente a cada destino con lo que proceso y le mando, y lo marca como terminado.
    def _finish_client(self, client_id, gateway_id):
        total = self._classifiers.total_sent(client_id)
        processed = self._processed.pop(client_id, 0)
        sent = self._sent.pop(client_id, {})
        for output in self._outputs:
            output.send_eofs(
                client_id,
                sent,
                lambda count, query=None: internal.encode_eof(
                    client_id, gateway_id, self._origin, processed, total, count, query
                ),
            )
        self._classifiers.forget(client_id)
        self._finished[client_id] = self._processed_messages
        logging.info(f"Cliente {client_id}: EOF enviado ({processed} de {total} textos procesados acá)")

    # Olvida los clientes terminados que ya no pueden tener batches en vuelo.
    def _forget_finished_clients(self):
        for client_id, finished_at in list(self._finished.items()):
            if self._processed_messages - finished_at >= self._in_flight_messages:
                del self._finished[client_id]
