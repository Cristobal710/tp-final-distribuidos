import logging
import threading

from common.protocol import external, internal

START_OF_2021 = 1609459200000
Q1 = 1
Q1_LANGUAGE = "en"
TEXT = "texto"
BIO = "bio"
RESULTS = "resultados"
STOP_POLL_SECONDS = 1


# Dice si el articulo se escribio hasta 2020, o None si no tiene fecha de creacion.
def until_2020(article):
    if article.date_created is None:
        return None
    return article.date_created < START_OF_2021


# Devuelve el idioma del articulo, o None si no lo tiene.
def language_of(article):
    return (article.in_language or {}).get("identifier")


# Devuelve la url de la imagen del articulo, o None si no tiene.
def image_url_of(article):
    return (article.image or {}).get("content_url")


# Arma las filas de texto, bio y Q1 de un batch, salteando los datos vacios que cada consulta necesita.
def classify(article_list):
    texts, bios, q1_results = [], [], []
    for article in article_list:
        period = until_2020(article)
        language = language_of(article)
        if article.abstract is not None:
            texts.append(internal.Text(article.url, article.abstract, period, language))
        if not period:
            continue
        image_url = image_url_of(article)
        if article.sections is not None and image_url is not None:
            bios.append(internal.Bio(article.name, article.url, article.sections, image_url))
        if language == Q1_LANGUAGE and article.date_modified is not None:
            q1_results.append(internal.Q1Result(article.url, article.date_modified))
    return texts, bios, q1_results


# Clasifica los batches de los clientes con un hilo de datos y coordina los EOF con los demas clasificadores en un hilo de control.
class Classifier:

    def __init__(self, classifier_id, classification_queue, control_input, control_output,
                 text_queue, bio_queue, open_results_queue, in_flight_messages):
        self._classifier_id = classifier_id
        self._origin = f"classifier_{classifier_id}"
        self._classification_queue = classification_queue
        self._control_input = control_input
        self._control_output = control_output
        self._text_queue = text_queue
        self._bio_queue = bio_queue
        self._open_results_queue = open_results_queue
        self._in_flight_messages = in_flight_messages
        self._results_queues = {}
        self._processed = {}
        self._sent = {}
        self._finished = {}
        self._processed_messages = 0
        self._control_thread = threading.Thread(target=self._consume_control)
        self._stopping = False

    # Arranca el hilo de control y consume la cola de clasificacion hasta que se pida parar, y al final frena el hilo de control.
    def run(self):
        logging.info(f"Clasificador {self._classifier_id} consumiendo de clasificacion")
        self._control_thread.start()
        try:
            if not self._stopping:
                self._classification_queue.start_consuming(self._on_message)
        finally:
            self._stopping = True
            while self._control_thread.is_alive():
                self._control_input.stop_consuming()
                self._control_thread.join(timeout=STOP_POLL_SECONDS)
        logging.info(f"Clasificador {self._classifier_id} detenido")

    # Pide cortar el consumo de clasificacion, desde el handler de SIGTERM.
    def stop(self):
        self._stopping = True
        self._classification_queue.stop_consuming()

    # Cierra las colas de resultados que abrio.
    def close(self):
        for results_queue in self._results_queues.values():
            results_queue.close()
        self._results_queues.clear()

    # Consume los EOF que avisan los clasificadores y, si se corta por un error, frena todo el clasificador.
    def _consume_control(self):
        try:
            if not self._stopping:
                self._control_input.start_consuming(self._on_control_message)
        except Exception as e:
            logging.error(f"Se cortó el consumo de control: {e}")
            self.stop()

    # En el hilo de control: al llegar un EOF le pide al hilo de datos que termine ese cliente.
    def _on_control_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error(f"Mensaje de control inválido descartado: {e}")
            ack()
            return
        if msg_type == internal.MsgType.EOF:
            self._classification_queue.run_in_consumer_thread(lambda: self._finish_client(value))
        ack()

    # En el hilo de datos: clasifica cada batch y reenvia el EOF del gateway a todos los clasificadores.
    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error(f"Mensaje inválido descartado: {e}")
            ack()
            return
        if msg_type == internal.MsgType.ARTICLES:
            self._process_articles(value["client_id"], value["gateway_id"], value["articles"])
        elif msg_type == internal.MsgType.EOF:
            self._control_output.send(message)
            logging.info(f"Cliente {value['client_id']}: EOF de {value['origin']} reenviado a los clasificadores")
        self._processed_messages += 1
        self._forget_finished_clients()
        ack()

    # Publica las filas de un batch en cada cola y lo cuenta, o manda un LATE_EOF si el cliente ya habia terminado.
    def _process_articles(self, client_id, gateway_id, article_list):
        texts, bios, q1_results = classify(article_list)
        sent = {TEXT: 0, BIO: 0, RESULTS: 0}
        if texts:
            self._text_queue.send(internal.encode_texts(client_id, gateway_id, texts))
            sent[TEXT] = 1
        if bios:
            self._bio_queue.send(internal.encode_bios(client_id, gateway_id, bios))
            sent[BIO] = 1
        if q1_results:
            self._results_queue(gateway_id).send(internal.encode_results(client_id, gateway_id, Q1, q1_results))
            sent[RESULTS] = 1
        logging.info(
            f"Cliente {client_id}: {len(article_list)} artículos -> "
            f"{len(texts)} texto, {len(bios)} bio, {len(q1_results)} Q1"
        )
        if client_id in self._finished:
            self._send_eofs(
                gateway_id, sent,
                lambda count, query=None: internal.encode_late_eof(client_id, gateway_id, self._origin, 1, count, query),
            )
            logging.info(f"Cliente {client_id}: batch tardío, LATE_EOF enviado")
            return
        self._processed[client_id] = self._processed.get(client_id, 0) + 1
        client_sent = self._sent.setdefault(client_id, {TEXT: 0, BIO: 0, RESULTS: 0})
        for destination, count in sent.items():
            client_sent[destination] += count

    # Manda el EOF propio de un cliente a cada destino con lo que proceso y le mando, y lo marca como terminado.
    def _finish_client(self, eof):
        client_id, gateway_id, total = eof["client_id"], eof["gateway_id"], eof["sent"]
        processed = self._processed.pop(client_id, 0)
        sent = self._sent.pop(client_id, {TEXT: 0, BIO: 0, RESULTS: 0})
        self._send_eofs(
            gateway_id, sent,
            lambda count, query=None: internal.encode_eof(
                client_id, gateway_id, self._origin, processed, total, count, query
            ),
        )
        self._finished[client_id] = self._processed_messages
        logging.info(f"Cliente {client_id}: EOF enviado ({processed} de {total} batches procesados acá)")

    # Manda un aviso de fin a texto, bio y resultados, armado con lo que se le mando a cada uno.
    def _send_eofs(self, gateway_id, sent, encode):
        self._text_queue.send(encode(sent[TEXT]))
        self._bio_queue.send(encode(sent[BIO]))
        self._results_queue(gateway_id).send(encode(sent[RESULTS], Q1))

    # Olvida los clientes terminados que ya no pueden tener batches en vuelo.
    def _forget_finished_clients(self):
        for client_id, finished_at in list(self._finished.items()):
            if self._processed_messages - finished_at >= self._in_flight_messages:
                del self._finished[client_id]

    # Devuelve la cola de resultados del gateway, abriendola la primera vez.
    def _results_queue(self, gateway_id):
        if gateway_id not in self._results_queues:
            self._results_queues[gateway_id] = self._open_results_queue(gateway_id)
        return self._results_queues[gateway_id]
