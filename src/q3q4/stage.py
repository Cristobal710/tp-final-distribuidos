import logging

from common.protocol import external, internal


# Nodo que consume una cola, guarda lo que le manda la etapa anterior y, cuando esa etapa avisa que termino los datos
# de un cliente, lo cierra. Aca esta el recorrido de cada mensaje; cada nodo dice que hacer con un dato
# (_on_data) y como cerrar a un cliente (_finish_client).
class Stage:

    # label es como se nombra el nodo en los logs. upstream es el UpstreamTracker de la etapa anterior y data_type el
    # tipo de los mensajes con datos que esta etapa recibe de ella.
    def __init__(self, label, input_queue, upstream, data_type):
        self._label = label
        self._input_queue = input_queue
        self._upstream = upstream
        self._data_type = data_type

    # Consume la cola de entrada hasta que se pida parar.
    def run(self):
        logging.info(f"{self._label} consumiendo")
        self._input_queue.start_consuming(self._on_message)
        logging.info(f"{self._label} detenido")

    # Pide cortar el consumo, desde el handler de SIGTERM.
    def stop(self):
        self._input_queue.stop_consuming()

    # Cierra lo que el nodo abrio para mandar. Los nodos que abren colas de salida lo redefinen.
    def close(self):
        pass

    # Guarda los datos del mensaje, anota los EOF de la etapa anterior y termina al cliente cuando esta todo.
    def _on_message(self, message, ack, nack):
        try:
            msg_type, value = internal.decode(message)
        except external.ProtocolError as e:
            logging.error(f"Mensaje inválido descartado: {e}")
            ack()
            return
        client_id = value["client_id"]
        if msg_type == self._data_type:
            self._on_data(client_id, value)
            self._upstream.add_message(client_id)
        elif msg_type == internal.MsgType.EOF:
            self._upstream.add_eof(value)
        elif msg_type == internal.MsgType.LATE_EOF:
            self._upstream.add_late_eof(value)
        else:
            logging.warning(f"Mensaje de tipo {msg_type.name} ignorado")
        if self._upstream.is_complete(client_id):
            self._finish_client(client_id, value["gateway_id"], self._upstream.received(client_id))
            self._upstream.forget(client_id)
        ack()

    # Guarda lo que trajo un mensaje con datos de un cliente.
    def _on_data(self, client_id, value):
        raise NotImplementedError

    # Manda lo que le toca al nodo cuando terminaron los datos del cliente. received es cuantos mensajes con datos
    # recibio de la etapa anterior para ese cliente.
    def _finish_client(self, client_id, gateway_id, received):
        raise NotImplementedError
