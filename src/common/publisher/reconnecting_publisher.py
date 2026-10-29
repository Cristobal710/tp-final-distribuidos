import time


# Publicador que abre su conexion recien cuando la necesita y la renueva si estuvo mucho tiempo sin usarse.
# Un publicador ocioso puede perder su conexion por falta de heartbeats.
class ReconnectingPublisher:

    def __init__(self, open_publisher, idle_seconds=30, clock=time.monotonic):
        self._open = open_publisher
        self._idle_seconds = idle_seconds
        self._clock = clock
        self._publisher = None
        self._last_used = None

    # Envia los mensajes, crea una conexion nueva cada idle_seconds o mas al enviar un mensaje. 
    # Si falla el envio, el mensaje puede haber llegado al broker, ahora esperamos que el broker confirme el envio.
    def send(self, message):
        if self._publisher is not None and self._clock() - self._last_used >= self._idle_seconds:
            self._discard()
        if self._publisher is None:
            self._publisher = self._open()
        try:
            self._publisher.send(message)
        except Exception:
            self._discard()
            raise
        self._last_used = self._clock()

    # Cierra la conexion si esta abierta.
    def close(self):
        self._discard()

    # Cierra la conexion actual sin importar si ya estaba rota.
    def _discard(self):
        if self._publisher is not None:
            try:
                self._publisher.close()
            except Exception:
                pass
        self._publisher = None
