# Lo que se sabe de un cliente a partir de los EOF y de los mensajes que llegaron de la etapa anterior.
class _ClientState:

    def __init__(self):
        self.origins = set()
        self.processed = 0
        self.total = None
        self.sent = 0
        self.received = 0


# Lleva la cuenta de los EOF de la etapa anterior para saber cuando terminaron los datos de un cliente.
# Cada EOF trae processed (lo que proceso ese origen), total (lo que se esperaba en total) y sent (lo que mando
# a este destino). Los LATE_EOF suman lo que un origen proceso despues de haber mandado su EOF.
class UpstreamTracker:

    # check_total pide que lo procesado por todos los origenes sea igual al total esperado.
    # check_received pide haber recibido todos los mensajes que los origenes dicen haber mandado.
    def __init__(self, expected_origins, check_total=True, check_received=False):
        self._expected_origins = expected_origins
        self._check_total = check_total
        self._check_received = check_received
        self._clients = {}

    # Devuelve el estado de un cliente y lo crea si todavia no existe.
    def _state(self, client_id):
        if client_id not in self._clients:
            self._clients[client_id] = _ClientState()
        return self._clients[client_id]

    # Registra el EOF de un origen, ignorandolo si ese origen ya habia mandado el suyo.
    def add_eof(self, value):
        state = self._state(value["client_id"])
        if value["origin"] in state.origins:
            return
        state.origins.add(value["origin"])
        state.processed += value["processed"]
        state.total = value["total"]
        state.sent += value["sent"]

    # Registra lo que un origen proceso despues de haber mandado su EOF.
    def add_late_eof(self, value):
        state = self._state(value["client_id"])
        state.processed += value["processed"]
        state.sent += value["sent"]

    # Registra un mensaje de datos recibido.
    def add_message(self, client_id):
        self._state(client_id).received += 1

    # Dice si ya llegaron todos los EOF y se cumplen las cuentas pedidas.
    def is_complete(self, client_id):
        state = self._clients.get(client_id)
        if state is None or len(state.origins) < self._expected_origins:
            return False
        if self._check_total and state.processed != state.total:
            return False
        if self._check_received and state.received != state.sent:
            return False
        return True

    # Devuelve la suma de lo que los origenes dijeron haber mandado.
    def total_sent(self, client_id):
        return self._clients[client_id].sent

    # Devuelve cuantos mensajes de datos se recibieron del cliente.
    def received(self, client_id):
        state = self._clients.get(client_id)
        return state.received if state else 0

    # Se olvida de un cliente que ya termino.
    def forget(self, client_id):
        self._clients.pop(client_id, None)
