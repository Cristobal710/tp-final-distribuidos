import logging
import threading
from dataclasses import dataclass, field

from common.eof import UpstreamTracker
from common.protocol import external, internal


@dataclass
class _Session:
    socket: object 
    trackers: dict
    lock: object = field(default_factory=threading.Lock)
    done: object = field(default_factory=threading.Event)
    finished_queries: set = field(default_factory=set)
    input_finished: bool = False

# Se encarga de entregar los resultados a los clientes
# Cada cliente tiene una Session abierta, con el socket, un trackeo de que Queries terminaron, etc
# La idea es trackear en un objecto aparte los resultados y su entrega
class Results:

    def __init__(self, open_queue, expected_origins):
        if set(expected_origins) != {1, 2, 3, 4, 5} or min(expected_origins.values()) < 1:
            raise ValueError("Se necesitan cantidades de origenes para las cinco consultas")
        self._open_queue = open_queue
        self._expected = expected_origins
        self._sessions = {}
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run)
        self._queue = None
        self._stopping = False
        self._error = None
        self.on_failure = None

    def start(self):
        self._thread.start()
        if not self._ready.wait(30):
            raise TimeoutError("No arranco el consumidor de resultados")
        if self._error:
            raise self._error

    def stop(self):
        self._stopping = True
        if self._queue is not None:
            self._queue.stop_consuming()

    def close(self):
        self.stop()
        if self._thread.ident is not None:
            self._thread.join(timeout=10)

    def register(self, client_id, sock):
        session = _Session(sock, {q: UpstreamTracker(n, check_total=q in (1, 2), check_received=True)
                                  for q, n in self._expected.items()})
        with self._lock:
            self._sessions[client_id] = session
        return session

    def unregister(self, client_id):
        with self._lock:
            self._sessions.pop(client_id, None)

    def finish_input(self, client_id):
        with self._lock:
            session = self._sessions[client_id]
        with session.lock:
            session.input_finished = True
            self._maybe_finish(session)

    def _run(self):
        try:
            self._queue = self._open_queue()
            self._ready.set()
            if not self._stopping:
                self._queue.start_consuming(self._on_message)
        except Exception as error:
            self._error = error
            self._ready.set()
            logging.exception("Se corto el consumidor de resultados")
            if self.on_failure is not None:
                self.on_failure()
        finally:
            if self._queue is not None:
                self._queue.close()

    def _on_message(self, message, ack, nack):
        try:
            kind, value = internal.decode(message)
            query = value.get("query")
            if type(query) is not int or query not in self._expected:
                raise external.ProtocolError("El resultado necesita una consulta valida")
            if kind not in (internal.MsgType.RESULTS, internal.MsgType.EOF, internal.MsgType.LATE_EOF):
                raise external.ProtocolError("Tipo de resultado invalido")
        except external.ProtocolError as error:
            logging.error("Resultado invalido descartado: %s", error)
            ack()
            return
        client_id = value["client_id"]
        with self._lock:
            session = self._sessions.get(client_id)
        if session is None:
            logging.warning("Resultado sin sesion activa: %s", client_id)
            ack()
            return
        try:
            with session.lock:
                if query in session.finished_queries:
                    raise external.ProtocolError(f"Q{query} recibio datos despues de terminar")
                tracker = session.trackers[query]
                if kind == internal.MsgType.RESULTS:
                    external.send_message(session.socket, external.MsgType.RESULTS,
                                          external.serialize({"query": query, "rows": value["rows"]}))
                    tracker.add_message(client_id)
                elif kind == internal.MsgType.EOF:
                    tracker.add_eof(value)
                else:
                    tracker.add_late_eof(value)
                if tracker.is_complete(client_id):
                    external.send_message(session.socket, external.MsgType.QUERY_DONE,
                                          external.serialize({"query": query}))
                    session.finished_queries.add(query)
                    self._maybe_finish(session)
        except (OSError, ConnectionError, external.ProtocolError) as error:
            logging.error("Cliente %s: fallo entregando resultados: %s", client_id, error)
            try:
                external.send_message(session.socket, external.MsgType.ERROR, external.serialize({"message": str(error)}))
            except (OSError, ConnectionError):
                pass
            session.done.set()
        ack()

    def _maybe_finish(self, session):
        if session.input_finished and len(session.finished_queries) == 5 and not session.done.is_set():
            external.send_message(session.socket, external.MsgType.ALL_DONE)
            session.done.set()
