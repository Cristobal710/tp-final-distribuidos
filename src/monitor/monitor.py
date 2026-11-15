import logging
import time

from common.heartbeat import OK, PING, PONG, parse_pong, ping_message, pong_reply

ELECTION = "ELECTION"
ANSWER = "ANSWER"
COORDINATOR = "COORDINATOR"

FOLLOWER = "seguidor"
ELECTING = "eligiendo"
LEADER = "lider"


class Monitor:

    def __init__(self, monitor_id, monitors, nodes, transport, restart, clock=time.monotonic, ping_interval=1.0,
                 missed_pings=3, grace_seconds=10.0, election_timeout=2.0):
        if not 0 <= monitor_id < len(monitors):
            raise ValueError("El id del monitor tiene que estar entre los monitores")
        if ping_interval <= 0 or missed_pings < 1 or grace_seconds < 0 or election_timeout <= 0:
            raise ValueError("Los tiempos tienen que ser positivos")
        self._id = monitor_id
        self._monitors = list(monitors)
        self._nodes = list(nodes)
        self._transport = transport
        self._restart = restart
        self._clock = clock
        self._ping_interval = ping_interval
        self._timeout = missed_pings * ping_interval
        self._grace_seconds = grace_seconds
        self._election_timeout = election_timeout
        self._role = FOLLOWER
        self._leader = None
        self._answered = False
        self._election_deadline = None
        self._next_ping = 0.0
        self._sequence = 0
        self._last_seen = {}
        self._stopping = False

    @property
    def role(self):
        return self._role

    @property
    def leader(self):
        return self._leader

    def run(self):
        self.start()
        while not self._stopping:
            received = self._transport.receive(self._wait_seconds())
            if received is not None:
                self.handle(*received)
            self.tick()

    def stop(self):
        self._stopping = True

    def start(self):
        logging.info("Monitor %s arrancando", self._id)
        self._start_election()

    def handle(self, message, address):
        parts = message.decode(errors="replace").split()
        if not parts:
            return
        kind = parts[0]
        if kind == PING:
            reply = pong_reply(message, OK)
            if reply is not None:
                self._transport.reply(address, reply)
            return
        if kind == PONG:
            pong = parse_pong(message)
            if pong is not None:
                self._last_seen[pong[0].rpartition(":")[0]] = self._clock()
            return
        sender = self._sender_of(parts)
        if sender is None:
            return
        if kind == ELECTION:
            self._on_election(sender)
        elif kind == ANSWER:
            self._on_answer(sender)
        elif kind == COORDINATOR:
            self._on_coordinator(sender)

    def tick(self):
        now = self._clock()
        if self._role == ELECTING and now >= self._election_deadline:
            if self._answered:
                logging.info("Monitor %s: nadie se proclamo lider, vuelve a elegir", self._id)
                self._start_election()
            else:
                self._become_leader()
        if now < self._next_ping:
            return
        self._next_ping = now + self._ping_interval
        if self._role == LEADER:
            self._announce()
            self._watch_targets(now)
        elif self._role == FOLLOWER:
            self._watch_leader(now)

    def _wait_seconds(self):
        deadlines = [self._next_ping]
        if self._role == ELECTING:
            deadlines.append(self._election_deadline)
        return max(0.0, min(deadlines) - self._clock())

    def _sender_of(self, parts):
        if len(parts) != 2 or not parts[1].isdigit():
            return None
        sender = int(parts[1])
        if sender == self._id or sender >= len(self._monitors):
            return None
        return sender

    def _on_election(self, sender):
        if sender > self._id:
            return
        self._send(sender, ANSWER)
        if self._role == LEADER:
            self._send(sender, COORDINATOR)
        elif self._role != ELECTING:
            self._start_election()

    def _on_answer(self, sender):
        if sender > self._id and self._role == ELECTING:
            self._answered = True
            self._election_deadline = self._clock() + self._election_timeout

    def _on_coordinator(self, sender):
        if sender < self._id:
            if self._role != ELECTING:
                self._start_election()
            return
        if self._leader != sender:
            logging.info("Monitor %s: el lider es el monitor %s", self._id, sender)
        self._role = FOLLOWER
        self._leader = sender
        self._answered = False
        self._election_deadline = None
        self._last_seen[self._monitors[sender]] = self._clock()

    def _start_election(self):
        higher = range(self._id + 1, len(self._monitors))
        if not higher:
            self._become_leader()
            return
        logging.info("Monitor %s: arranca una eleccion", self._id)
        self._role = ELECTING
        self._leader = None
        self._answered = False
        self._election_deadline = self._clock() + self._election_timeout
        for monitor in higher:
            self._send(monitor, ELECTION)

    def _become_leader(self):
        logging.info("Monitor %s: ahora es el lider", self._id)
        self._role = LEADER
        self._leader = self._id
        self._answered = False
        self._election_deadline = None
        now = self._clock()
        for target in self._targets():
            self._last_seen[target] = now + self._grace_seconds
        self._next_ping = now
        self._announce()

    def _watch_targets(self, now):
        for target in self._targets():
            if now - self._last_seen.get(target, now) > self._timeout:
                logging.warning("Monitor %s: %s no responde, se reinicia", self._id, target)
                self._restart(target)
                self._last_seen[target] = now + self._grace_seconds
            self._ping(target)

    def _watch_leader(self, now):
        if self._leader is None:
            self._start_election()
            return
        leader = self._monitors[self._leader]
        if now - self._last_seen.get(leader, now) > self._timeout:
            logging.warning("Monitor %s: el lider %s no responde", self._id, leader)
            self._start_election()
            return
        self._ping(leader)

    def _announce(self):
        for monitor in range(len(self._monitors)):
            if monitor != self._id:
                self._send(monitor, COORDINATOR)

    def _targets(self):
        return self._nodes + [name for monitor, name in enumerate(self._monitors) if monitor != self._id]

    def _ping(self, target):
        self._sequence += 1
        self._transport.send(target, ping_message(f"{target}:{self._sequence}"))

    def _send(self, monitor, kind):
        self._transport.send(self._monitors[monitor], f"{kind} {self._id}".encode())
