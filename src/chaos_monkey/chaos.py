import logging
import random
import threading


class ChaosMonkey:

    def __init__(self, targets, docker, kills_per_round=1, rng=None):
        if kills_per_round < 1:
            raise ValueError("Tiene que matar al menos un contenedor por ronda")
        self._targets = list(targets)
        self._docker = docker
        self._kills_per_round = kills_per_round
        self._rng = rng or random.Random()
        self._stop = threading.Event()

    def run(self, interval_seconds):
        logging.info("Chaos monkey arrancando: mata %s cada %ss entre %s contenedores",
                     self._kills_per_round, interval_seconds, len(self._targets))
        while not self._stop.wait(interval_seconds):
            self.round()

    def stop(self):
        self._stop.set()

    def round(self):
        try:
            running = set(self._docker.running())
        except Exception:
            logging.exception("Chaos monkey: no se pudo listar los contenedores")
            return []
        candidates = sorted(target for target in self._targets if target in running)
        victims = self._rng.sample(candidates, min(self._kills_per_round, len(candidates)))
        killed = []
        for victim in victims:
            try:
                self._docker.kill(victim)
            except Exception:
                logging.exception("Chaos monkey: no se pudo matar a %s", victim)
                continue
            logging.warning("Chaos monkey: mato a %s", victim)
            killed.append(victim)
        return killed
