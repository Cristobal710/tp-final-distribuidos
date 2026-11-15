import logging
import os
import random
import signal
import sys

from chaos import ChaosMonkey
from docker_api import DockerClient

CHAOS_TARGETS = [name.strip() for name in os.environ["CHAOS_TARGETS"].split(",") if name.strip()]
CHAOS_INTERVAL_SECONDS = float(os.environ["CHAOS_INTERVAL_SECONDS"])
CHAOS_KILLS_PER_ROUND = int(os.environ["CHAOS_KILLS_PER_ROUND"])
CHAOS_SEED = os.environ.get("CHAOS_SEED", "")
DOCKER_SOCKET = os.environ["DOCKER_SOCKET"]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rng = random.Random(CHAOS_SEED) if CHAOS_SEED else random.Random()
    monkey = ChaosMonkey(CHAOS_TARGETS, DockerClient(DOCKER_SOCKET), CHAOS_KILLS_PER_ROUND, rng)
    signal.signal(signal.SIGTERM, lambda signum, frame: monkey.stop())
    monkey.run(CHAOS_INTERVAL_SECONDS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
