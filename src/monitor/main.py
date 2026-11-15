import logging
import os
import signal
import sys

from monitor import Monitor
from restarter import BackgroundRestarter, DockerRestarter
from transport import UdpTransport

MONITOR_ID = int(os.environ["MONITOR_ID"])
MONITOR_AMOUNT = int(os.environ["MONITOR_AMOUNT"])
WATCHED_NODES = [name.strip() for name in os.environ["WATCHED_NODES"].split(",") if name.strip()]
HEARTBEAT_PORT = int(os.environ["HEARTBEAT_PORT"])
PING_INTERVAL_SECONDS = float(os.environ["PING_INTERVAL_SECONDS"])
MISSED_PINGS = int(os.environ["MISSED_PINGS"])
RESTART_GRACE_SECONDS = float(os.environ["RESTART_GRACE_SECONDS"])
ELECTION_TIMEOUT_SECONDS = float(os.environ["ELECTION_TIMEOUT_SECONDS"])
DOCKER_SOCKET = os.environ["DOCKER_SOCKET"]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    transport = UdpTransport(HEARTBEAT_PORT)
    restart = BackgroundRestarter(DockerRestarter(DOCKER_SOCKET).restart)
    monitors = [f"monitor_{index}" for index in range(MONITOR_AMOUNT)]
    monitor = Monitor(
        MONITOR_ID, monitors, WATCHED_NODES, transport, restart,
        ping_interval=PING_INTERVAL_SECONDS, missed_pings=MISSED_PINGS,
        grace_seconds=RESTART_GRACE_SECONDS, election_timeout=ELECTION_TIMEOUT_SECONDS,
    )
    signal.signal(signal.SIGTERM, lambda signum, frame: monitor.stop())
    try:
        monitor.run()
    finally:
        transport.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
