import socket
import threading
import time
import unittest

from transport import UdpTransport


def wait_until(condition, seconds=2.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return False


class Clock:

    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


class UdpTransportTest(unittest.TestCase):

    def setUp(self):
        self.peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.peer.bind(("127.0.0.1", 0))
        self.peer.settimeout(2)
        self.port = self.peer.getsockname()[1]
        self.clock = Clock()
        self.resolved = []

    def tearDown(self):
        self.peer.close()

    def received(self):
        self.peer.settimeout(0.2)
        payloads = []
        try:
            while True:
                payloads.append(self.peer.recvfrom(1024)[0])
        except OSError:
            return payloads

    def transport(self, resolve):
        def counted(name):
            self.resolved.append(name)
            return resolve(name)
        transport = UdpTransport(self.port, "127.0.0.2", resolve=counted, clock=self.clock, refresh_seconds=5)
        self.addCleanup(transport.close)
        return transport

    def test_sends_to_the_address_of_the_name_once_it_is_resolved(self):
        transport = self.transport(lambda name: "127.0.0.1")
        transport.send("top20_0", b"PING a")
        self.assertTrue(wait_until(lambda: self.resolved == ["top20_0"]))
        time.sleep(0.05)
        transport.send("top20_0", b"PING b")
        self.assertEqual(self.received()[-1], b"PING b")

    def test_a_name_that_takes_long_to_resolve_does_not_block_the_send(self):
        release = threading.Event()
        self.addCleanup(release.set)

        def slow(name):
            release.wait(5)
            raise socket.gaierror("Try again")

        transport = self.transport(slow)
        start = time.monotonic()
        for _ in range(10):
            transport.send("caido_0", b"PING x")
        self.assertLess(time.monotonic() - start, 0.5)
        self.assertEqual(self.resolved, ["caido_0"])

    def test_resolves_the_name_again_after_the_refresh_time(self):
        transport = self.transport(lambda name: "127.0.0.1")
        transport.send("top20_0", b"PING a")
        self.assertTrue(wait_until(lambda: len(self.resolved) == 1))
        time.sleep(0.05)
        self.clock.now += 4
        transport.send("top20_0", b"PING b")
        self.assertEqual(len(self.resolved), 1)
        self.clock.now += 2
        transport.send("top20_0", b"PING c")
        self.assertTrue(wait_until(lambda: len(self.resolved) == 2))

    def test_keeps_the_last_address_when_the_name_stops_resolving(self):
        answers = ["127.0.0.1"]

        def resolve(name):
            if not answers:
                raise socket.gaierror("Try again")
            return answers.pop()

        transport = self.transport(resolve)
        transport.send("top20_0", b"PING a")
        self.assertTrue(wait_until(lambda: len(self.resolved) == 1))
        time.sleep(0.05)
        self.clock.now += 6
        transport.send("top20_0", b"PING b")
        self.assertEqual(self.received()[-1], b"PING b")
        self.assertTrue(wait_until(lambda: len(self.resolved) == 2))
        time.sleep(0.05)
        transport.send("top20_0", b"PING c")
        self.assertEqual(self.received(), [b"PING c"])


if __name__ == "__main__":
    unittest.main()
