import threading
import unittest

from restarter import BackgroundRestarter, DockerRestarter


class FakeResponse:

    def __init__(self, status, body=b""):
        self.status = status
        self.body = body

    def read(self):
        return self.body


class FakeConnection:

    def __init__(self, status, body=b""):
        self.response = FakeResponse(status, body)
        self.requests = []
        self.closed = False

    def request(self, method, path):
        self.requests.append((method, path))

    def getresponse(self):
        return self.response

    def close(self):
        self.closed = True


class DockerRestarterTest(unittest.TestCase):

    def test_asks_docker_to_restart_the_container_by_name(self):
        connection = FakeConnection(204)
        DockerRestarter("/sock", connect=lambda: connection).restart("word_counter_1")
        self.assertEqual(connection.requests, [("POST", "/containers/word_counter_1/restart?t=0")])
        self.assertTrue(connection.closed)

    def test_fails_when_docker_does_not_restart_it(self):
        connection = FakeConnection(404, b"no such container")
        with self.assertRaises(RuntimeError):
            DockerRestarter("/sock", connect=lambda: connection).restart("nadie")
        self.assertTrue(connection.closed)


class BackgroundRestarterTest(unittest.TestCase):

    def test_restarts_in_another_thread_and_skips_a_restart_already_running(self):
        release = threading.Event()
        restarted = []

        def restart(name):
            restarted.append(name)
            release.wait(5)

        restarter = BackgroundRestarter(restart)
        first = restarter("top20_0")
        self.assertIsNone(restarter("top20_0"))
        other = restarter("average_0")
        release.set()
        first.join(5)
        other.join(5)
        self.assertEqual(sorted(restarted), ["average_0", "top20_0"])
        again = restarter("top20_0")
        again.join(5)
        self.assertEqual(restarted.count("top20_0"), 2)

    def test_a_failed_restart_does_not_stop_the_next_ones(self):
        def restart(name):
            raise RuntimeError("docker caido")

        restarter = BackgroundRestarter(restart)
        with self.assertLogs(level="ERROR"):
            restarter("top20_0").join(5)
            again = restarter("top20_0")
            self.assertIsNotNone(again)
            again.join(5)


if __name__ == "__main__":
    unittest.main()
