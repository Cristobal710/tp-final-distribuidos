import json
import random
import threading
import unittest

from chaos import ChaosMonkey
from docker_api import DockerClient

TARGETS = ["classifier_0", "top20_0", "word_counter_0", "monitor_0", "monitor_1"]


class FakeDocker:

    def __init__(self, running, failing=()):
        self.alive = list(running)
        self.failing = set(failing)
        self.killed = []

    def running(self):
        return list(self.alive)

    def kill(self, name):
        if name in self.failing:
            raise RuntimeError("docker no pudo")
        self.killed.append(name)
        self.alive.remove(name)


class FakeResponse:

    def __init__(self, status, body):
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


class ChaosMonkeyTest(unittest.TestCase):

    def monkey(self, docker, kills=1, seed=7):
        return ChaosMonkey(TARGETS, docker, kills, random.Random(seed))

    def test_kills_one_running_target_per_round(self):
        docker = FakeDocker(TARGETS + ["gateway", "rabbitmq"])
        killed = self.monkey(docker).round()
        self.assertEqual(len(killed), 1)
        self.assertIn(killed[0], TARGETS)
        self.assertEqual(docker.killed, killed)

    def test_never_kills_what_is_not_a_target(self):
        docker = FakeDocker(TARGETS + ["gateway", "rabbitmq", "client_0"])
        monkey = self.monkey(docker, kills=2)
        for _ in range(10):
            monkey.round()
        self.assertEqual(sorted(docker.killed), sorted(TARGETS))
        self.assertEqual(sorted(docker.alive), ["client_0", "gateway", "rabbitmq"])

    def test_only_picks_targets_that_are_running(self):
        docker = FakeDocker(["top20_0", "gateway"])
        self.assertEqual(self.monkey(docker, kills=3).round(), ["top20_0"])
        self.assertEqual(self.monkey(docker).round(), [])

    def test_kills_several_at_once_when_asked(self):
        docker = FakeDocker(TARGETS)
        killed = self.monkey(docker, kills=3).round()
        self.assertEqual(len(set(killed)), 3)

    def test_the_same_seed_kills_the_same_containers(self):
        first = self.monkey(FakeDocker(TARGETS), kills=2, seed=3).round()
        second = self.monkey(FakeDocker(TARGETS), kills=2, seed=3).round()
        self.assertEqual(first, second)

    def test_a_failed_kill_does_not_stop_the_round(self):
        docker = FakeDocker(TARGETS, failing=TARGETS[:2])
        with self.assertLogs(level="ERROR"):
            killed = self.monkey(docker, kills=len(TARGETS)).round()
        self.assertEqual(sorted(killed), sorted(TARGETS[2:]))

    def test_a_docker_that_does_not_answer_skips_the_round(self):
        class Down:
            def running(self):
                raise OSError("sin socket")

        with self.assertLogs(level="ERROR"):
            self.assertEqual(self.monkey(Down()).round(), [])

    def test_stops_between_rounds(self):
        docker = FakeDocker(TARGETS)
        monkey = self.monkey(docker)
        thread = threading.Thread(target=monkey.run, args=(60,))
        thread.start()
        monkey.stop()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(docker.killed, [])

    def test_rejects_rounds_without_kills(self):
        with self.assertRaises(ValueError):
            ChaosMonkey(TARGETS, FakeDocker(TARGETS), 0)


class DockerClientTest(unittest.TestCase):

    def test_lists_the_names_of_the_running_containers(self):
        body = json.dumps([{"Names": ["/top20_0"]}, {"Names": ["/gateway"]}]).encode()
        connection = FakeConnection(200, body)
        self.assertEqual(DockerClient("/sock", connect=lambda: connection).running(), ["top20_0", "gateway"])
        self.assertEqual(connection.requests, [("GET", "/containers/json")])
        self.assertTrue(connection.closed)

    def test_kills_the_container_by_name(self):
        connection = FakeConnection(204)
        DockerClient("/sock", connect=lambda: connection).kill("word_counter_1")
        self.assertEqual(connection.requests, [("POST", "/containers/word_counter_1/kill")])
        self.assertTrue(connection.closed)

    def test_fails_when_docker_does_not_kill_it(self):
        connection = FakeConnection(409, b"is not running")
        with self.assertRaises(RuntimeError):
            DockerClient("/sock", connect=lambda: connection).kill("top20_0")
        self.assertTrue(connection.closed)


if __name__ == "__main__":
    unittest.main()
