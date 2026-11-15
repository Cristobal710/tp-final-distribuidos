import unittest

from monitor import ELECTING, FOLLOWER, LEADER, Monitor

MONITORS = ["monitor_0", "monitor_1", "monitor_2"]
NODES = ["classifier_0", "top20_0"]


class FakeTransport:

    def __init__(self):
        self.sent = []
        self.replies = []

    def send(self, name, payload):
        self.sent.append((name, payload.decode()))

    def reply(self, address, payload):
        self.replies.append((address, payload.decode()))

    def take(self):
        sent, self.sent = self.sent, []
        return sent


class Clock:

    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


class MonitorTest(unittest.TestCase):

    def monitor(self, monitor_id):
        self.transport = FakeTransport()
        self.clock = Clock()
        self.restarted = []
        return Monitor(
            monitor_id, MONITORS, NODES, self.transport, self.restarted.append, self.clock,
            ping_interval=1.0, missed_pings=3, grace_seconds=10.0, election_timeout=2.0,
        )

    def advance(self, monitor, seconds, answering=()):
        for _ in range(int(seconds)):
            self.clock.now += 1
            monitor.tick()
            for name, message in list(self.transport.sent):
                kind, token = message.split()[:2]
                if kind == "PING" and name in answering:
                    monitor.handle(f"PONG {token} ok".encode(), (name, 9000))

    def pings(self):
        return sorted(name for name, message in self.transport.take() if message.startswith("PING"))

    def test_the_highest_monitor_becomes_leader_at_once(self):
        monitor = self.monitor(2)
        monitor.start()
        self.assertEqual((monitor.role, monitor.leader), (LEADER, 2))
        self.assertEqual(sorted(self.transport.take()),
                         [("monitor_0", "COORDINATOR 2"), ("monitor_1", "COORDINATOR 2")])

    def test_a_lower_monitor_asks_the_higher_ones_and_wins_if_nobody_answers(self):
        monitor = self.monitor(0)
        monitor.start()
        self.assertEqual(monitor.role, ELECTING)
        self.assertEqual(sorted(self.transport.take()), [("monitor_1", "ELECTION 0"), ("monitor_2", "ELECTION 0")])
        self.clock.now += 1.9
        monitor.tick()
        self.assertEqual(monitor.role, ELECTING)
        self.clock.now += 0.2
        monitor.tick()
        self.assertEqual((monitor.role, monitor.leader), (LEADER, 0))
        self.assertIn(("monitor_1", "COORDINATOR 0"), self.transport.sent)
        self.assertIn(("monitor_2", "COORDINATOR 0"), self.transport.sent)

    def test_follows_the_higher_monitor_that_answers_and_proclaims_itself(self):
        monitor = self.monitor(0)
        monitor.start()
        monitor.handle(b"ANSWER 2", ("monitor_2", 9000))
        self.clock.now += 3
        monitor.tick()
        self.assertEqual(monitor.role, ELECTING)
        monitor.handle(b"COORDINATOR 2", ("monitor_2", 9000))
        self.assertEqual((monitor.role, monitor.leader), (FOLLOWER, 2))

    def test_starts_again_if_the_one_that_answered_never_proclaims_itself(self):
        monitor = self.monitor(0)
        monitor.start()
        self.transport.take()
        monitor.handle(b"ANSWER 1", ("monitor_1", 9000))
        self.clock.now += 2.1
        monitor.tick()
        self.assertEqual(monitor.role, ELECTING)
        self.assertEqual(sorted(self.transport.take()), [("monitor_1", "ELECTION 0"), ("monitor_2", "ELECTION 0")])

    def test_the_leader_answers_an_election_and_reminds_who_leads(self):
        monitor = self.monitor(2)
        monitor.start()
        self.transport.take()
        monitor.handle(b"ELECTION 0", ("monitor_0", 9000))
        self.assertEqual(self.transport.take(), [("monitor_0", "ANSWER 2"), ("monitor_0", "COORDINATOR 2")])
        self.assertEqual(monitor.role, LEADER)

    def test_a_follower_answers_a_lower_election_and_runs_its_own(self):
        monitor = self.monitor(1)
        monitor.handle(b"COORDINATOR 2", ("monitor_2", 9000))
        monitor.handle(b"ELECTION 0", ("monitor_0", 9000))
        self.assertEqual(self.transport.take(), [("monitor_0", "ANSWER 1"), ("monitor_2", "ELECTION 1")])
        self.assertEqual(monitor.role, ELECTING)

    def test_a_coordinator_from_a_lower_monitor_makes_the_higher_one_take_over(self):
        monitor = self.monitor(2)
        monitor.handle(b"COORDINATOR 0", ("monitor_0", 9000))
        self.assertEqual((monitor.role, monitor.leader), (LEADER, 2))
        self.assertIn(("monitor_0", "COORDINATOR 2"), self.transport.sent)

    def test_the_leader_pings_every_node_and_the_other_monitors(self):
        monitor = self.monitor(2)
        monitor.start()
        self.transport.take()
        monitor.tick()
        self.assertEqual(self.pings(), sorted(NODES + ["monitor_0", "monitor_1"]))

    def test_the_leader_keeps_announcing_itself_in_case_the_first_one_was_lost(self):
        monitor = self.monitor(2)
        monitor.start()
        self.transport.take()
        self.clock.now += 1
        monitor.tick()
        announced = sorted(item for item in self.transport.take() if item[1].startswith("COORDINATOR"))
        self.assertEqual(announced, [("monitor_0", "COORDINATOR 2"), ("monitor_1", "COORDINATOR 2")])

    def test_two_leaders_end_up_following_the_highest(self):
        lower = self.monitor(1)
        lower.handle(b"COORDINATOR 0", ("monitor_0", 9000))
        self.assertEqual(lower.role, ELECTING)
        self.clock.now += 3
        lower.tick()
        self.assertEqual(lower.role, LEADER)
        lower.handle(b"COORDINATOR 2", ("monitor_2", 9000))
        self.assertEqual((lower.role, lower.leader), (FOLLOWER, 2))

    def test_a_node_that_answers_is_never_restarted(self):
        monitor = self.monitor(2)
        monitor.start()
        self.advance(monitor, 30, answering=NODES + ["monitor_0", "monitor_1"])
        self.assertEqual(self.restarted, [])

    def test_a_new_leader_gives_every_node_the_grace_period_before_judging_it(self):
        monitor = self.monitor(2)
        monitor.start()
        self.advance(monitor, 13)
        self.assertEqual(self.restarted, [])
        self.advance(monitor, 1)
        self.assertEqual(sorted(self.restarted), sorted(NODES + ["monitor_0", "monitor_1"]))

    def test_restarts_a_silent_node_once_and_waits_the_grace_period(self):
        monitor = self.monitor(2)
        monitor.start()
        answering = ["classifier_0", "monitor_0", "monitor_1"]
        self.advance(monitor, 13, answering)
        self.assertEqual(self.restarted, [])
        self.advance(monitor, 1, answering)
        self.assertEqual(self.restarted, ["top20_0"])
        self.advance(monitor, 13, answering)
        self.assertEqual(self.restarted, ["top20_0"])
        self.advance(monitor, 1, answering)
        self.assertEqual(self.restarted, ["top20_0", "top20_0"])

    def test_restarts_a_node_that_stops_answering(self):
        monitor = self.monitor(2)
        monitor.start()
        everyone = NODES + ["monitor_0", "monitor_1"]
        self.advance(monitor, 20, everyone)
        self.advance(monitor, 3, [name for name in everyone if name != "classifier_0"])
        self.assertEqual(self.restarted, [])
        self.advance(monitor, 1, [name for name in everyone if name != "classifier_0"])
        self.assertEqual(self.restarted, ["classifier_0"])

    def test_a_restarted_node_that_comes_back_is_left_alone(self):
        monitor = self.monitor(2)
        monitor.start()
        others = ["classifier_0", "monitor_0", "monitor_1"]
        self.advance(monitor, 14, others)
        self.advance(monitor, 30, others + ["top20_0"])
        self.assertEqual(self.restarted, ["top20_0"])

    def test_the_leader_restarts_a_silent_monitor(self):
        monitor = self.monitor(2)
        monitor.start()
        self.advance(monitor, 14, NODES + ["monitor_0"])
        self.assertEqual(self.restarted, ["monitor_1"])

    def test_a_follower_only_pings_the_leader(self):
        monitor = self.monitor(0)
        monitor.handle(b"COORDINATOR 2", ("monitor_2", 9000))
        self.clock.now += 1
        monitor.tick()
        self.assertEqual(self.pings(), ["monitor_2"])
        self.assertEqual(self.restarted, [])

    def test_a_follower_starts_an_election_when_the_leader_stops_answering(self):
        monitor = self.monitor(0)
        monitor.handle(b"COORDINATOR 2", ("monitor_2", 9000))
        self.advance(monitor, 3)
        self.assertEqual(monitor.role, FOLLOWER)
        self.advance(monitor, 1)
        self.assertEqual(monitor.role, ELECTING)
        self.assertIn(("monitor_2", "ELECTION 0"), self.transport.sent)

    def test_a_follower_keeps_following_a_leader_that_answers(self):
        monitor = self.monitor(0)
        monitor.handle(b"COORDINATOR 2", ("monitor_2", 9000))
        self.advance(monitor, 20, answering=["monitor_2"])
        self.assertEqual((monitor.role, monitor.leader), (FOLLOWER, 2))

    def test_answers_pings_from_other_monitors(self):
        monitor = self.monitor(1)
        monitor.handle(b"PING monitor_1:5", ("10.0.0.2", 9000))
        self.assertEqual(self.transport.replies, [(("10.0.0.2", 9000), "PONG monitor_1:5 ok")])

    def test_ignores_messages_from_itself_unknown_monitors_or_malformed(self):
        monitor = self.monitor(1)
        for message in (b"COORDINATOR 1", b"COORDINATOR 7", b"COORDINATOR x", b"ELECTION", b"", b"OTRO 0"):
            monitor.handle(message, ("x", 9000))
        self.assertEqual((monitor.role, monitor.leader), (FOLLOWER, None))
        self.assertEqual(self.transport.sent, [])

    def test_rejects_an_id_outside_the_monitors(self):
        with self.assertRaises(ValueError):
            Monitor(3, MONITORS, NODES, FakeTransport(), print)


if __name__ == "__main__":
    unittest.main()
