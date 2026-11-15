import socket
import unittest

from common.heartbeat import OK, RECOVERING, HeartbeatServer, parse_pong, ping_message, pong_reply


class HeartbeatMessagesTest(unittest.TestCase):

    def test_a_ping_is_answered_with_its_token_and_the_state(self):
        self.assertEqual(pong_reply(ping_message("top20_0:7"), OK), b"PONG top20_0:7 ok")

    def test_malformed_pings_get_no_answer(self):
        for message in (b"", b"PING", b"PING a b", b"PONG a ok", b"HOLA a"):
            self.assertIsNone(pong_reply(message, OK))

    def test_parses_a_pong(self):
        self.assertEqual(parse_pong(b"PONG classifier_2:3 recuperando"), ("classifier_2:3", RECOVERING))
        for message in (b"PONG a", b"PING a ok", b"", b"PONG a ok extra"):
            self.assertIsNone(parse_pong(message))


class HeartbeatServerTest(unittest.TestCase):

    def setUp(self):
        self.server = HeartbeatServer(0, "127.0.0.1").start()
        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client.settimeout(2)

    def tearDown(self):
        self.client.close()
        self.server.stop()

    def ask(self, message):
        self.client.sendto(message, ("127.0.0.1", self.server.port))
        return self.client.recvfrom(1024)[0]

    def test_answers_each_ping(self):
        self.assertEqual(self.ask(ping_message("n:1")), b"PONG n:1 ok")
        self.assertEqual(self.ask(ping_message("n:2")), b"PONG n:2 ok")

    def test_answers_with_the_current_state(self):
        self.server.set_state(RECOVERING)
        self.assertEqual(self.ask(ping_message("n:1")), b"PONG n:1 recuperando")
        self.server.set_state(OK)
        self.assertEqual(self.ask(ping_message("n:2")), b"PONG n:2 ok")

    def test_ignores_malformed_messages_and_keeps_answering(self):
        self.client.sendto(b"basura", ("127.0.0.1", self.server.port))
        self.assertEqual(self.ask(ping_message("n:1")), b"PONG n:1 ok")

    def test_stops(self):
        self.server.stop()
        self.assertFalse(self.server._thread.is_alive())


if __name__ == "__main__":
    unittest.main()
