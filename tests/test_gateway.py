import socket
import threading
import time
import unittest

from common.protocol import articles, external, internal
from common.protocol.articles import Article
from server import Server


def article(i):
    return Article(f"Articulo {i}", f"https://fr.wikipedia.org/wiki/{i}", {"identifier": "fr"}, None,
                   1662214022000 + i, f"Resumen {i}", "[]", None)


class FakeQueue:

    def __init__(self):
        self.messages = []
        self.closed = False

    def send(self, message):
        self.messages.append(message)

    def close(self):
        self.closed = True


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class InternalProtocolTest(unittest.TestCase):

    def test_articles_round_trip(self):
        message = internal.encode_articles("c1", 3, [article(1), article(2)])
        msg_type, value = internal.decode(message)
        self.assertEqual(msg_type, internal.MsgType.ARTICLES)
        self.assertEqual((value["client_id"], value["gateway_id"]), ("c1", 3))
        self.assertEqual(value["articles"], [article(1), article(2)])

    def test_eof_round_trip(self):
        msg_type, value = internal.decode(internal.encode_eof("c1", 3, "gateway_3", 5, 5, 5))
        self.assertEqual(msg_type, internal.MsgType.EOF)
        self.assertEqual((value["client_id"], value["gateway_id"], value["origin"]), ("c1", 3, "gateway_3"))
        self.assertEqual((value["processed"], value["total"], value["sent"]), (5, 5, 5))
        self.assertNotIn("query", value)

    def test_articles_carry_their_msg_id(self):
        _, value = internal.decode(internal.encode_articles("c1", 3, [article(1)], "b7"))
        self.assertEqual(value["msg_id"], "b7")
        self.assertEqual(value["articles"], [article(1)])
        self.assertNotIn("msg_id", internal.decode(internal.encode_articles("c1", 3, [article(1)]))[1])

    def test_results_carry_their_msg_id(self):
        _, value = internal.decode(internal.encode_results("c1", 3, 1, [["url", 1]], "b7:q1"))
        self.assertEqual((value["query"], value["msg_id"]), (1, "b7:q1"))

    def test_rejects_a_msg_id_that_is_not_a_string(self):
        for msg_id in ('""', "7", "null", "[1]"):
            message = b'{"type":5,"client_id":"c1","gateway_id":0,"query":1,"msg_id":' + msg_id.encode() + b',"rows":[]}'
            with self.assertRaises(external.ProtocolError):
                internal.decode(message)

    def test_invalid_messages(self):
        for message in (b"[1, 2]", b'{"type": 99, "client_id": "c1"}', b'{"type": 1}'):
            with self.assertRaises(external.ProtocolError):
                internal.decode(message)


class GatewayTest(unittest.TestCase):

    def setUp(self):
        self.queues = []
        self.port = free_port()
        self.server = Server("127.0.0.1", self.port, 7, self.open_queue)
        self.thread = threading.Thread(target=self.server.run)
        self.thread.start()

    def tearDown(self):
        self.server.stop()
        self.thread.join(timeout=10)

    def open_queue(self):
        queue = FakeQueue()
        self.queues.append(queue)
        return queue

    def connect(self):
        for _ in range(50):
            try:
                return socket.create_connection(("127.0.0.1", self.port))
            except ConnectionRefusedError:
                time.sleep(0.05)
        raise AssertionError("El gateway no aceptó conexiones")

    def wait_for(self, condition):
        for _ in range(100):
            if condition():
                return
            time.sleep(0.05)
        raise AssertionError("No se cumplió la condición a tiempo")

    def send_batches(self, sock, article_list, columns=Article._fields):
        for i in range(0, len(article_list), 2):
            rows = [articles.encode_row(list(a)) for a in article_list[i:i + 2]]
            external.send_message(sock, external.MsgType.ARTICLES, articles.encode_batch(columns, rows))

    def test_publishes_every_batch_and_one_eof(self):
        sent = [article(i) for i in range(10)]
        with self.connect() as sock:
            self.send_batches(sock, sent)
            external.send_message(sock, external.MsgType.END_OF_DATA)
            self.wait_for(lambda: self.queues and self.queues[0].closed)

        decoded = [internal.decode(m) for m in self.queues[0].messages]
        types = [t for t, _ in decoded]
        self.assertEqual(types, [internal.MsgType.ARTICLES] * 5 + [internal.MsgType.EOF])
        client_ids = {v["client_id"] for _, v in decoded}
        self.assertEqual(len(client_ids), 1)
        self.assertEqual({v["gateway_id"] for _, v in decoded[:-1]}, {7})
        self.assertEqual([a for _, v in decoded[:-1] for a in v["articles"]], sent)
        eof = decoded[-1][1]
        self.assertEqual((eof["origin"], eof["gateway_id"]), ("gateway_7", 7))
        self.assertEqual((eof["processed"], eof["total"], eof["sent"]), (5, 5, 5))

    def test_numbers_the_batches_of_each_client_from_zero(self):
        with self.connect() as a, self.connect() as b:
            self.send_batches(a, [article(i) for i in range(6)])
            self.send_batches(b, [article(i) for i in range(4)])
            external.send_message(a, external.MsgType.END_OF_DATA)
            external.send_message(b, external.MsgType.END_OF_DATA)
            self.wait_for(lambda: len(self.queues) == 2 and all(q.closed for q in self.queues))

        ids = sorted([internal.decode(m)[1].get("msg_id") for m in q.messages[:-1]] for q in self.queues)
        self.assertEqual(ids, [["b0", "b1"], ["b0", "b1", "b2"]])
        for queue in self.queues:
            eof = internal.decode(queue.messages[-1])[1]
            self.assertNotIn("msg_id", eof)
            self.assertEqual(eof["total"], len(queue.messages) - 1)

    def test_drops_the_columns_it_does_not_need(self):
        columns = ["references"] + list(Article._fields)
        with self.connect() as sock:
            rows = [articles.encode_row(["muchas referencias"] + list(article(1)))]
            external.send_message(sock, external.MsgType.ARTICLES, articles.encode_batch(columns, rows))
            external.send_message(sock, external.MsgType.END_OF_DATA)
            self.wait_for(lambda: self.queues and self.queues[0].closed)

        message = self.queues[0].messages[0]
        self.assertNotIn(b"muchas referencias", message)
        self.assertEqual(internal.decode(message)[1]["articles"], [article(1)])

    def test_each_client_gets_its_own_id_and_queue(self):
        with self.connect() as a, self.connect() as b:
            self.send_batches(a, [article(1)])
            self.send_batches(b, [article(2)])
            external.send_message(a, external.MsgType.END_OF_DATA)
            external.send_message(b, external.MsgType.END_OF_DATA)
            self.wait_for(lambda: len(self.queues) == 2 and all(q.closed for q in self.queues))

        client_ids = {internal.decode(q.messages[0])[1]["client_id"] for q in self.queues}
        self.assertEqual(len(client_ids), 2)


if __name__ == "__main__":
    unittest.main()
