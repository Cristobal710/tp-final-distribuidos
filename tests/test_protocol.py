import socket
import unittest
from datetime import datetime, timezone

from common import safe_socket
from common.protocol import articles, external
from common.protocol.articles import Article


def article(**overrides):
    values = dict(
        name="Ada Lovelace",
        url="https://en.wikipedia.org/wiki/Ada_Lovelace",
        in_language={"identifier": "en"},
        date_created=1009843200000,
        date_modified=1727000000000,
        abstract="Augusta Ada King, Countess of Lovelace...",
        sections='[{"type":"section","name":"Biography","has_parts":[{"value":"a \\"quote\\""}]}]',
        image={"content_url": "https://upload.wikimedia.org/ada.jpg", "height": 600, "width": 400},
    )
    values.update(overrides)
    return Article(**values)


def batch_of(*article_list):
    columns = list(Article._fields)
    return articles.encode_batch(columns, [articles.encode_row(a) for a in article_list])


class ShortSocket:

    def __init__(self, chunk):
        self.chunk = chunk
        self.data = bytearray()
        self.read_pos = 0
        self.sends = 0
        self.recvs = 0

    def send(self, data):
        self.sends += 1
        piece = bytes(data[:self.chunk])
        self.data += piece
        return len(piece)

    def recv_into(self, view):
        self.recvs += 1
        piece = self.data[self.read_pos:self.read_pos + min(self.chunk, len(view))]
        view[:len(piece)] = piece
        self.read_pos += len(piece)
        return len(piece)


class SafeSocketTest(unittest.TestCase):

    def test_short_writes_and_reads(self):
        sock = ShortSocket(chunk=3)
        data = bytes(range(256)) * 40
        safe_socket.send_all(sock, data)
        self.assertEqual(bytes(sock.data), data)
        self.assertGreater(sock.sends, 1)
        self.assertEqual(safe_socket.recv_all(sock, len(data)), data)
        self.assertGreater(sock.recvs, 1)

    def test_closed_before_all_data(self):
        sock = ShortSocket(chunk=1000)
        sock.data += b"abc"
        with self.assertRaises(ConnectionError):
            safe_socket.recv_all(sock, 10)

    def test_big_message_with_one_byte_at_a_time(self):
        sock = ShortSocket(chunk=1)
        data = b"x" * 100_000
        safe_socket.send_all(sock, data)
        self.assertEqual(safe_socket.recv_all(sock, len(data)), data)


class SerializationTest(unittest.TestCase):

    def test_dates_become_milliseconds_anywhere(self):
        value = {"a": datetime(2001, 12, 10), "b": [{"c": datetime(1960, 1, 1, tzinfo=timezone.utc)}]}
        self.assertEqual(external.deserialize(external.serialize(value)),
                         {"a": 1007942400000, "b": [{"c": -315619200000}]})

    def test_serialize_list_equals_serializing_the_whole_list(self):
        values = [["été", None, 1], [], ["x"]]
        serialized = external.serialize_list([external.serialize(v) for v in values])
        self.assertEqual(external.deserialize(serialized), values)

    def test_serialize_fields(self):
        serialized = external.serialize_fields({"a": external.serialize([1]), "ñ": external.serialize(None)})
        self.assertEqual(external.deserialize(serialized), {"a": [1], "ñ": None})


class ArticlesTest(unittest.TestCase):

    def test_batch_round_trip(self):
        originals = [
            article(),
            article(date_created=None, image=None),
            article(name=None, url=None, in_language=None, abstract=None, sections=None),
            article(name="Château d'Amboise", abstract="L'homme était là ✓", sections="",
                    in_language={"identifier": "fr"}),
        ]
        self.assertEqual(articles.decode_batch(external.deserialize(batch_of(*originals))), originals)

    def test_keeps_only_the_needed_columns_by_name(self):
        columns = ["references", "image", "sections", "abstract", "date_modified", "date_created",
                   "in_language", "url", "name", "tables"]
        a = article()
        row = ["muchas"] + list(reversed(list(a))) + ["tablas"]
        value = external.deserialize(articles.encode_batch(columns, [articles.encode_row(row)]))
        self.assertEqual(articles.decode_batch(value), [a])

    def test_missing_needed_columns(self):
        value = external.deserialize(articles.encode_batch(["name", "url"], [articles.encode_row(["a", "b"])]))
        with self.assertRaises(external.ProtocolError):
            articles.decode_batch(value)

    def test_empty_batch(self):
        self.assertEqual(articles.decode_batch(external.deserialize(batch_of())), [])

    def test_invalid_batches(self):
        columns = list(Article._fields)
        for value in ([["a"]], {"columns": columns}, {"columns": columns, "rows": [["corta"]]},
                      {"columns": columns, "rows": ["no es lista"]}):
            with self.assertRaises(external.ProtocolError):
                articles.decode_batch(value)


class ExternalTest(unittest.TestCase):

    def setUp(self):
        self.a, self.b = socket.socketpair()

    def tearDown(self):
        self.a.close()
        self.b.close()

    def test_messages_arrive_in_order(self):
        value = batch_of(article())
        external.send_message(self.a, external.MsgType.ARTICLES, value)
        external.send_message(self.a, external.MsgType.END_OF_DATA)
        self.assertEqual(external.receive_message(self.b), (external.MsgType.ARTICLES, external.deserialize(value)))
        self.assertEqual(external.receive_message(self.b), (external.MsgType.END_OF_DATA, None))

    def test_message_split_in_small_pieces(self):
        sock = ShortSocket(chunk=7)
        value = batch_of(*[article()] * 20)
        external.send_message(sock, external.MsgType.ARTICLES, value)
        external.send_message(sock, external.MsgType.END_OF_DATA)
        msg_type, batch = external.receive_message(sock)
        self.assertEqual(msg_type, external.MsgType.ARTICLES)
        self.assertEqual(articles.decode_batch(batch), [article()] * 20)
        self.assertEqual(external.receive_message(sock), (external.MsgType.END_OF_DATA, None))

    def test_closed_connection(self):
        self.a.close()
        with self.assertRaises(ConnectionError):
            external.receive_message(self.b)

    def test_rejects_huge_value(self):
        self.a.sendall(external.HEADER.pack(external.MsgType.ARTICLES, external.MAX_VALUE_SIZE + 1))
        with self.assertRaises(external.ProtocolError):
            external.receive_message(self.b)

    def test_rejects_unknown_type(self):
        self.a.sendall(external.HEADER.pack(99, 0))
        with self.assertRaises(external.ProtocolError):
            external.receive_message(self.b)


if __name__ == "__main__":
    unittest.main()
