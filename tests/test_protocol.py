import socket
import unittest
from datetime import datetime, timezone

from common import safe_socket
from common.protocol import articles, external, internal
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


class InternalTest(unittest.TestCase):

    def test_texts_round_trip(self):
        texts = [internal.Text("https://fr.wikipedia.org/wiki/A", "Résumé", False, "fr")]
        msg_type, value = internal.decode(internal.encode_texts("c1", 3, texts))
        self.assertEqual(msg_type, internal.MsgType.TEXTS)
        self.assertEqual((value["client_id"], value["gateway_id"], value["texts"]), ("c1", 3, texts))

    def test_bios_round_trip(self):
        bios = [internal.Bio("Ada", "https://en.wikipedia.org/wiki/Ada", "[]", None)]
        msg_type, value = internal.decode(internal.encode_bios("c1", 0, bios))
        self.assertEqual(msg_type, internal.MsgType.BIOS)
        self.assertEqual(value["bios"], bios)

    def test_results_carry_query_and_rows(self):
        rows = [internal.Q1Result("https://en.wikipedia.org/wiki/Ada", 1009843200000)]
        msg_type, value = internal.decode(internal.encode_results("c1", 0, 1, rows))
        self.assertEqual(msg_type, internal.MsgType.RESULTS)
        self.assertEqual((value["query"], value["rows"]), (1, [["https://en.wikipedia.org/wiki/Ada", 1009843200000]]))

    def test_image_requests_round_trip(self):
        images = [internal.ImageRequest("Émilie", "https://wiki/Emilie", "https://images/emilie.jpg"),
                  internal.ImageRequest(None, None, None)]
        msg_type, value = internal.decode(internal.encode_images("c2", 7, images))
        self.assertEqual(msg_type, internal.MsgType.IMAGES)
        self.assertEqual((value["client_id"], value["gateway_id"], value["images"]), ("c2", 7, images))

    def test_image_requests_require_gateway_and_three_columns(self):
        for fields in ({"rows": [["Ada", "url", "image"]]}, {"gateway_id": 1, "rows": [["Ada", "url"]]}):
            with self.subTest(fields=fields), self.assertRaises(external.ProtocolError):
                internal.decode(external.serialize({"type": internal.MsgType.IMAGES, "client_id": "c1", **fields}))

    def test_q2_results_preserve_every_byte(self):
        image = bytes(range(256))
        row = internal.Q2Result("Ada", "https://wiki/Ada", list(image))
        msg_type, value = internal.decode(internal.encode_results("c1", 2, 2, [row]))
        self.assertEqual(msg_type, internal.MsgType.RESULTS)
        self.assertEqual((value["client_id"], value["gateway_id"], value["query"]), ("c1", 2, 2))
        self.assertEqual(bytes(value["rows"][0][2]), image)

    def test_q5_partial_and_final_average_round_trip(self):
        for encode, row_type, kind, key in (
            (internal.encode_partial_average, internal.PartialAverage, internal.MsgType.PARTIAL_AVERAGE, "partial_average"),
            (internal.encode_average, internal.FinalAverage, internal.MsgType.FINAL_AVERAGE, "average"),
        ):
            for row in (row_type(2 ** 60 + 1, 3), row_type(0, 0), row_type(0, 5)):
                with self.subTest(kind=kind, row=row):
                    msg_type, value = internal.decode(encode("c1", 7, row))
                    self.assertEqual((msg_type, value["client_id"], value["gateway_id"], value[key]), (kind, "c1", 7, row))

    def test_q5_candidates_round_trip_and_results_use_query_five(self):
        rows = [internal.CandidateArticle("https://wiki/A", 10), internal.CandidateArticle(None, 0)]
        kind, value = internal.decode(internal.encode_candidates("c1", 2, rows))
        self.assertEqual((kind, value["candidates"]), (internal.MsgType.CANDIDATES, rows))
        _, result = internal.decode(internal.encode_results("c1", 2, 5, [internal.Q5Result(*row) for row in rows]))
        self.assertEqual((result["query"], result["rows"]), (5, [list(row) for row in rows]))

    def test_q5_rejects_invalid_counts_and_statistics_shapes(self):
        for kind in (internal.MsgType.PARTIAL_AVERAGE, internal.MsgType.FINAL_AVERAGE):
            for rows in ([], [[1, 1], [2, 1]], [[1, 0]], [[-1, 1]], [[1, -1]], [[True, 1]], [[1.5, 1]], [[1, "2"]]):
                with self.subTest(kind=kind, rows=rows), self.assertRaises(external.ProtocolError):
                    internal.decode(external.serialize({"type": kind, "client_id": "c1", "gateway_id": 0, "rows": rows}))
        for row in (("url", -1), ("url", True), ("url", 1.5), ({}, 1)):
            with self.subTest(row=row), self.assertRaises(external.ProtocolError):
                internal.decode(internal.encode_candidates("c1", 0, [row]))

    def test_q5_messages_require_gateway_id(self):
        for kind in (internal.MsgType.PARTIAL_AVERAGE, internal.MsgType.CANDIDATES, internal.MsgType.FINAL_AVERAGE):
            with self.subTest(kind=kind), self.assertRaises(external.ProtocolError):
                internal.decode(external.serialize({"type": kind, "client_id": "c1", "rows": []}))

    def test_eof_with_query_round_trip(self):
        msg_type, value = internal.decode(internal.encode_eof("c1", 0, "classifier_1", 3, 10, 2, query=1))
        self.assertEqual(msg_type, internal.MsgType.EOF)
        self.assertEqual((value["processed"], value["total"], value["sent"], value["query"]), (3, 10, 2, 1))

    def test_late_eof_round_trip(self):
        msg_type, value = internal.decode(internal.encode_late_eof("c1", 0, "classifier_1", 1, 0))
        self.assertEqual(msg_type, internal.MsgType.LATE_EOF)
        self.assertEqual((value["origin"], value["processed"], value["sent"]), ("classifier_1", 1, 0))

    def test_rejects_eof_without_counts(self):
        message = external.serialize({"type": internal.MsgType.EOF, "client_id": "c1", "gateway_id": 0, "origin": "x"})
        with self.assertRaises(external.ProtocolError):
            internal.decode(message)

    def test_rejects_rows_with_wrong_length(self):
        message = external.serialize({"type": internal.MsgType.TEXTS, "client_id": "c1", "rows": [["solo url"]]})
        with self.assertRaises(external.ProtocolError):
            internal.decode(message)


if __name__ == "__main__":
    unittest.main()
