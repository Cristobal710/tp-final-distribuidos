import os
import tempfile
import unittest

from common import partition
from common.batching import chunk_rows
from common.eof import UpstreamTracker
from common.protocol import articles, external, internal
from common.publisher import ReconnectingPublisher
from common.storage import ClientFiles


def eof(client_id, origin, processed, total, sent):
    return external.deserialize(internal.encode_eof(client_id, 0, origin, processed, total, sent))


def late_eof(client_id, origin, processed, sent):
    return external.deserialize(internal.encode_late_eof(client_id, 0, origin, processed, sent))


class PartitionTest(unittest.TestCase):

    def test_same_key_always_goes_to_the_same_shard(self):
        self.assertEqual(partition.shard_of("hola", 5), partition.shard_of("hola", 5))

    def test_shard_is_stable_across_processes(self):
        # El valor esta fijo para detectar si el hash dejara de ser el mismo en todos los procesos.
        self.assertEqual(partition.shard_of("wikipedia", 7), 4)

    def test_shard_is_inside_the_range_and_uses_all_shards(self):
        shards = {partition.shard_of(f"palabra{i}", 4) for i in range(200)}
        self.assertEqual(shards, {0, 1, 2, 3})


class ClientFilesTest(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.files = ClientFiles(os.path.join(self.directory.name, "datos"), "nodo")

    def test_rows_come_back_in_order_and_per_client(self):
        self.files.append("c1", [external.serialize(["a", 1]), external.serialize(["b", 2])])
        self.files.append("c2", [external.serialize(["z", 9])])
        self.files.append("c1", [external.serialize(["c", 3])])
        self.assertEqual(list(self.files.read("c1")), [["a", 1], ["b", 2], ["c", 3]])
        self.assertEqual(list(self.files.read("c2")), [["z", 9]])

    def test_rows_with_new_lines_and_accents_survive(self):
        self.files.append("c1", [external.serialize(["a\nb", "café"])])
        self.assertEqual(list(self.files.read("c1")), [["a\nb", "café"]])

    def test_raw_rows_are_the_serialized_ones(self):
        row = external.serialize(["a", 1])
        self.files.append("c1", [row])
        self.assertEqual(list(self.files.read_raw("c1")), [row])

    def test_missing_client_is_empty_and_delete_removes_the_file(self):
        self.assertEqual(list(self.files.read("nadie")), [])
        self.files.append("c1", [external.serialize([1])])
        self.files.delete("c1")
        self.files.delete("c1")
        self.assertEqual(list(self.files.read("c1")), [])
        self.assertEqual(os.listdir(os.path.join(self.directory.name, "datos")), [])


class ChunkRowsTest(unittest.TestCase):

    def test_chunks_respect_the_row_and_byte_limits_and_keep_every_row(self):
        rows = [articles.encode_row([f"palabra{i}", i]) for i in range(25)]
        chunks = list(chunk_rows(rows, 10, 10_000))
        self.assertEqual([len(chunk) for chunk in chunks], [10, 10, 5])
        self.assertEqual([row for chunk in chunks for row in chunk], rows)
        by_bytes = list(chunk_rows(rows, 100, 60))
        self.assertTrue(all(sum(len(row) for row in chunk) <= 60 for chunk in by_bytes))
        self.assertEqual([row for chunk in by_bytes for row in chunk], rows)

    def test_no_rows_means_no_chunks(self):
        self.assertEqual(list(chunk_rows([], 10, 100)), [])


class UpstreamTrackerTest(unittest.TestCase):

    def test_needs_the_eof_of_every_origin(self):
        tracker = UpstreamTracker(2)
        tracker.add_eof(eof("c1", "a", 3, 5, 1))
        self.assertFalse(tracker.is_complete("c1"))
        tracker.add_eof(eof("c1", "b", 2, 5, 1))
        self.assertTrue(tracker.is_complete("c1"))
        self.assertEqual(tracker.total_sent("c1"), 2)

    def test_waits_until_everything_was_processed_with_late_eofs(self):
        tracker = UpstreamTracker(2)
        tracker.add_eof(eof("c1", "a", 3, 6, 1))
        tracker.add_eof(eof("c1", "b", 2, 6, 1))
        self.assertFalse(tracker.is_complete("c1"))
        tracker.add_late_eof(late_eof("c1", "b", 1, 4))
        self.assertTrue(tracker.is_complete("c1"))
        self.assertEqual(tracker.total_sent("c1"), 6)

    def test_late_eof_can_arrive_before_the_eof_of_its_origin(self):
        tracker = UpstreamTracker(1)
        tracker.add_late_eof(late_eof("c1", "a", 1, 2))
        self.assertFalse(tracker.is_complete("c1"))
        tracker.add_eof(eof("c1", "a", 2, 3, 1))
        self.assertTrue(tracker.is_complete("c1"))

    def test_without_total_check_only_origins_and_received_messages_matter(self):
        tracker = UpstreamTracker(1, check_total=False, check_received=True)
        tracker.add_eof(eof("c1", "a", 7, 7, 2))
        self.assertFalse(tracker.is_complete("c1"))
        tracker.add_message("c1")
        self.assertFalse(tracker.is_complete("c1"))
        tracker.add_message("c1")
        self.assertTrue(tracker.is_complete("c1"))
        self.assertEqual(tracker.received("c1"), 2)

    def test_messages_before_the_eof_are_counted(self):
        tracker = UpstreamTracker(1, check_total=False, check_received=True)
        tracker.add_message("c1")
        tracker.add_eof(eof("c1", "a", 1, 1, 1))
        self.assertTrue(tracker.is_complete("c1"))

    def test_clients_are_independent_and_can_be_forgotten(self):
        tracker = UpstreamTracker(1)
        tracker.add_eof(eof("c1", "a", 1, 1, 1))
        self.assertTrue(tracker.is_complete("c1"))
        self.assertFalse(tracker.is_complete("c2"))
        tracker.forget("c1")
        self.assertFalse(tracker.is_complete("c1"))
        self.assertEqual(tracker.received("c1"), 0)

    def test_client_with_no_data_is_complete_with_zeros(self):
        tracker = UpstreamTracker(2)
        tracker.add_eof(eof("c1", "a", 0, 0, 0))
        tracker.add_eof(eof("c1", "b", 0, 0, 0))
        self.assertTrue(tracker.is_complete("c1"))


class FakePublisher:

    def __init__(self, fail_times=0):
        self.sent = []
        self.closed = False
        self.fail_times = fail_times

    def send(self, message):
        if self.fail_times:
            self.fail_times -= 1
            raise ConnectionError("conexion cerrada")
        self.sent.append(message)

    def close(self):
        self.closed = True


class ReconnectingPublisherTest(unittest.TestCase):

    def setUp(self):
        self.now = 0
        self.opened = []

    def open_publisher(self):
        publisher = FakePublisher(self.fail_next)
        self.fail_next = 0
        self.opened.append(publisher)
        return publisher

    fail_next = 0

    def make(self):
        return ReconnectingPublisher(self.open_publisher, 30, lambda: self.now)

    def test_opens_lazily_and_reuses_the_connection(self):
        publisher = self.make()
        self.assertEqual(self.opened, [])
        publisher.send(b"1")
        self.now = 10
        publisher.send(b"2")
        self.assertEqual(len(self.opened), 1)
        self.assertEqual(self.opened[0].sent, [b"1", b"2"])

    def test_renews_the_connection_after_being_idle(self):
        publisher = self.make()
        publisher.send(b"1")
        self.now = 31
        publisher.send(b"2")
        self.assertEqual(len(self.opened), 2)
        self.assertTrue(self.opened[0].closed)
        self.assertEqual(self.opened[1].sent, [b"2"])

    def test_retries_once_with_a_new_connection_if_sending_fails(self):
        publisher = self.make()
        publisher.send(b"1")
        self.opened[0].fail_times = 1
        publisher.send(b"2")
        self.assertEqual(len(self.opened), 2)
        self.assertEqual(self.opened[1].sent, [b"2"])

    def test_gives_up_if_the_retry_fails_too(self):
        self.fail_next = 1
        publisher = ReconnectingPublisher(lambda: FakePublisher(1), 30, lambda: self.now)
        with self.assertRaises(ConnectionError):
            publisher.send(b"1")

    def test_close_is_idempotent(self):
        publisher = self.make()
        publisher.send(b"1")
        publisher.close()
        publisher.close()
        self.assertTrue(self.opened[0].closed)


class WordsProtocolTest(unittest.TestCase):

    def test_words_round_trip(self):
        rows = [articles.encode_row(internal.Word("hola", 3, True, False))]
        msg_type, value = internal.decode(internal.encode_serialized_rows(internal.MsgType.WORDS, "c1", 2, rows))
        self.assertEqual(msg_type, internal.MsgType.WORDS)
        self.assertEqual((value["client_id"], value["gateway_id"]), ("c1", 2))
        self.assertEqual(value["words"], [internal.Word("hola", 3, True, False)])

    def test_shared_words_and_top_words_round_trip(self):
        shared = [articles.encode_row(internal.SharedWord("casa"))]
        top = [articles.encode_row(internal.WordCount("casa", 9))]
        _, value = internal.decode(internal.encode_serialized_rows(internal.MsgType.SHARED_WORDS, "c1", 0, shared))
        self.assertEqual(value["shared_words"], [internal.SharedWord("casa")])
        _, value = internal.decode(internal.encode_serialized_rows(internal.MsgType.TOP_WORDS, "c1", 0, top))
        self.assertEqual(value["top_words"], [internal.WordCount("casa", 9)])

    def test_words_without_gateway_id_are_invalid(self):
        message = external.serialize({"type": int(internal.MsgType.WORDS), "client_id": "c1", "rows": []})
        with self.assertRaises(external.ProtocolError):
            internal.decode(message)

    def test_word_row_with_wrong_size_is_invalid(self):
        message = internal.encode_serialized_rows(internal.MsgType.WORDS, "c1", 0, [external.serialize(["a", 1])])
        with self.assertRaises(external.ProtocolError):
            internal.decode(message)

    def test_serialized_results_have_the_same_shape_as_results(self):
        rows = [articles.encode_row(internal.WordCount("casa", 9))]
        message = internal.encode_serialized_rows(
            internal.MsgType.RESULTS, "c1", 0, rows, {"query": external.serialize(4)}
        )
        self.assertEqual(
            external.deserialize(message),
            external.deserialize(internal.encode_results("c1", 0, 4, [internal.WordCount("casa", 9)])),
        )


if __name__ == "__main__":
    unittest.main()
