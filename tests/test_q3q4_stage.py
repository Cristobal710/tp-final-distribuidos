import logging
import tempfile
import unittest

from common import partition
from common.eof import UpstreamTracker
from common.protocol import articles, internal
from common.protocol.internal import MsgType
from common.storage import ClientFiles
from q3q4.stage import Stage
from tests.q3q4_fakes import FakeInputQueue
from word_buckets import WordBuckets


def shared_message(client_id, *words, gateway_id=0):
    rows = [articles.encode_row(internal.SharedWord(word)) for word in words]
    return internal.encode_serialized_rows(MsgType.SHARED_WORDS, client_id, gateway_id, rows)


def eof_message(client_id, origin, received, gateway_id=0):
    return internal.encode_eof(client_id, gateway_id, origin, received, received, received)


# Nodo de prueba que anota lo que le piden hacer, para ver el recorrido de Stage.
class RecordingStage(Stage):

    def __init__(self, input_queue, origins=1):
        upstream = UpstreamTracker(origins, check_total=False, check_received=True)
        super().__init__("Nodo de prueba", input_queue, upstream, MsgType.SHARED_WORDS)
        self.data = []
        self.finished = []
        self.closed = False

    def close(self):
        self.closed = True

    def _on_data(self, client_id, value):
        self.data.append((client_id, [row.word for row in value["shared_words"]]))

    def _finish_client(self, client_id, gateway_id, received):
        self.finished.append((client_id, gateway_id, received))


class StageTest(unittest.TestCase):

    def run_stage(self, messages, origins=1):
        queue = FakeInputQueue(messages)
        stage = RecordingStage(queue, origins)
        with self.assertLogs(level="INFO") as logs:
            stage.run()
        self.logs = logs.output
        return stage, queue

    def test_data_goes_to_the_node_and_every_message_is_acked(self):
        stage, queue = self.run_stage([shared_message("c1", "sol", "luna"), shared_message("c1", "mar")])
        self.assertEqual(stage.data, [("c1", ["sol", "luna"]), ("c1", ["mar"])])
        self.assertEqual(queue.acks, 2)
        self.assertEqual(stage.finished, [])

    def test_finishes_the_client_when_the_previous_stage_says_it_is_done(self):
        stage, queue = self.run_stage([
            shared_message("c1", "sol", gateway_id=3), shared_message("c1", "mar", gateway_id=3),
            eof_message("c1", "word_counter_0", 2, gateway_id=3),
        ])
        self.assertEqual(stage.finished, [("c1", 3, 2)])
        self.assertEqual(queue.acks, 3)

    def test_it_does_not_finish_while_data_is_missing(self):
        stage, _ = self.run_stage([shared_message("c1", "sol"), eof_message("c1", "word_counter_0", 2)])
        self.assertEqual(stage.finished, [])

    def test_waits_for_the_eof_of_every_origin(self):
        stage, _ = self.run_stage([
            shared_message("c1", "sol"), eof_message("c1", "word_counter_0", 1),
            shared_message("c1", "mar"), eof_message("c1", "word_counter_1", 1),
        ], origins=2)
        self.assertEqual(stage.finished, [("c1", 0, 2)])

    def test_each_client_is_finished_on_its_own(self):
        stage, _ = self.run_stage([
            shared_message("c1", "sol"), shared_message("c2", "mar"), eof_message("c2", "word_counter_0", 1),
        ])
        self.assertEqual([client for client, _, _ in stage.finished], ["c2"])

    def test_forgets_the_client_after_finishing_it(self):
        stage, _ = self.run_stage([shared_message("c1", "sol"), eof_message("c1", "word_counter_0", 1)])
        self.assertEqual(stage._upstream.received("c1"), 0)

    def test_an_invalid_message_is_discarded_and_acked(self):
        stage, queue = self.run_stage([b"basura", shared_message("c1", "sol")])
        self.assertEqual(stage.data, [("c1", ["sol"])])
        self.assertEqual(queue.acks, 2)
        self.assertTrue(any(line.startswith("ERROR") and "Mensaje inválido" in line for line in self.logs))

    def test_a_message_of_another_type_is_ignored_with_a_warning_and_acked(self):
        words = internal.encode_serialized_rows(MsgType.WORDS, "c1", 0, [])
        stage, queue = self.run_stage([words])
        self.assertEqual(stage.data, [])
        self.assertEqual(queue.acks, 1)
        self.assertTrue(any(line.startswith("WARNING") and "WORDS ignorado" in line for line in self.logs))

    def test_stop_stops_the_input_queue(self):
        queue = FakeInputQueue()
        RecordingStage(queue).stop()
        self.assertTrue(queue.stopped)

    def test_the_node_has_to_say_what_to_do_with_data_and_how_to_finish(self):
        queue = FakeInputQueue([shared_message("c1", "sol")])
        stage = Stage("sin hacer", queue, UpstreamTracker(1), MsgType.SHARED_WORDS)
        with self.assertRaises(NotImplementedError), self.assertLogs(level="INFO"):
            stage.run()

    def test_close_does_nothing_by_default(self):
        Stage("x", FakeInputQueue(), UpstreamTracker(1), MsgType.WORDS).close()


class WordBucketsTest(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.files = [ClientFiles(self.directory.name, f"shard_{bucket}") for bucket in range(4)]
        self.buckets = WordBuckets(self.files)

    def tearDown(self):
        self.directory.cleanup()

    def test_every_word_is_stored_in_the_file_of_its_bucket(self):
        words = ["sol", "luna", "mar", "aire", "fuego", "tierra", "agua", "viento"]
        self.buckets.append("c1", [[word, 1, True, False] for word in words])
        for index, file in enumerate(self.files):
            stored = {row[0] for row in file.read("c1")}
            self.assertEqual(stored, {word for word in words if partition.bucket_of(word, 4) == index})

    def test_counts_per_file_adds_what_arrived_in_different_messages(self):
        self.buckets.append("c1", [["sol", 1, True, False]])
        self.buckets.append("c1", [["sol", 2, False, True], ["luna", 1, True, False]])
        merged = {}
        for counts in self.buckets.counts_per_file("c1"):
            merged.update(counts)
        self.assertEqual(merged, {"sol": [3, True, True], "luna": [1, True, False]})

    def test_counts_per_file_gives_one_item_per_file_even_if_it_is_empty(self):
        self.assertEqual(list(self.buckets.counts_per_file("c1")), [{}, {}, {}, {}])

    def test_the_files_of_each_client_are_kept_apart_and_deleted_together(self):
        self.buckets.append("c1", [["sol", 1, True, False]])
        self.buckets.append("c2", [["mar", 1, True, False]])
        self.buckets.delete("c1")
        self.assertEqual(sum(len(counts) for counts in self.buckets.counts_per_file("c1")), 0)
        self.assertEqual(sum(len(counts) for counts in self.buckets.counts_per_file("c2")), 1)


if __name__ == "__main__":
    unittest.main()
