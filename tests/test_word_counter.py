import os
import tempfile
import unittest

from common import partition
from common.protocol import articles, internal
from common.protocol.internal import MsgType
from common.storage import ClientFiles
from tests.q3q4_fakes import FakeInputQueue, FakeOutputQueue
from word_counter import WordCounter


def words_message(client_id, *words, gateway_id=0):
    rows = [articles.encode_row(internal.Word(*word)) for word in words]
    return internal.encode_serialized_rows(MsgType.WORDS, client_id, gateway_id, rows)


def eof_message(client_id, origin, processed, total, sent, gateway_id=0):
    return internal.encode_eof(client_id, gateway_id, origin, processed, total, sent)


def late_eof_message(client_id, origin, processed, sent, gateway_id=0):
    return internal.encode_late_eof(client_id, gateway_id, origin, processed, sent)


class WordCounterTest(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def run_counter(self, *events, text_processors=2, joins=1, tops=1, top_size=3, max_rows=100, buckets=3):
        self.input = FakeInputQueue(events)
        self.joins = [FakeOutputQueue() for _ in range(joins)]
        self.tops = [FakeOutputQueue() for _ in range(tops)]
        self.buckets = [ClientFiles(self.directory.name, f"word_counter_1_{bucket}") for bucket in range(buckets)]
        self.counter = WordCounter(
            1, self.input, self.buckets, self.joins, self.tops, text_processors, top_size, max_rows, 1_000_000
        )
        with self.assertLogs(level="INFO"):
            self.counter.run()

    def shared_words(self, queue):
        return sorted(row.word for value in queue.data() for row in value["shared_words"])

    def top_words(self, queue):
        return [(row.word, row.articles) for value in queue.data() for row in value["top_words"]]

    def test_waits_for_every_text_processor_and_every_words_message(self):
        self.run_counter(
            words_message("c1", ("sol", 1, True, True)),
            eof_message("c1", "text_processor_0", 1, 2, 2),
            eof_message("c1", "text_processor_1", 1, 2, 0),
        )
        self.assertEqual(self.joins[0].sent, [])
        self.assertEqual(self.tops[0].sent, [])
        for name in os.listdir(self.directory.name):
            os.remove(os.path.join(self.directory.name, name))
        self.run_counter(
            words_message("c1", ("sol", 1, True, True)),
            eof_message("c1", "text_processor_0", 1, 2, 2),
            eof_message("c1", "text_processor_1", 1, 2, 0),
            words_message("c1", ("luna", 2, True, False)),
        )
        self.assertEqual(self.shared_words(self.joins[0]), ["sol"])
        self.assertEqual(self.top_words(self.tops[0]), [("luna", 2), ("sol", 1)])

    def test_waits_for_the_late_eof_of_a_text_processor(self):
        self.run_counter(
            eof_message("c1", "text_processor_0", 1, 2, 0),
            eof_message("c1", "text_processor_1", 0, 2, 0),
        )
        self.assertEqual(self.joins[0].sent, [])
        self.run_counter(
            eof_message("c1", "text_processor_0", 1, 2, 0),
            eof_message("c1", "text_processor_1", 0, 2, 0),
            late_eof_message("c1", "text_processor_1", 1, 1),
            words_message("c1", ("sol", 1, True, True)),
        )
        self.assertEqual(self.shared_words(self.joins[0]), ["sol"])

    def test_a_word_is_shared_when_it_shows_up_in_both_languages_even_in_different_messages(self):
        self.run_counter(
            words_message("c1", ("sol", 2, True, False), ("luna", 1, True, False)),
            words_message("c1", ("sol", 1, False, True), ("etoile", 3, False, True)),
            eof_message("c1", "text_processor_0", 2, 2, 2),
            eof_message("c1", "text_processor_1", 0, 2, 0),
        )
        self.assertEqual(self.shared_words(self.joins[0]), ["sol"])

    def test_top_counts_articles_of_both_languages_and_breaks_ties_alphabetically(self):
        self.run_counter(
            words_message("c1", ("zeta", 5, True, False), ("beta", 5, True, False), ("alfa", 5, False, True),
                          ("gamma", 9, True, True), ("delta", 1, True, False)),
            words_message("c1", ("beta", 2, False, True), ("epsilon", 4, True, False)),
            eof_message("c1", "text_processor_0", 2, 2, 2),
            eof_message("c1", "text_processor_1", 0, 2, 0),
            top_size=3,
        )
        self.assertEqual(self.top_words(self.tops[0]), [("gamma", 9), ("beta", 7), ("alfa", 5)])

    def test_sends_the_eof_after_the_data_with_what_it_received_and_sent(self):
        self.run_counter(
            words_message("c1", ("a", 1, True, True), ("b", 1, True, True), ("c", 1, True, True)),
            words_message("c1", ("d", 1, True, True)),
            eof_message("c1", "text_processor_0", 2, 2, 2),
            eof_message("c1", "text_processor_1", 0, 2, 0),
            max_rows=2,
        )
        self.assertEqual(self.shared_words(self.joins[0]), ["a", "b", "c", "d"])
        for queue, msg_type in ((self.joins[0], MsgType.SHARED_WORDS), (self.tops[0], MsgType.TOP_WORDS)):
            kinds = [kind for kind, _ in queue.sent]
            self.assertEqual(kinds[-1], MsgType.EOF)
            self.assertEqual(set(kinds[:-1]), {msg_type})
            _, eof = queue.sent[-1]
            self.assertEqual(eof["origin"], "word_counter_1")
            self.assertEqual((eof["processed"], eof["total"]), (2, 2))
            self.assertEqual(eof["sent"], len(queue.data()))
        self.assertEqual(len(self.joins[0].data()), 2)

    def test_a_shard_without_words_still_sends_empty_eofs(self):
        self.run_counter(
            eof_message("c1", "text_processor_0", 0, 0, 0, gateway_id=4),
            eof_message("c1", "text_processor_1", 0, 0, 0, gateway_id=4),
        )
        for queue in (self.joins[0], self.tops[0]):
            [(msg_type, eof)] = queue.sent
            self.assertEqual((msg_type, eof["sent"], eof["gateway_id"]), (MsgType.EOF, 0, 4))

    def test_each_client_goes_to_the_instance_that_its_hash_picks(self):
        clients = [f"cliente{i}" for i in range(12)]
        events = []
        for client in clients:
            events += [
                words_message(client, ("sol", 1, True, True)),
                eof_message(client, "text_processor_0", 1, 1, 1),
                eof_message(client, "text_processor_1", 0, 1, 0),
            ]
        self.run_counter(*events, joins=2, tops=3)
        for client in clients:
            for queues, amount in ((self.joins, 2), (self.tops, 3)):
                owners = [i for i, queue in enumerate(queues) if any(v["client_id"] == client for _, v in queue.sent)]
                self.assertEqual(owners, [partition.shard_of(client, amount)])

    def test_clients_do_not_mix_and_their_files_are_deleted(self):
        self.run_counter(
            words_message("c1", ("sol", 1, True, False)),
            words_message("c2", ("sol", 1, False, True), gateway_id=1),
            eof_message("c1", "text_processor_0", 1, 1, 1),
            eof_message("c1", "text_processor_1", 0, 1, 0),
            eof_message("c2", "text_processor_0", 1, 1, 1, gateway_id=1),
            eof_message("c2", "text_processor_1", 0, 1, 0, gateway_id=1),
        )
        self.assertEqual(self.shared_words(self.joins[0]), [])
        self.assertEqual([v["client_id"] for _, v in self.joins[0].eofs()], ["c1", "c2"])
        self.assertEqual(os.listdir(self.directory.name), [])

    def test_every_appearance_of_a_word_is_stored_in_the_file_of_its_bucket(self):
        words = [f"palabra{i}" for i in range(30)]
        self.run_counter(
            words_message("c1", *[(word, 1, True, False) for word in words]),
            words_message("c1", *[(word, 1, False, True) for word in words[:10]]),
            buckets=4,
        )
        for bucket, files in enumerate(self.buckets):
            stored = [row[0] for row in files.read("c1")]
            self.assertTrue(all(partition.bucket_of(word, 4) == bucket for word in stored))
        stored = sorted(row[0] for files in self.buckets for row in files.read("c1"))
        self.assertEqual(stored, sorted(words + words[:10]))

    def test_the_result_does_not_depend_on_the_amount_of_buckets(self):
        events = [
            words_message("c1", *[(f"palabra{i}", i % 7 + 1, i % 2 == 0, i % 3 == 0) for i in range(40)]),
            words_message("c1", *[(f"palabra{i}", 2, i % 5 == 0, True) for i in range(0, 40, 4)]),
            eof_message("c1", "text_processor_0", 2, 2, 2),
            eof_message("c1", "text_processor_1", 0, 2, 0),
        ]
        results = []
        for buckets in (1, 5, 16):
            for name in os.listdir(self.directory.name):
                os.remove(os.path.join(self.directory.name, name))
            self.run_counter(*events, top_size=5, buckets=buckets)
            results.append((self.shared_words(self.joins[0]), self.top_words(self.tops[0])))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], results[2])
        self.assertTrue(results[0][0])

    def test_invalid_messages_are_discarded_and_acked(self):
        self.run_counter(b"basura")
        self.assertEqual(self.input.acks, 1)


if __name__ == "__main__":
    unittest.main()
