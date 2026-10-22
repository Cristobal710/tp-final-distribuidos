import os
import tempfile
import unittest

from common.protocol import articles, internal
from common.protocol.internal import MsgType
from common.storage import ClientFiles
from join_q3 import JoinQ3
from tests.q3q4_fakes import FakeInputQueue, FakeOutputQueue
from top20 import Top20


def shared_message(client_id, *words, gateway_id=0):
    rows = [articles.encode_row(internal.SharedWord(word)) for word in words]
    return internal.encode_serialized_rows(MsgType.SHARED_WORDS, client_id, gateway_id, rows)


def top_message(client_id, *tops, gateway_id=0):
    rows = [articles.encode_row(internal.WordCount(*top)) for top in tops]
    return internal.encode_serialized_rows(MsgType.TOP_WORDS, client_id, gateway_id, rows)


def eof_message(client_id, origin, received, sent, gateway_id=0):
    return internal.encode_eof(client_id, gateway_id, origin, received, received, sent)


class JoinQ3Test(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def run_join(self, *events, word_counters=2, max_rows=100):
        self.input = FakeInputQueue(events)
        self.results = {}

        def open_results_queue(gateway_id):
            self.results[gateway_id] = FakeOutputQueue()
            return self.results[gateway_id]

        self.join = JoinQ3(0, self.input, ClientFiles(self.directory.name, "join_q3_0"), open_results_queue,
                           word_counters, max_rows, 1_000_000)
        with self.assertLogs(level="INFO"):
            self.join.run()

    def words_of(self, gateway_id):
        return [row[0] for value in self.results[gateway_id].data() for row in value["rows"]]

    def test_publishes_the_words_of_every_counter_once_all_of_them_finished(self):
        self.run_join(
            shared_message("c1", "sol", "luna"),
            eof_message("c1", "word_counter_0", 1, 1),
            shared_message("c1", "estrella"),
            eof_message("c1", "word_counter_1", 1, 1),
        )
        self.assertEqual(sorted(self.words_of(0)), ["estrella", "luna", "sol"])
        for value in self.results[0].data():
            self.assertEqual((value["type"], value["query"], value["client_id"]), (int(MsgType.RESULTS), 3, "c1"))

    def test_publishes_nothing_while_a_counter_is_missing(self):
        self.run_join(shared_message("c1", "sol"), eof_message("c1", "word_counter_0", 1, 1))
        self.assertEqual(self.results, {})

    def test_waits_for_the_messages_that_a_counter_says_it_sent(self):
        self.run_join(
            eof_message("c1", "word_counter_0", 1, 2),
            eof_message("c1", "word_counter_1", 1, 0),
            shared_message("c1", "sol"),
        )
        self.assertEqual(self.results, {})

    def test_sends_the_eof_of_q3_after_the_results_with_the_message_count(self):
        self.run_join(
            shared_message("c1", "a", "b", "c"),
            eof_message("c1", "word_counter_0", 1, 1),
            eof_message("c1", "word_counter_1", 0, 0),
            max_rows=2,
        )
        kinds = [kind for kind, _ in self.results[0].sent]
        self.assertEqual(kinds, [MsgType.RESULTS, MsgType.RESULTS, MsgType.EOF])
        _, eof = self.results[0].sent[-1]
        self.assertEqual((eof["origin"], eof["query"], eof["sent"], eof["gateway_id"]), ("join_q3_0", 3, 2, 0))

    def test_without_shared_words_it_only_sends_the_eof(self):
        self.run_join(eof_message("c1", "word_counter_0", 0, 0, gateway_id=2),
                      eof_message("c1", "word_counter_1", 0, 0, gateway_id=2))
        [(msg_type, eof)] = self.results[2].sent
        self.assertEqual((msg_type, eof["sent"], eof["query"]), (MsgType.EOF, 0, 3))

    def test_results_go_to_the_queue_of_the_gateway_of_each_client_and_files_are_deleted(self):
        self.run_join(
            shared_message("c1", "sol", gateway_id=0), shared_message("c2", "luna", gateway_id=1),
            eof_message("c1", "word_counter_0", 1, 1), eof_message("c1", "word_counter_1", 0, 0),
            eof_message("c2", "word_counter_0", 1, 1, gateway_id=1), eof_message("c2", "word_counter_1", 0, 0, gateway_id=1),
        )
        self.assertEqual(self.words_of(0), ["sol"])
        self.assertEqual(self.words_of(1), ["luna"])
        self.assertEqual(os.listdir(self.directory.name), [])


class Top20Test(unittest.TestCase):

    def run_top(self, *events, word_counters=2, top_size=3):
        self.input = FakeInputQueue(events)
        self.results = {}

        def open_results_queue(gateway_id):
            self.results[gateway_id] = FakeOutputQueue()
            return self.results[gateway_id]

        self.top = Top20(0, self.input, open_results_queue, word_counters, top_size)
        with self.assertLogs(level="INFO"):
            self.top.run()

    def rows_of(self, gateway_id):
        return [tuple(row) for value in self.results[gateway_id].data() for row in value["rows"]]

    def test_picks_the_words_that_appear_in_more_articles_and_breaks_ties_alphabetically(self):
        self.run_top(
            top_message("c1", ("zeta", 5), ("beta", 5), ("omega", 2)),
            eof_message("c1", "word_counter_0", 1, 1),
            top_message("c1", ("alfa", 5), ("gamma", 9), ("delta", 1)),
            eof_message("c1", "word_counter_1", 1, 1),
        )
        self.assertEqual(self.rows_of(0), [("gamma", 9), ("alfa", 5), ("beta", 5)])

    def test_publishes_nothing_while_a_counter_is_missing(self):
        self.run_top(top_message("c1", ("sol", 1)), eof_message("c1", "word_counter_0", 1, 1))
        self.assertEqual(self.results, {})

    def test_sends_one_results_message_with_query_4_followed_by_its_eof(self):
        self.run_top(
            top_message("c1", ("sol", 3)), eof_message("c1", "word_counter_0", 1, 1),
            eof_message("c1", "word_counter_1", 0, 0),
        )
        [(first, result), (second, eof)] = self.results[0].sent
        self.assertEqual((first, result["query"]), (MsgType.RESULTS, 4))
        self.assertEqual((second, eof["origin"], eof["query"], eof["sent"]), (MsgType.EOF, "top20_0", 4, 1))

    def test_without_candidates_it_only_sends_the_eof(self):
        self.run_top(eof_message("c1", "word_counter_0", 0, 0), eof_message("c1", "word_counter_1", 0, 0))
        [(msg_type, eof)] = self.results[0].sent
        self.assertEqual((msg_type, eof["sent"], eof["query"]), (MsgType.EOF, 0, 4))

    def test_clients_do_not_mix(self):
        self.run_top(
            top_message("c1", ("sol", 3)), top_message("c2", ("luna", 4), gateway_id=1),
            eof_message("c1", "word_counter_0", 1, 1), eof_message("c1", "word_counter_1", 0, 0),
            eof_message("c2", "word_counter_0", 1, 1, gateway_id=1), eof_message("c2", "word_counter_1", 0, 0, gateway_id=1),
        )
        self.assertEqual(self.rows_of(0), [("sol", 3)])
        self.assertEqual(self.rows_of(1), [("luna", 4)])


if __name__ == "__main__":
    unittest.main()
