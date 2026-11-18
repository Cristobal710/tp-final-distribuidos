import unittest

from common.protocol import articles, internal
from common.protocol.internal import MsgType
from q3q4.ranking import best_words, rank_key
from q3q4.results_publisher import ResultsPublisher
from q3q4.row_sender import RowSender
from shard_summary import ShardSummary, total_counts
from tests.q3q4_fakes import FakeOutputQueue


def serialized_rows(*words):
    return [articles.encode_row(internal.SharedWord(word)) for word in words]


class RankingTest(unittest.TestCase):

    def test_more_articles_first_and_ties_break_alphabetically(self):
        candidates = [("luna", 2), ("sol", 5), ("mar", 2), ("aire", 5)]
        self.assertEqual(best_words(candidates, 4), [("aire", 5), ("sol", 5), ("luna", 2), ("mar", 2)])

    def test_keeps_only_the_best_ones(self):
        candidates = [("a", 1), ("b", 3), ("c", 2)]
        self.assertEqual(best_words(candidates, 2), [("b", 3), ("c", 2)])

    def test_works_with_word_count_rows_and_gives_back_the_same_rows(self):
        rows = [internal.WordCount("luna", 2), internal.WordCount("sol", 5)]
        self.assertEqual(best_words(rows, 1), [rows[1]])

    def test_size_zero_or_no_candidates_give_nothing(self):
        self.assertEqual(best_words([("a", 1)], 0), [])
        self.assertEqual(best_words([], 3), [])

    def test_rank_key_is_the_one_used_to_order(self):
        self.assertLess(rank_key(("zeta", 9)), rank_key(("alfa", 8)))
        self.assertLess(rank_key(("alfa", 8)), rank_key(("beta", 8)))


class ShardSummaryTest(unittest.TestCase):

    def test_total_counts_adds_articles_and_joins_the_languages(self):
        counts = total_counts([("sol", 1, True, False), ("sol", 2, False, True), ("luna", 1, True, False)])
        self.assertEqual(counts, {"sol": [3, True, True], "luna": [1, True, False]})

    def test_gives_the_shared_words_of_every_file_and_the_top_over_all_of_them(self):
        first = {"sol": [4, True, True], "luna": [1, True, False]}
        second = {"mar": [3, False, True], "aire": [3, True, True]}
        summary = ShardSummary(top_size=2)
        self.assertEqual(list(summary.shared_words(iter([first, second]))), ["sol", "aire"])
        self.assertEqual(summary.top, [("sol", 4), ("aire", 3)])

    def test_the_top_is_complete_only_after_going_through_every_file(self):
        summary = ShardSummary(top_size=3)
        words = summary.shared_words(iter([{"sol": [1, True, True]}, {"mar": [5, True, True]}]))
        self.assertEqual(next(words), "sol")
        self.assertEqual(summary.top, [])
        self.assertEqual(list(words), ["mar"])
        self.assertEqual(summary.top, [("mar", 5), ("sol", 1)])

    def test_no_files_means_no_words(self):
        summary = ShardSummary(top_size=3)
        self.assertEqual(list(summary.shared_words(iter([]))), [])
        self.assertEqual(summary.top, [])


class RowSenderTest(unittest.TestCase):

    def test_splits_the_rows_and_closes_with_an_eof_that_counts_the_messages(self):
        queue = FakeOutputQueue()
        sent = RowSender("word_counter_1", 2, 1_000_000).send(
            queue, MsgType.SHARED_WORDS, "c1", 3, serialized_rows("a", "b", "c"), 7)
        self.assertEqual(sent, 2)
        self.assertEqual([[row.word for row in value["shared_words"]] for value in queue.data()], [["a", "b"], ["c"]])
        kind, eof = queue.sent[-1]
        self.assertEqual(kind, MsgType.EOF)
        self.assertEqual((eof["origin"], eof["processed"], eof["total"], eof["sent"], eof["gateway_id"]),
                         ("word_counter_1", 7, 7, 2, 3))
        self.assertNotIn("query", eof)

    def test_without_rows_it_only_sends_the_eof(self):
        queue = FakeOutputQueue()
        self.assertEqual(RowSender("x", 2, 100).send(queue, MsgType.SHARED_WORDS, "c1", 0, [], 4), 0)
        self.assertEqual([kind for kind, _ in queue.sent], [MsgType.EOF])
        self.assertEqual(queue.sent[0][1]["sent"], 0)

    def test_without_limits_all_the_rows_go_in_one_message(self):
        queue = FakeOutputQueue()
        sent = RowSender("x").send(queue, MsgType.SHARED_WORDS, "c1", 0, iter(serialized_rows("a", "b", "c")), 1)
        self.assertEqual(sent, 1)
        self.assertEqual(len(queue.data()[0]["shared_words"]), 3)

    def test_with_a_query_the_messages_and_the_eof_belong_to_it(self):
        queue = FakeOutputQueue()
        RowSender("join_q3_0", 10, 1_000_000, query=3).send(queue, MsgType.RESULTS, "c1", 0, serialized_rows("a"), 2)
        self.assertEqual(queue.data()[0]["query"], 3)
        self.assertEqual(queue.sent[-1][1]["query"], 3)


class ResultsPublisherTest(unittest.TestCase):

    def setUp(self):
        self.queues = {}
        self.opened = []

        def open_queue(gateway_id):
            self.opened.append(gateway_id)
            self.queues[gateway_id] = FakeOutputQueue()
            return self.queues[gateway_id]

        self.publisher = ResultsPublisher(open_queue, "top20_0", 4)

    def test_opens_the_queue_of_a_gateway_once_and_only_when_it_is_used(self):
        self.assertEqual(self.opened, [])
        self.publisher.publish("c1", 2, serialized_rows("a"), 1)
        self.publisher.publish("c2", 2, serialized_rows("b"), 1)
        self.assertEqual(self.opened, [2])

    def test_each_gateway_gets_its_own_results_and_eof_of_the_query(self):
        self.publisher.publish("c1", 0, serialized_rows("a"), 1)
        self.publisher.publish("c2", 1, [], 1)
        self.assertEqual([kind for kind, _ in self.queues[0].sent], [MsgType.RESULTS, MsgType.EOF])
        self.assertEqual([kind for kind, _ in self.queues[1].sent], [MsgType.EOF])
        self.assertEqual(self.queues[0].sent[-1][1]["query"], 4)

    def test_close_closes_every_queue_it_opened(self):
        self.publisher.publish("c1", 0, [], 0)
        self.publisher.publish("c1", 1, [], 0)
        self.publisher.close()
        self.assertTrue(all(queue.closed for queue in self.queues.values()))


if __name__ == "__main__":
    unittest.main()
