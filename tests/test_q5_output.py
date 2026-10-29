import unittest

from common import partition
from common.protocol import internal
from q5_output import Q5Output, distinct_word_count
from tests.q2_fakes import OutputQueue
from text_processor import ProcessedText


def text(url, period, words):
    return ProcessedText(url, period, "en", words)


class Q5OutputTest(unittest.TestCase):

    def setUp(self):
        self.averages = [OutputQueue(), OutputQueue()]
        self.comparators = [OutputQueue(), OutputQueue(), OutputQueue()]
        self.output = Q5Output("promedio", self.averages, "comparacion", self.comparators, 2, 1000)

    def test_counts_distinct_words_per_article_in_both_branches(self):
        sent = self.output.send_batch("c1", 7, [
            text("old", True, ["sun", "sun", "moon"]), text("empty", True, []),
            text("new", False, ["sky", "sky", "cloud"]), text("empty-new", False, []),
            text("unknown", None, ["ignored"]),
        ])
        shard = partition.shard_of("c1", 2)
        [stats] = self.averages[shard].decoded(internal.MsgType.PARTIAL_AVERAGE)
        self.assertEqual(stats["partial_average"], internal.PartialAverage(2, 2))
        self.assertEqual((stats["client_id"], stats["gateway_id"]), ("c1", 7))
        self.assertEqual(self.averages[1 - shard].messages, [])
        candidates = [row for queue in self.comparators for value in queue.decoded() for row in value["candidates"]]
        self.assertEqual(candidates, [internal.CandidateArticle("new", 2), internal.CandidateArticle("empty-new", 0)])
        self.assertEqual(sent, {f"promedio.{shard}": 1, "comparacion.0": 1})

    def test_distinct_count_does_not_deduplicate_across_articles(self):
        self.assertEqual(distinct_word_count(["a", "a", "b"]), 2)
        shard = partition.shard_of("c1", 2)
        self.output.send_batch("c1", 0, [text("a", True, ["same"]), text("b", True, ["same"])])
        self.assertEqual(self.averages[shard].decoded()[0]["partial_average"], internal.PartialAverage(2, 2))

    def test_unknown_period_and_empty_input_send_no_data(self):
        self.assertEqual(self.output.send_batch("c1", 0, [text("x", None, ["a"])]), {})
        self.assertEqual(self.output.send_batch("c1", 0, []), {})
        self.assertTrue(all(not queue.messages for queue in self.averages + self.comparators))

    def test_candidate_batches_are_distributed_round_robin(self):
        self.output._max_rows = 1
        sent = self.output.send_batch("c1", 0, [text(str(i), False, ["word"]) for i in range(7)])
        self.assertEqual([len(queue.messages) for queue in self.comparators], [3, 2, 2])
        self.assertEqual(sent, {"comparacion.0": 3, "comparacion.1": 2, "comparacion.2": 2})

    def test_full_candidate_message_limit_and_oversized_rows(self):
        limit = len(internal.encode_candidates("c1", 0, [internal.CandidateArticle("url", 1)]))
        self.output._max_bytes = limit
        self.output.send_batch("c1", 0, [text("url", False, ["a"]), text("X" * 1000, False, ["a"]),
                                         text("url", False, ["a"])])
        messages = [message for queue in self.comparators for message in queue.messages]
        self.assertEqual(len(messages), 2)
        self.assertTrue(all(len(message) <= limit for message in messages))

    def test_statistics_are_not_silently_dropped_when_limit_is_too_small(self):
        self.output._max_bytes = 1
        with self.assertRaises(ValueError):
            self.output.send_batch("c1", 0, [text("old", True, ["a"])])
        self.assertTrue(all(not queue.messages for queue in self.averages))

    def test_eofs_reach_selected_average_and_all_comparators_even_without_data(self):
        self.output.send_eofs("empty", {}, lambda count, query=None:
                              internal.encode_eof("empty", 4, "text_processor_0", 0, 0, count, query))
        shard = partition.shard_of("empty", 2)
        self.assertEqual(self.averages[1 - shard].messages, [])
        for queue in [self.averages[shard], *self.comparators]:
            [eof] = queue.decoded(internal.MsgType.EOF)
            self.assertEqual((eof["client_id"], eof["gateway_id"], eof["sent"]), ("empty", 4, 0))

    def test_completion_uses_actual_message_counts_and_supports_late_eof(self):
        sent = self.output.send_batch("c1", 0, [text("old", True, ["a"]), text("new", False, ["b"])])
        self.output.send_eofs("c1", sent, lambda count, query=None:
                              internal.encode_late_eof("c1", 0, "text_processor_0", 1, count, query))
        shard = partition.shard_of("c1", 2)
        self.assertEqual(self.averages[shard].decoded(internal.MsgType.LATE_EOF)[0]["sent"], 1)
        self.assertEqual([q.decoded(internal.MsgType.LATE_EOF)[0]["sent"] for q in self.comparators], [1, 0, 0])

    def test_close_releases_publishers(self):
        self.output.close()
        self.assertTrue(all(queue.closed for queue in self.averages + self.comparators))
