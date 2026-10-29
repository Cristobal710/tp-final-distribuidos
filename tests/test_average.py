import unittest
from unittest.mock import patch

from average import Average
from common.protocol import external, internal
from tests.q2_fakes import ConsumerQueue, OutputQueue


class AverageTest(unittest.TestCase):

    def setUp(self):
        self.input = ConsumerQueue()
        self.outputs = [OutputQueue(), OutputQueue()]
        self.worker = Average(0, self.input, self.outputs, 2)

    def stats(self, words, articles, client="c1", gateway=3):
        self.input.deliver(self.worker, internal.encode_partial_average(
            client, gateway, internal.PartialAverage(words, articles),
        ))

    def eof(self, origin, processed=1, total=2, sent=1, client="c1", gateway=3):
        self.input.deliver(self.worker, internal.encode_eof(client, gateway, origin, processed, total, sent))

    def test_uses_weighted_average_and_broadcasts_exact_totals(self):
        self.stats(10, 1)
        self.stats(18, 9)
        self.eof("text_processor_0")
        self.assertTrue(all(not output.messages for output in self.outputs))
        self.eof("text_processor_1")
        self.assertEqual(self.outputs[0].messages, self.outputs[1].messages)
        [value] = self.outputs[0].decoded(internal.MsgType.FINAL_AVERAGE)
        self.assertEqual(value["average"], internal.FinalAverage(28, 10))
        self.assertEqual((value["client_id"], value["gateway_id"]), ("c1", 3))
        self.assertNotIn("c1", self.worker._totals)
        self.assertFalse(self.worker._upstream.is_complete("c1"))

    def test_waits_for_advertised_statistics_after_eofs(self):
        self.eof("text_processor_0")
        self.eof("text_processor_1")
        self.stats(2, 1)
        self.assertEqual(self.outputs[0].messages, [])
        self.stats(4, 1)
        self.assertEqual(self.outputs[0].decoded()[0]["average"], internal.FinalAverage(6, 2))

    def test_waits_for_late_eof_to_complete_processed_total(self):
        self.stats(5, 2)
        self.eof("text_processor_0", processed=0, total=1, sent=0)
        self.eof("text_processor_1", processed=0, total=1, sent=0)
        self.assertEqual(self.outputs[0].messages, [])
        self.input.deliver(self.worker, internal.encode_late_eof("c1", 3, "text_processor_0", 1, 1))
        self.assertEqual(self.outputs[0].decoded()[0]["average"], internal.FinalAverage(5, 2))

    def test_no_historical_articles_still_broadcasts_empty_baseline(self):
        self.eof("text_processor_0", processed=0, total=0, sent=0)
        self.eof("text_processor_1", processed=0, total=0, sent=0)
        for output in self.outputs:
            self.assertEqual(output.decoded()[0]["average"], internal.FinalAverage(0, 0))

    def test_zero_words_preserves_positive_article_count(self):
        self.stats(0, 4)
        self.eof("text_processor_0", processed=1, total=1, sent=1)
        self.eof("text_processor_1", processed=0, total=1, sent=0)
        self.assertEqual(self.outputs[0].decoded()[0]["average"], internal.FinalAverage(0, 4))

    def test_clients_and_gateways_are_isolated(self):
        for client, gateway, words in (("a", 0, 5), ("b", 7, 10)):
            self.stats(words, 2, client, gateway)
            self.eof("text_processor_0", 1, 1, 1, client, gateway)
            self.eof("text_processor_1", 0, 1, 0, client, gateway)
        self.assertEqual([(v["client_id"], v["gateway_id"], v["average"]) for v in self.outputs[0].decoded()],
                         [("a", 0, internal.FinalAverage(5, 2)), ("b", 7, internal.FinalAverage(10, 2))])

    def test_recent_completed_client_is_not_broadcast_again(self):
        for _ in range(2):
            self.eof("text_processor_0", 0, 0, 0)
            self.eof("text_processor_1", 0, 0, 0)
        self.assertEqual(len(self.outputs[0].messages), 1)

    def test_broadcast_failure_keeps_state_and_does_not_ack_trigger(self):
        self.stats(5, 1)
        self.eof("text_processor_0", 1, 1, 1)
        before = self.input.acks
        with patch.object(self.outputs[1], "send", side_effect=OSError("broker unavailable")):
            with self.assertRaises(OSError):
                self.eof("text_processor_1", 0, 1, 0)
        self.assertEqual(self.input.acks, before)
        self.assertIn("c1", self.worker._totals)
        self.assertNotIn("c1", self.worker._finished)

    def test_invalid_statistics_are_discarded_without_changing_totals(self):
        message = external.serialize({"type": internal.MsgType.PARTIAL_AVERAGE, "client_id": "c1",
                                      "gateway_id": 3, "rows": [[-1, 2]]})
        self.input.deliver(self.worker, message)
        self.assertEqual(self.input.acks, 1)
        self.assertEqual(self.worker._totals, {})

    def test_stop_and_close(self):
        self.worker.stop()
        self.worker.close()
        self.assertTrue(self.input.stopped)
        self.assertTrue(all(output.closed for output in self.outputs))
