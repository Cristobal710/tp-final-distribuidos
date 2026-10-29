import tempfile
import unittest
from unittest.mock import patch

from common.protocol import internal
from common.storage import ClientFiles
from comparator import Comparator, exceeds_average
from tests.q2_fakes import ConsumerQueue, OutputQueue, noop


class ComparatorTest(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.input, self.averages = ConsumerQueue(), ConsumerQueue()
        self.files = ClientFiles(self.directory.name, "comparator_0")
        self.results = {}

        def open_results(gateway_id):
            return self.results.setdefault(gateway_id, OutputQueue())

        self.worker = Comparator(0, self.input, self.averages, self.files, open_results, 2, 2, 1000, drain_rows=2)
        self.addCleanup(self.worker.close)

    def send(self, *counts, client="c1", gateway=3):
        rows = [internal.CandidateArticle(f"url-{i}-{count}", count) for i, count in enumerate(counts)]
        self.input.deliver(self.worker, internal.encode_candidates(client, gateway, rows))
        return rows

    def average(self, words, articles, client="c1", gateway=3, pump=True):
        self.worker._on_average_message(
            internal.encode_average(client, gateway, internal.FinalAverage(words, articles)), self.averages.ack, noop,
        )
        if pump:
            self.input.pump()
            self.averages.pump()

    def eof(self, origin, processed=1, total=2, sent=1, client="c1", gateway=3):
        self.input.deliver(self.worker, internal.encode_eof(client, gateway, origin, processed, total, sent))

    def result_rows(self, gateway=3):
        if gateway not in self.results:
            return []
        return [row for value in self.results[gateway].decoded(internal.MsgType.RESULTS) for row in value["rows"]]

    def test_strict_exact_comparison_and_empty_baseline(self):
        baseline = internal.FinalAverage(5, 2)
        self.assertFalse(exceeds_average(2, baseline))
        self.assertTrue(exceeds_average(3, baseline))
        self.assertFalse(exceeds_average(3, internal.FinalAverage(6, 2)))
        self.assertFalse(exceeds_average(100, internal.FinalAverage(0, 0)))
        self.assertFalse(exceeds_average(0, internal.FinalAverage(0, 2)))
        self.assertTrue(exceeds_average(1, internal.FinalAverage(0, 2)))
        self.assertFalse(exceeds_average(2 ** 60, internal.FinalAverage(2 ** 60 + 1, 1)))

    def test_candidates_are_written_before_ack_while_waiting_for_average(self):
        rows = self.send(2, 3, 4)
        self.assertEqual(list(self.files.read("c1")), [list(row) for row in rows])
        self.assertEqual(self.results, {})
        self.assertEqual(self.input.acks, 1)

    def test_drains_pending_file_and_publishes_only_matches(self):
        rows = self.send(2, 3, 4)
        self.average(6, 2)
        self.assertEqual(self.result_rows(), [list(rows[2])])
        self.assertEqual(list(self.files.read("c1")), [])
        self.assertEqual(self.averages.acks, 1)

    def test_average_ack_is_scheduled_on_its_connection_thread(self):
        self.average(0, 1, pump=False)
        self.input.pump()
        self.assertEqual(self.averages.acks, 0)
        self.averages.pump()
        self.assertEqual(self.averages.acks, 1)

    def test_candidates_after_average_are_compared_without_spooling(self):
        self.average(2, 1)
        with patch.object(self.files, "append", wraps=self.files.append) as append:
            rows = self.send(2, 3)
        append.assert_not_called()
        self.assertEqual(self.result_rows(), [list(rows[1])])

    def test_eofs_before_average_wait_until_pending_results_are_published(self):
        self.send(4)
        self.eof("text_processor_0", 1, 1, 1)
        self.eof("text_processor_1", 0, 1, 0)
        self.assertEqual(self.results, {})
        self.average(2, 1)
        kinds = [internal.decode(message)[0] for message in self.results[3].messages]
        self.assertEqual(kinds, [internal.MsgType.RESULTS, internal.MsgType.EOF])
        [eof] = self.results[3].decoded(internal.MsgType.EOF)
        self.assertEqual((eof["query"], eof["processed"], eof["total"], eof["sent"]), (5, 1, 1, 1))
        self.assertEqual(self.worker._baselines, {})

    def test_eofs_before_candidate_wait_for_advertised_messages(self):
        self.average(0, 1)
        self.eof("text_processor_0", 1, 1, 1)
        self.eof("text_processor_1", 0, 1, 0)
        self.assertEqual(self.results, {})
        self.send(1)
        self.assertEqual(len(self.results[3].decoded(internal.MsgType.EOF)), 1)

    def test_late_upstream_accounting_is_required_before_completion(self):
        self.average(0, 1)
        self.send(1)
        self.eof("text_processor_0", 0, 1, 0)
        self.eof("text_processor_1", 0, 1, 0)
        self.assertEqual(self.results[3].decoded(internal.MsgType.EOF), [])
        self.input.deliver(self.worker, internal.encode_late_eof("c1", 3, "text_processor_0", 1, 1))
        self.assertEqual(len(self.results[3].decoded(internal.MsgType.EOF)), 1)

    def test_file_draining_is_bounded_even_when_nothing_matches(self):
        self.worker._drain_rows = 1
        self.send(0, 0, 0)
        self.average(10, 1, pump=False)
        self.input.pending.get_nowait()()  # Acepta el promedio y programa la primera lectura.
        self.input.pending.get_nowait()()  # Lee solo una fila, aunque no produce resultados.
        self.assertIn("c1", self.worker._drains)
        self.assertEqual(self.averages.acks, 0)
        self.assertEqual(self.results, {})
        self.input.pump()
        self.averages.pump()
        self.assertNotIn("c1", self.worker._drains)
        self.assertEqual(self.averages.acks, 1)

    def test_new_candidates_can_arrive_during_pending_file_drain(self):
        self.worker._drain_rows = 1
        old = self.send(2, 3, 4)
        self.average(1, 1, pump=False)
        self.input.pending.get_nowait()()
        self.input.pending.get_nowait()()
        new = self.send(5)
        self.input.pump()
        self.averages.pump()
        self.assertCountEqual(self.result_rows(), [list(row) for row in old + new])

    def test_duplicate_baseline_does_not_replay_pending_results(self):
        self.send(4)
        self.average(2, 1, pump=False)
        self.average(2, 1, pump=False)
        self.input.pump()
        self.averages.pump()
        self.assertEqual(len(self.result_rows()), 1)
        self.assertEqual(self.averages.acks, 2)
        self.average(2, 1)
        self.assertEqual(len(self.result_rows()), 1)

    def test_conflicting_baseline_is_discarded(self):
        self.send(4)
        self.average(2, 1, pump=False)
        self.average(100, 1, pump=False)
        self.input.pump()
        self.averages.pump()
        self.assertEqual(len(self.result_rows()), 1)
        self.assertEqual(self.worker._baselines["c1"], internal.FinalAverage(2, 1))

    def test_no_historical_articles_produces_no_matches_and_completes(self):
        self.send(10, 20)
        self.average(0, 0)
        self.eof("text_processor_0", 1, 1, 1)
        self.eof("text_processor_1", 0, 1, 0)
        self.assertEqual(self.result_rows(), [])
        self.assertEqual(self.results[3].decoded(internal.MsgType.EOF)[0]["sent"], 0)

    def test_empty_candidate_input_still_completes(self):
        self.average(5, 2)
        self.eof("text_processor_0", 0, 0, 0)
        self.eof("text_processor_1", 0, 0, 0)
        [eof] = self.results[3].decoded(internal.MsgType.EOF)
        self.assertEqual((eof["processed"], eof["total"], eof["sent"]), (0, 0, 0))

    def test_clients_have_separate_files_baselines_and_gateways(self):
        a = self.send(3, client="a", gateway=0)
        b = self.send(3, client="b", gateway=7)
        self.average(2, 1, client="a", gateway=0)
        self.average(4, 1, client="b", gateway=7)
        self.assertEqual(self.result_rows(0), [list(a[0])])
        self.assertEqual(self.result_rows(7), [])
        self.assertEqual(b[0].word_count, 3)

    def test_result_limits_include_envelope_and_batch_row_limit(self):
        self.average(0, 1)
        self.send(1, 2, 3, 4, 5)
        self.assertEqual([len(v["rows"]) for v in self.results[3].decoded()], [2, 2, 1])
        self.results[3].messages.clear()
        row = internal.CandidateArticle("url", 1)
        limit = len(internal.encode_results("c1", 3, 5, [row]))
        self.worker._max_bytes = limit
        rows = [row, internal.CandidateArticle("X" * 1000, 10), row]
        self.input.deliver(self.worker, internal.encode_candidates("c1", 3, rows))
        self.assertEqual(self.result_rows(), [list(row), list(row)])
        self.assertTrue(all(len(message) <= limit for message in self.results[3].messages))

    def test_file_write_failure_leaves_candidate_unacknowledged(self):
        with patch.object(self.files, "append", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.send(1)
        self.assertEqual(self.input.acks, 0)

    def test_file_read_failure_leaves_average_unacknowledged(self):
        with patch.object(self.files, "read_raw", side_effect=OSError("read failed")):
            self.average(0, 1, pump=False)
            with self.assertRaises(OSError):
                self.input.pump()
        self.assertEqual(self.averages.acks, 0)

    def test_publication_failure_leaves_direct_candidate_unacknowledged(self):
        self.average(0, 1)
        output = OutputQueue()
        self.worker._open_results_queue = lambda gateway: output
        with patch.object(output, "send", side_effect=OSError("broker unavailable")):
            with self.assertRaises(OSError):
                self.send(1)
        self.assertEqual(self.input.acks, 0)

    def test_drain_publication_failure_preserves_file_and_average_delivery(self):
        self.send(1)
        output = OutputQueue()
        self.worker._open_results_queue = lambda gateway: output
        with patch.object(output, "send", side_effect=OSError("broker unavailable")):
            self.average(0, 1, pump=False)
            with self.assertRaises(OSError):
                self.input.pump()
        self.assertEqual(self.averages.acks, 0)
        self.assertEqual(len(list(self.files.read("c1"))), 1)

    def test_duplicate_average_after_completion_does_not_create_new_client_state(self):
        self.average(0, 0)
        self.eof("text_processor_0", 0, 0, 0)
        self.eof("text_processor_1", 0, 0, 0)
        self.average(0, 0)
        self.assertEqual(self.worker._baselines, {})
        self.assertEqual(len(self.results[3].messages), 1)

    def test_stop_preserves_pending_file_and_leaves_average_unacknowledged(self):
        self.send(4)
        self.average(2, 1, pump=False)
        self.input.pending.get_nowait()()
        self.worker.stop()
        self.input.pump()
        self.worker.close()
        self.assertTrue(self.input.stopped)
        self.assertEqual(self.averages.acks, 0)
        self.assertEqual(len(list(self.files.read("c1"))), 1)

    def test_malformed_inputs_are_discarded(self):
        self.input.deliver(self.worker, b"invalid")
        self.worker._on_average_message(b"invalid", self.averages.ack, noop)
        self.assertEqual((self.input.acks, self.averages.acks), (1, 1))
