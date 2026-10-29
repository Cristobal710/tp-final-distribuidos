import unittest
from unittest.mock import patch

from common.protocol import external, internal
from filter_biography import FilterBiography, is_biography
from tests.q2_fakes import ConsumerQueue, GroupControl, OutputQueue


def bio(name="Ada", sections='[{"name":"Biography"}]'):
    return internal.Bio(name, f"https://wiki/{name}", sections, f"https://images/{name}.png")


class FilterBiographyTest(unittest.TestCase):

    def setUp(self):
        self.input = ConsumerQueue()
        self.output = OutputQueue()
        self.control = GroupControl()
        self.worker = FilterBiography(0, self.input, None, self.control, self.output, 2, 200, 1_000_000)
        self.control.workers = [self.worker]

    def send(self, *bios, client_id="c1", gateway_id=3):
        self.input.deliver(self.worker, internal.encode_bios(client_id, gateway_id, bios))

    def eof(self, origin, processed=1, total=2, sent=1, client_id="c1", gateway_id=3):
        self.input.deliver(self.worker, internal.encode_eof(client_id, gateway_id, origin, processed, total, sent))
        self.input.pump()

    def test_case_insensitive_search_in_full_sections_string(self):
        for sections in ('[{"name":"BIOGRAPHY"}]', '[{"name":"Biographie"}]', "a biography section"):
            with self.subTest(sections=sections):
                self.assertTrue(is_biography(sections))
        for sections in (None, "", "[]", '[{"name":"History"}]', {}, ["Biography"]):
            with self.subTest(sections=sections):
                self.assertFalse(is_biography(sections))

    def test_filters_and_drops_sections_preserving_metadata(self):
        self.send(bio(), bio("Emilie", "Biographie"), bio("Other", "History"))
        [message] = self.output.decoded(internal.MsgType.IMAGES)
        self.assertEqual((message["client_id"], message["gateway_id"]), ("c1", 3))
        self.assertEqual(message["images"], [internal.ImageRequest("Ada", "https://wiki/Ada", "https://images/Ada.png"),
                                             internal.ImageRequest("Emilie", "https://wiki/Emilie", "https://images/Emilie.png")])
        self.assertNotIn("sections", message)
        self.assertEqual(self.input.acks, 1)

    def test_dropped_and_empty_batches_count_as_processed_without_output(self):
        self.send(bio(sections="History"))
        self.send()
        self.assertEqual(self.output.messages, [])
        self.eof("classifier_0", processed=2, total=2, sent=2)
        self.assertEqual(self.output.messages, [])
        self.eof("classifier_1", processed=0, total=2, sent=0)
        [eof] = self.output.decoded(internal.MsgType.EOF)
        self.assertEqual((eof["processed"], eof["total"], eof["sent"]), (2, 2, 0))

    def test_complete_message_size_and_row_limits_are_enforced(self):
        row = internal.ImageRequest(*bio()[:2], bio().image_url)
        limit = len(internal.encode_images("c1", 3, [row]))
        self.worker._max_bytes = limit
        self.worker._max_rows = 2
        self.send(bio(), bio(), bio("X" * 1000), bio())
        images = self.output.decoded(internal.MsgType.IMAGES)
        self.assertEqual(sum(len(value["images"]) for value in images), 3)
        self.assertTrue(all(len(message) <= limit for message in self.output.messages))
        self.assertEqual(self.worker._sent["c1"], 3)

    def test_batches_respect_max_rows(self):
        self.worker._max_rows = 2
        self.send(*[bio() for _ in range(5)])
        self.assertEqual([len(value["images"]) for value in self.output.decoded()], [2, 2, 1])

    def test_waits_for_late_upstream_accounting(self):
        self.eof("classifier_0", processed=0, total=1, sent=0)
        self.eof("classifier_1", processed=0, total=1, sent=0)
        self.assertEqual(self.output.messages, [])
        self.send(bio())
        self.input.deliver(self.worker, internal.encode_late_eof("c1", 3, "classifier_0", 1, 1))
        self.input.pump()
        [eof] = self.output.decoded(internal.MsgType.EOF)
        self.assertEqual((eof["processed"], eof["total"], eof["sent"]), (1, 1, 1))

    def test_batch_after_local_eof_emits_late_eof(self):
        self.eof("classifier_0", processed=1, total=1, sent=1)
        self.eof("classifier_1", processed=0, total=1, sent=0)
        self.send(bio())
        [late] = self.output.decoded(internal.MsgType.LATE_EOF)
        self.assertEqual((late["processed"], late["sent"]), (1, 1))

    def test_clients_and_gateways_are_preserved(self):
        self.send(bio(), client_id="a", gateway_id=0)
        self.send(bio("Emilie", "Biographie"), client_id="b", gateway_id=8)
        self.assertEqual([(m["client_id"], m["gateway_id"]) for m in self.output.decoded()], [("a", 0), ("b", 8)])

    def test_malformed_input_is_acknowledged_and_discarded(self):
        for message in (b"invalid", external.serialize({"type": 4, "client_id": "x", "rows": [[]]})):
            self.input.deliver(self.worker, message)
        self.assertEqual(self.input.acks, 2)
        self.assertEqual(self.output.messages, [])

    def test_publication_failure_leaves_input_unacknowledged(self):
        with patch.object(self.output, "send", side_effect=RuntimeError("broker unavailable")):
            with self.assertRaises(RuntimeError):
                self.send(bio())
        self.assertEqual(self.input.acks, 0)

    def test_stop_and_close_release_owned_resources(self):
        self.worker.stop()
        self.worker.close()
        self.assertTrue(self.input.stopped)
        self.assertTrue(self.output.closed)
