import hashlib
import json
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock

from common.protocol import external, internal
from receiver import ResultReceiver
from results import Results
from server import Server


class ResultsDeliveryTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.gateway, self.client = socket.socketpair()
        self.addCleanup(self.gateway.close)
        self.addCleanup(self.client.close)
        self.failed = Mock()
        self.receiver = ResultReceiver(self.client, self.directory.name, self.failed)
        self.results = Results(lambda: None, {1: 2, 2: 2, 3: 1, 4: 1, 5: 2})
        self.session = self.results.register("client", self.gateway)

    def deliver(self, message):
        ack = Mock()
        self.results._on_message(message, ack, Mock(side_effect=AssertionError("Unexpected nack")))
        ack.assert_called_once_with()

    def eof(self, query, origin, processed=0, total=0, sent=0):
        self.deliver(internal.encode_eof("client", 0, origin, processed, total, sent, query))

    def test_five_queries_return_images_and_wait_for_late_shard(self):
        self.receiver.start()
        try:
            self.results.finish_input("client")
            self.deliver(internal.encode_results("client", 0, 1, [["old", 123]]))
            self.eof(1, "classifier_0", 1, 2, 1)
            self.eof(1, "classifier_1", 0, 2, 0)
            self.assertNotIn(1, self.session.finished_queries)
            self.deliver(internal.encode_results("client", 0, 1, [["late", 456]]))
            self.deliver(internal.encode_late_eof("client", 0, "classifier_1", 1, 1, 1))
            image = bytes([0, 1, 127, 128, 255])
            self.deliver(internal.encode_results("client", 0, 2, [["Biographie", "bio", list(image)]]))
            self.eof(2, "image_0", 1, 1, 1)
            self.eof(2, "image_1", 0, 1, 0)
            self.deliver(internal.encode_results("client", 0, 3, [["shared"]]))
            self.eof(3, "join", 1, 1, 1)
            self.deliver(internal.encode_results("client", 0, 4, [["shared", 10]]))
            self.eof(4, "top", 1, 1, 1)
            self.eof(5, "comparator_0", 7, 7, 1)
            self.eof(5, "comparator_1", 3, 3, 0)
            self.assertNotIn(5, self.session.finished_queries)
            self.assertFalse(self.session.done.is_set())
            self.deliver(internal.encode_results("client", 0, 5, [["recent", 12]]))
            self.assertTrue(self.session.done.is_set())
        finally:
            self.gateway.close()
        self.receiver.wait(2)
        self.failed.assert_not_called()
        self.assertEqual(self.receiver.rows, {1: 2, 2: 1, 3: 1, 4: 1, 5: 1})
        root = Path(self.directory.name)
        digest = hashlib.sha256(image).hexdigest()
        self.assertEqual((root / "images" / f"{digest}.bin").read_bytes(), image)
        self.assertEqual(json.loads((root / "q2.jsonl").read_text()), ["Biographie", "bio", digest, 5])

    def test_empty_queries_wait_for_input_completion(self):
        self.receiver.start()
        try:
            for query, origins in ((1, 2), (2, 2), (3, 1), (4, 1), (5, 2)):
                for origin in range(origins):
                    self.eof(query, str(origin))
            self.assertFalse(self.session.done.is_set())
            self.results.finish_input("client")
            self.assertTrue(self.session.done.is_set())
        finally:
            self.gateway.close()
        self.receiver.wait(2)
        self.assertEqual(sum(self.receiver.rows.values()), 0)

    def test_unknown_session_is_not_delivered_to_another_client(self):
        self.deliver(internal.encode_results("other", 0, 1, [["wrong", 123]]))
        self.client.settimeout(0.02)
        with self.assertRaises(socket.timeout):
            self.client.recv(1)


class ResultReceiverFailureTest(unittest.TestCase):
    def run_frames(self, frames):
        with tempfile.TemporaryDirectory() as directory:
            gateway, client = socket.socketpair()
            with gateway, client:
                failed = Mock()
                receiver = ResultReceiver(client, directory, failed)
                for kind, value in frames:
                    external.send_message(gateway, kind, external.serialize(value) if value is not None else b"")
                gateway.shutdown(socket.SHUT_WR)
                receiver.start()
                with self.assertRaises((external.ProtocolError, ConnectionError)):
                    receiver.wait(2)
                failed.assert_called_once_with()

    def test_incomplete_stream_fails(self):
        self.run_frames([(external.MsgType.RESULTS, {"query": 1, "rows": [["url", 1]]})])

    def test_early_all_done_fails(self):
        self.run_frames([(external.MsgType.ALL_DONE, None)])

    def test_duplicate_completion_fails(self):
        self.run_frames([(external.MsgType.QUERY_DONE, {"query": 1})] * 2)

    def test_invalid_image_byte_fails(self):
        self.run_frames([(external.MsgType.RESULTS, {"query": 2, "rows": [["name", "url", [256]]]})])

    def test_trailing_result_after_all_done_fails(self):
        frames = [(external.MsgType.QUERY_DONE, {"query": q}) for q in range(1, 6)]
        self.run_frames(frames + [(external.MsgType.ALL_DONE, None),
                                 (external.MsgType.RESULTS, {"query": 1, "rows": [["late", 1]]})])

    def test_output_error_stops_sender(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "file"
            path.write_text("not a directory")
            failed = Mock()
            receiver = ResultReceiver(None, path, failed)
            receiver.start()
            with self.assertRaises(FileExistsError):
                receiver.wait(2)
            failed.assert_called_once_with()


class GatewayReturnPathTest(unittest.TestCase):
    def test_end_of_input_keeps_socket_until_all_results_return(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway, client = socket.socketpair()
            with gateway, client:
                results = Results(lambda: None, {q: 1 for q in range(1, 6)})
                published = []
                publisher = Mock()

                def publish(message):
                    kind, value = internal.decode(message)
                    published.append((kind, value))
                    if kind == internal.MsgType.EOF:
                        for query in range(1, 6):
                            results._on_message(internal.encode_eof(
                                value["client_id"], 0, "origin", 0, 0, 0, query), lambda: None, lambda: None)

                publisher.send.side_effect = publish
                server = Server("unused", 0, 0, lambda: publisher, results)
                handler = threading.Thread(target=server._handle_client, args=(gateway, ("local", 0)))
                receiver = ResultReceiver(client, directory, Mock())
                handler.start()
                receiver.start()
                external.send_message(client, external.MsgType.END_OF_DATA)
                receiver.wait(2)
                handler.join(2)
                self.assertFalse(handler.is_alive())
                self.assertTrue(receiver.all_done)
                self.assertEqual([kind for kind, _ in published], [internal.MsgType.EOF])
                publisher.close.assert_called_once_with()
                self.assertEqual(results._sessions, {})
