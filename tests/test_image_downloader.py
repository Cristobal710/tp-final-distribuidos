import base64
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

from common.protocol import internal
from download import DownloadCancelled, DownloadError, download_image
from image_downloader import ImageDownloader
from tests.q2_fakes import ConsumerQueue, GroupControl, OutputQueue

PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aZ1sAAAAASUVORK5CYII=")


class ImageDownloaderTest(unittest.TestCase):

    def setUp(self):
        self.input = ConsumerQueue()
        self.control = GroupControl()
        self.results = {}
        self.download_threads = []

        def fetch(url, *, stop_event):
            self.download_threads.append(threading.get_ident())
            return bytes(range(256))

        def open_results(gateway_id):
            return self.results.setdefault(gateway_id, OutputQueue())

        self.worker = ImageDownloader(0, self.input, None, self.control, open_results, fetch, 2, 1_000_000)
        self.control.workers = [self.worker]
        self.addCleanup(self.worker.close)

    def start(self, *urls, client_id="c1", gateway_id=3):
        images = [internal.ImageRequest(f"Person {i}", f"https://wiki/{i}", url) for i, url in enumerate(urls)]
        self.input.deliver(self.worker, internal.encode_images(client_id, gateway_id, images))

    def finish(self):
        self.input.until(lambda: self.worker._active is None)

    def eof(self, origin, processed=1, total=2, sent=1, client_id="c1", gateway_id=3):
        self.input.deliver(self.worker, internal.encode_eof(client_id, gateway_id, origin, processed, total, sent))
        self.input.pump()

    def test_streams_original_bytes_and_routes_by_gateway(self):
        self.start("https://images/1", "https://images/2")
        self.finish()
        messages = self.results[3].decoded(internal.MsgType.RESULTS)
        self.assertEqual(len(messages), 2)
        self.assertEqual([len(message["rows"]) for message in messages], [1, 1])
        for i, message in enumerate(messages):
            self.assertEqual((message["client_id"], message["gateway_id"], message["query"]), ("c1", 3, 2))
            self.assertEqual(message["rows"][0][:2], [f"Person {i}", f"https://wiki/{i}"])
            self.assertEqual(bytes(message["rows"][0][2]), bytes(range(256)))
        self.assertTrue(all(t != threading.get_ident() for t in self.download_threads))
        self.assertEqual(self.results[3].threads, [threading.get_ident()] * 2)
        self.assertEqual(self.input.acks, 1)

    def test_download_does_not_block_consumer_or_ack_early(self):
        entered, release = threading.Event(), threading.Event()

        def fetch(url, *, stop_event):
            entered.set()
            release.wait(3)
            return PNG

        self.worker._download_image = fetch
        self.addCleanup(release.set)
        self.start("https://images/1")
        self.assertTrue(entered.wait(1))
        self.assertEqual(self.input.acks, 0)
        self.assertEqual(self.results, {})
        ran = []
        self.input.run_in_consumer_thread(lambda: ran.append(True))
        self.input.pump()
        self.assertEqual(ran, [True])
        release.set()
        self.finish()
        self.assertEqual(self.input.acks, 1)

    def test_terminal_failure_is_skipped_and_next_image_is_processed(self):
        def fetch(url, *, stop_event):
            if url == "bad":
                raise DownloadError("404")
            return PNG

        self.worker._download_image = fetch
        self.start("good", "bad", "another")
        self.finish()
        self.assertEqual(len(self.results[3].decoded(internal.MsgType.RESULTS)), 2)
        self.assertEqual((self.worker._processed["c1"], self.worker._sent["c1"]), (1, 2))
        self.assertEqual(self.input.acks, 1)

    def test_all_failed_downloads_still_allow_query_completion(self):
        def fetch(url, *, stop_event):
            raise DownloadError("unavailable")

        self.worker._download_image = fetch
        self.start("bad", "bad")
        self.finish()
        self.eof("filter_biography_0", processed=1, total=1, sent=1)
        self.eof("filter_biography_1", processed=0, total=1, sent=0)
        [eof] = self.results[3].decoded(internal.MsgType.EOF)
        self.assertEqual((eof["query"], eof["processed"], eof["total"], eof["sent"]), (2, 1, 1, 0))

    def test_eof_during_active_download_is_deferred_until_result(self):
        release = threading.Event()
        self.addCleanup(release.set)

        def fetch(url, *, stop_event):
            release.wait(3)
            return PNG

        self.worker._download_image = fetch
        self.start("https://images/1")
        for origin in ("filter_biography_0", "filter_biography_1"):
            _, value = internal.decode(internal.encode_eof("c1", 3, origin, 1, 2, 1))
            self.worker._on_upstream(internal.MsgType.EOF, value)
        self.assertEqual(self.results, {})
        release.set()
        self.finish()
        kinds = [internal.decode(message)[0] for message in self.results[3].messages]
        self.assertEqual(kinds, [internal.MsgType.RESULTS, internal.MsgType.EOF])
        [eof] = self.results[3].decoded(internal.MsgType.EOF)
        self.assertEqual((eof["processed"], eof["total"], eof["sent"]), (1, 2, 1))

    def test_batch_after_local_eof_publishes_late_counts(self):
        self.eof("filter_biography_0", processed=1, total=1, sent=1)
        self.eof("filter_biography_1", processed=0, total=1, sent=0)
        self.start("https://images/1")
        self.finish()
        [eof] = self.results[3].decoded(internal.MsgType.EOF)
        [late] = self.results[3].decoded(internal.MsgType.LATE_EOF)
        self.assertEqual((eof["processed"], eof["total"], eof["sent"]), (0, 1, 0))
        self.assertEqual((late["processed"], late["sent"], late["query"]), (1, 1, 2))

    def test_zero_input_emits_query_eof_to_correct_gateway(self):
        self.eof("filter_biography_0", processed=0, total=0, sent=0, client_id="empty", gateway_id=7)
        self.assertEqual(self.results, {})
        self.eof("filter_biography_1", processed=0, total=0, sent=0, client_id="empty", gateway_id=7)
        [eof] = self.results[7].decoded(internal.MsgType.EOF)
        self.assertEqual((eof["client_id"], eof["query"], eof["processed"], eof["total"], eof["sent"]),
                         ("empty", 2, 0, 0, 0))

    def test_waits_for_late_upstream_processing(self):
        self.eof("filter_biography_0", processed=0, total=1, sent=0)
        self.eof("filter_biography_1", processed=0, total=1, sent=0)
        self.assertEqual(self.results, {})
        self.input.deliver(self.worker, internal.encode_late_eof("c1", 3, "filter_biography_0", 1, 0))
        self.input.pump()
        self.assertEqual(len(self.results[3].decoded(internal.MsgType.EOF)), 1)

    def test_serialized_result_limit_is_enforced(self):
        self.worker._max_result_bytes = 100
        self.start("https://images/1")
        self.finish()
        self.assertEqual(self.results, {})
        self.assertEqual(self.worker._sent["c1"], 0)
        self.assertEqual(self.input.acks, 1)

    def test_publication_failure_leaves_batch_unacknowledged(self):
        output = OutputQueue()
        self.worker._open_results_queue = lambda gateway_id: output
        with patch.object(output, "send", side_effect=RuntimeError("broker unavailable")):
            self.start("https://images/1")
            with self.assertRaises(RuntimeError):
                self.finish()
        self.assertEqual(self.input.acks, 0)

    def test_unexpected_download_error_is_not_treated_as_skippable(self):
        def fetch(url, *, stop_event):
            raise RuntimeError("programming error")

        self.worker._download_image = fetch
        self.start("https://images/1")
        with self.assertRaises(RuntimeError):
            self.finish()
        self.assertEqual(self.input.acks, 0)

    def test_stop_cancels_active_batch_without_ack(self):
        entered = threading.Event()

        def fetch(url, *, stop_event):
            entered.set()
            stop_event.wait(3)
            raise DownloadCancelled()

        self.worker._download_image = fetch
        self.start("https://images/1")
        self.assertTrue(entered.wait(1))
        self.worker.stop()
        self.worker.close()
        self.input.pump()
        self.assertTrue(self.input.stopped)
        self.assertEqual(self.input.acks, 0)
        self.assertEqual(self.results, {})

    def test_clients_use_separate_gateway_publishers(self):
        for client, gateway in (("a", 1), ("b", 5)):
            self.start("https://images/1", client_id=client, gateway_id=gateway)
            self.finish()
        self.assertEqual(self.results[1].decoded()[0]["client_id"], "a")
        self.assertEqual(self.results[5].decoded()[0]["client_id"], "b")
        self.worker.close()
        self.assertTrue(all(output.closed for output in self.results.values()))

    def test_malformed_input_is_discarded(self):
        self.input.deliver(self.worker, b"invalid")
        self.assertEqual(self.input.acks, 1)
        self.assertEqual(self.results, {})


class DownloadHTTPTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.calls = {}
        cls.call_lock = threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                with cls.call_lock:
                    count = cls.calls[self.path] = cls.calls.get(self.path, 0) + 1
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/image")
                    self.end_headers()
                    return
                status = 200
                if self.path in ("/retry", "/rate") and count < 3:
                    status = 503 if self.path == "/retry" else 429
                if self.path in ("/retry-after", "/reference-retry-after") and count == 1:
                    status = 429
                if self.path == "/notfound":
                    status = 404
                if self.path == "/slow":
                    time.sleep(0.2)
                body = b"" if self.path == "/empty" else PNG
                self.send_response(status)
                if status == 429 and self.path.endswith("retry-after"):
                    self.send_header("Retry-After", "600")
                self.send_header("Content-Type", "text/html" if self.path == "/html" else "image/png")
                if self.path != "/no-length":
                    self.send_header("Content-Length", str(len(body) + (10 if self.path == "/truncated" else 0)))
                self.end_headers()
                try:
                    if self.path == "/trickle":
                        for byte in body:
                            self.wfile.write(bytes([byte]))
                            self.wfile.flush()
                            time.sleep(0.02)
                    else:
                        self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def fetch(self, path, **kwargs):
        options = dict(timeout=1, attempts=3, max_bytes=10_000, retry_delay=0)
        options.update(kwargs)
        return download_image(self.base_url + path, **options)

    def test_download_preserves_original_bytes(self):
        self.assertEqual(self.fetch("/image"), PNG)

    def test_follows_redirects(self):
        self.assertEqual(self.fetch("/redirect"), PNG)

    def test_reads_without_content_length(self):
        self.assertEqual(self.fetch("/no-length"), PNG)

    def test_retries_server_errors_and_rate_limits(self):
        for path in ("/retry", "/rate"):
            with self.subTest(path=path):
                self.assertEqual(self.fetch(path), PNG)
                self.assertEqual(self.calls[path], 3)

    def test_server_retry_after_overrides_shorter_backoff(self):
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.return_value = False
        self.assertEqual(self.fetch("/retry-after", stop_event=stop), PNG)
        stop.wait.assert_called_once_with(600)

    def test_reference_also_honors_server_retry_after(self):
        from tests.full_suite.images import snapshot_image
        with patch("tests.full_suite.images.time.sleep") as sleep:
            result = snapshot_image(self.base_url + "/reference-retry-after")
        self.assertTrue(result["success"])
        sleep.assert_called_once_with(600)

    def test_permanent_failures_do_not_retry(self):
        for path in ("/notfound", "/html", "/empty"):
            with self.subTest(path=path):
                with self.assertRaises(DownloadError):
                    self.fetch(path)
                self.assertEqual(self.calls[path], 1)

    def test_limits_bytes_with_and_without_content_length(self):
        for path in ("/image", "/no-length"):
            with self.subTest(path=path), self.assertRaises(DownloadError):
                self.fetch(path, max_bytes=len(PNG) - 1)
        self.assertEqual(self.fetch("/image", max_bytes=len(PNG)), PNG)

    def test_truncated_body_is_retried_and_rejected(self):
        before = self.calls.get("/truncated", 0)
        with self.assertRaises(DownloadError):
            self.fetch("/truncated")
        self.assertEqual(self.calls["/truncated"] - before, 3)

    def test_socket_timeout_is_bounded_and_retried(self):
        started = time.monotonic()
        with self.assertRaises(DownloadError):
            self.fetch("/slow", timeout=0.02, attempts=2)
        self.assertLess(time.monotonic() - started, 1)

    def test_slow_trickle_cannot_extend_download_indefinitely(self):
        started = time.monotonic()
        with self.assertRaises(DownloadError):
            self.fetch("/trickle", timeout=0.08, attempts=1)
        self.assertLess(time.monotonic() - started, 0.5)

    def test_invalid_urls_are_rejected(self):
        for url in (None, "", "file:///etc/passwd", "https://", "http://localhost:invalid/image"):
            with self.subTest(url=url), self.assertRaises(DownloadError):
                download_image(url, timeout=1, attempts=1, max_bytes=100)

    def test_pre_cancelled_download_never_starts(self):
        cancelled = threading.Event()
        cancelled.set()
        with self.assertRaises(DownloadCancelled):
            self.fetch("/image", stop_event=cancelled)

    def test_retry_wait_is_interruptible(self):
        cancelled = threading.Event()
        timer = threading.Timer(0.05, cancelled.set)
        timer.start()
        self.addCleanup(timer.cancel)
        started = time.monotonic()
        with self.assertRaises(DownloadCancelled):
            self.fetch("/truncated", retry_delay=10, stop_event=cancelled)
        self.assertLess(time.monotonic() - started, 1)
