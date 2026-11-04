import base64
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

from common.protocol import internal
from common.request_throttle import RequestThrottle
from download import DownloadCancelled, DownloadError, download_image
from image_cache import ImageCache
from image_downloader import ImageDownloader
from tests.q2_fakes import ConsumerQueue, GroupControl, OutputQueue

PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aZ1sAAAAASUVORK5CYII=")


class RequestThrottleTest(unittest.TestCase):

    def test_spaces_request_starts_and_does_not_add_delay_after_slow_requests(self):
        throttle = RequestThrottle(2.5)
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.return_value = False
        with patch("common.request_throttle.time.monotonic", side_effect=[10, 10, 11, 12.5, 20, 20]):
            for _ in range(3):
                self.assertTrue(throttle.wait(stop))
        stop.wait.assert_called_once_with(1.5)

    def test_throttle_wait_can_be_cancelled(self):
        throttle = RequestThrottle(60)
        stop = Mock()
        stop.is_set.return_value = False
        with patch("common.request_throttle.time.monotonic", return_value=10):
            self.assertTrue(throttle.wait(stop))
            stop.wait.return_value = True
            self.assertFalse(throttle.wait(stop))
        stop.wait.assert_called_once_with(60)

    def test_zero_disables_pacing_and_invalid_intervals_are_rejected(self):
        stop = Mock()
        stop.is_set.return_value = False
        throttle = RequestThrottle(0)
        for _ in range(2):
            self.assertTrue(throttle.wait(stop))
        stop.wait.assert_not_called()
        for interval in (-1, float("inf"), float("nan")):
            with self.subTest(interval=interval), self.assertRaises(ValueError):
                RequestThrottle(interval)


class ImageCacheTest(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def test_replicas_and_restarts_reuse_bytes_with_current_size_limit(self):
        cache = ImageCache(self.directory.name, 100)
        cache.put("https://images/a", PNG)
        other = ImageCache(self.directory.name, 100)
        self.assertEqual(other.get("https://images/a", len(PNG)), PNG)
        self.assertEqual(other.get("https://images/a"), PNG)
        self.assertIsNone(other.get("https://images/a", len(PNG) - 1))
        self.assertIsNone(other.get("https://images/b", 100))

    def test_evicts_least_recently_used_images_and_respects_reduced_capacity(self):
        cache = ImageCache(self.directory.name, 6)
        with patch("image_cache.time.time_ns", side_effect=range(1, 10)):
            cache.put("a", b"aaa")
            cache.put("b", b"bbb")
            self.assertEqual(cache.get("a", 100), b"aaa")
            cache.put("c", b"ccc")
            self.assertIsNone(cache.get("b", 100))
            cache.put("oversize", b"x" * 7)
            self.assertIsNone(cache.get("oversize", 100))
            smaller = ImageCache(self.directory.name, 3)
            self.assertIsNone(smaller.get("a", 100))
            self.assertEqual(smaller.get("c", 100), b"ccc")

    def test_cache_failure_does_not_prevent_a_download(self):
        cache = ImageCache(self.directory.name, 100)
        with patch("image_cache.sqlite3.connect", side_effect=OSError("disk unavailable")), \
                self.assertLogs(level="WARNING"):
            self.assertIsNone(cache.get("a", 100))
            cache.put("a", PNG)

    def test_simultaneous_replica_writes_keep_the_shared_capacity(self):
        caches = [ImageCache(self.directory.name, 400) for _ in range(2)]
        start = threading.Barrier(2)

        def populate(replica):
            start.wait(timeout=2)
            for index in range(20):
                caches[replica].put(f"{replica}/{index}", b"x" * 40)

        with self.assertNoLogs(level="WARNING"), ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(populate, range(2)))
        remaining = [caches[0].get(f"{replica}/{index}", 40)
                     for replica in range(2) for index in range(20)]
        self.assertEqual(sum(len(data) for data in remaining if data is not None), 400)


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

    def setUp(self):
        with self.call_lock:
            self.calls.clear()

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
                if self.path in ("/large", "/large-no-length"):
                    body += b"\0" * 1048576
                self.send_response(status)
                if status == 429 and self.path.endswith("retry-after"):
                    self.send_header("Retry-After", "600")
                self.send_header("Content-Type", "text/html" if self.path == "/html" else "image/png")
                if self.path not in ("/no-length", "/large-no-length"):
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

    def test_throttle_is_reused_across_images_and_includes_retries(self):
        throttle = Mock()
        throttle.wait.return_value = True
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        from urllib.error import HTTPError
        rate_limited = HTTPError(self.base_url + "/image", 429, "rate limited", {}, None)
        with patch("download.urlopen", side_effect=[rate_limited, response, response]), \
                patch("download._read_image", return_value=PNG):
            self.assertEqual(self.fetch("/image", throttle=throttle), PNG)
            self.assertEqual(self.fetch("/image", throttle=throttle), PNG)
        self.assertEqual(throttle.wait.call_count, 3)

    def test_cache_hits_avoid_http_and_throttle_and_failures_are_not_cached(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = ImageCache(directory, 1000)
            throttle = Mock()
            throttle.wait.return_value = True
            before = self.calls.get("/cached", 0)
            self.assertEqual(self.fetch("/cached", cache=cache, throttle=throttle), PNG)
            other = ImageCache(directory, 1000)
            self.assertEqual(self.fetch("/cached", cache=other, throttle=throttle), PNG)
            self.assertEqual(self.calls["/cached"] - before, 1)
            throttle.wait.assert_called_once()
            before = self.calls.get("/empty", 0)
            for _ in range(2):
                with self.assertRaises(DownloadError):
                    self.fetch("/empty", cache=cache)
            self.assertEqual(self.calls["/empty"] - before, 2)

    def test_cancellation_during_throttle_never_starts_http(self):
        throttle = Mock()
        throttle.wait.return_value = False
        with patch("download.urlopen") as open_url, self.assertRaises(DownloadCancelled):
            self.fetch("/image", throttle=throttle)
        open_url.assert_not_called()

    def test_server_retry_after_overrides_shorter_backoff(self):
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.return_value = False
        self.assertEqual(self.fetch("/retry-after", stop_event=stop), PNG)
        stop.wait.assert_called_once_with(600)

    def test_reference_also_honors_server_retry_after(self):
        from tests.full_suite.images import snapshot_image
        with patch("tests.full_suite.images.time.sleep") as sleep, \
                patch("tests.full_suite.images.THROTTLE") as throttle:
            result = snapshot_image(self.base_url + "/reference-retry-after")
        self.assertTrue(result["success"])
        sleep.assert_called_once_with(600)
        self.assertEqual(throttle.wait.call_count, 2)

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

    def test_default_download_and_cache_accept_images_larger_than_one_mebibyte(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = ImageCache(directory, 3 * 1048576)
            for path in ("/large", "/large-no-length"):
                with self.subTest(path=path):
                    url = self.base_url + path
                    image = download_image(url, timeout=2, attempts=1, cache=cache)
                    self.assertEqual(image, PNG + b"\0" * 1048576)
                    with patch("download.urlopen", side_effect=AssertionError("Unexpected HTTP request")):
                        self.assertEqual(download_image(url, timeout=2, attempts=1, cache=cache), image)
            from tests.full_suite.images import snapshot_image
            with patch("tests.full_suite.images.THROTTLE"):
                reference = snapshot_image(self.base_url + "/large")
            self.assertTrue(reference["success"])
            self.assertEqual(reference["bytes"], len(PNG) + 1048576)

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
