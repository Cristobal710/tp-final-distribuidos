import contextlib
import hashlib
import io
import unittest
from unittest.mock import Mock, patch

import requests

from tests.full_suite.images import snapshot_image


class FullSuiteImagesTest(unittest.TestCase):
    def response(self, data=b"image", status=200, content_type="image/png"):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.status_code = status
        response.ok = status < 400
        response.headers = {"Content-Type": content_type}
        response.iter_content.return_value = iter([data])
        if status >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(response=response)
        return response

    def stop_event(self):
        stop = Mock()
        stop.is_set.return_value = False
        stop.wait.return_value = False
        return stop

    def test_transient_failures_retry_past_previous_cap_and_preserve_large_image(self):
        data = b"x" * (1048576 + 1)
        stop = self.stop_event()
        failures = [requests.ConnectionError("unavailable")] * 8
        with patch("tests.full_suite.images.requests.get", side_effect=failures + [self.response(data)]) as get, \
                patch("tests.full_suite.images.THROTTLE"), contextlib.redirect_stdout(io.StringIO()):
            result = snapshot_image("https://images/person", stop_event=stop)
        self.assertTrue(result["success"])
        self.assertEqual((result["bytes"], result["sha256"]), (len(data), hashlib.sha256(data).hexdigest()))
        self.assertEqual(get.call_count, 9)
        self.assertEqual([call.args[0] for call in stop.wait.call_args_list], [1, 2, 4, 8, 16, 32, 60, 60])

    def test_http_rate_limits_and_server_errors_retry_until_success(self):
        for status in (429, 503):
            with self.subTest(status=status):
                stop = self.stop_event()
                responses = [self.response(status=status) for _ in range(4)] + [self.response()]
                responses[0].headers["Retry-After"] = "600"
                with patch("tests.full_suite.images.requests.get", side_effect=responses) as get, \
                        patch("tests.full_suite.images.THROTTLE"), contextlib.redirect_stdout(io.StringIO()):
                    self.assertTrue(snapshot_image("https://images/person", stop_event=stop)["success"])
                self.assertEqual(get.call_count, 5)
                self.assertEqual(stop.wait.call_args_list[0].args, (600,))

    def test_permanent_responses_are_not_retried(self):
        for options in ({"status": 404}, {"content_type": "text/html"}, {"data": b""}):
            with self.subTest(options=options), \
                    patch("tests.full_suite.images.requests.get", return_value=self.response(**options)) as get, \
                    patch("tests.full_suite.images.THROTTLE"):
                self.assertFalse(snapshot_image("https://images/person")["success"])
                get.assert_called_once()

    def test_retry_wait_can_be_cancelled(self):
        stop = self.stop_event()
        stop.wait.return_value = True
        with patch("tests.full_suite.images.requests.get", side_effect=requests.Timeout("timeout")) as get, \
                patch("tests.full_suite.images.THROTTLE"), contextlib.redirect_stdout(io.StringIO()), \
                self.assertRaises(InterruptedError):
            snapshot_image("https://images/person", stop_event=stop)
        get.assert_called_once()

    def test_pre_cancelled_reference_never_starts_http(self):
        stop = self.stop_event()
        stop.is_set.return_value = True
        with patch("tests.full_suite.images.requests.get") as get, self.assertRaises(InterruptedError):
            snapshot_image("https://images/person", stop_event=stop)
        get.assert_not_called()
