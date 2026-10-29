import io
import json
import subprocess
import unittest
from unittest.mock import Mock, patch

from tests.full_suite.monitor import require_running, run_client


class FullSuiteMonitorTest(unittest.TestCase):
    def test_exited_missing_and_unhealthy_workers_fail(self):
        for records in ([], [{"Service": "classifier_0", "State": "exited", "ExitCode": 1}],
                        [{"Service": "classifier_0", "State": "running", "Health": "unhealthy"}]):
            with self.subTest(records=records), self.assertRaisesRegex(RuntimeError, "classifier_0"):
                require_running(json.dumps(records), {"classifier_0"})

    def test_compose_json_lines_and_arrays_are_supported(self):
        records = [{"Service": name, "State": "running", "Health": ""} for name in ("a", "b")]
        for output in (json.dumps(records), "\n".join(map(json.dumps, records))):
            require_running(output, {"a", "b"})

    def test_worker_failure_terminates_client_immediately(self):
        process = Mock()
        process.poll.return_value = None
        check = Mock(side_effect=RuntimeError("classifier_0 exited"))
        with patch("tests.full_suite.monitor.subprocess.Popen", return_value=process), \
                patch("tests.full_suite.monitor.time.sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "classifier_0"):
                run_client(["client"], cwd=".", log=io.StringIO(), timeout=7200, check_workers=check)
        process.terminate.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=10)
        sleep.assert_not_called()

    def test_client_failure_is_propagated(self):
        process = Mock(returncode=2)
        process.poll.return_value = 2
        with patch("tests.full_suite.monitor.subprocess.Popen", return_value=process):
            with self.assertRaises(subprocess.CalledProcessError):
                run_client(["client"], cwd=".", log=io.StringIO(), timeout=10, check_workers=Mock())

    def test_successful_client_still_requires_healthy_workers(self):
        process = Mock(returncode=0)
        process.poll.return_value = 0
        check = Mock()
        with patch("tests.full_suite.monitor.subprocess.Popen", return_value=process):
            run_client(["client"], cwd=".", log=io.StringIO(), timeout=10, check_workers=check)
        check.assert_called_once_with()
        process.terminate.assert_not_called()
