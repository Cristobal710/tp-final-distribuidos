import os
import unittest


@unittest.skipUnless(os.environ.get("RUN_FULL_SUITE") == "1", "Opt-in 2 GB live-image test: make test-full")
class SingleClientFullSuite(unittest.TestCase):
    configuration = {}

    def test_single_client_matches_all_five_queries(self):
        from tests.full_suite.run import execute
        report = execute(**self.configuration)
        self.assertTrue(report["passed"])
        self.assertEqual(report["client_sessions"], 1)
        self.assertEqual(set(report["queries"]), {"1", "2", "3", "4", "5"})
