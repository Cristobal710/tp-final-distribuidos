import contextlib
import io
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from tests.full_suite.reference import calculate
from tests.test_client import write_parquet

OLD = datetime(2020, 12, 31, 23, 59, 59, 999000)
NEW = datetime(2021, 1, 1)


def article(url, text="", date=OLD, language="en", **fields):
    return {"url": url, "name": url, "abstract": text, "date_created": date,
            "date_modified": datetime(1970, 1, 1, 0, 0, 1), "in_language": {"identifier": language},
            "image": None, "sections": None, **fields}


class ReferenceScenariosTest(unittest.TestCase):
    def reference(self, rows, download=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "data"
            dataset.mkdir()
            write_parquet(dataset / "part.parquet", rows)
            (dataset / "manifest.json").write_text(json.dumps({"parts": [{"file": "part.parquet"}],
                                                              "rows": len(rows), "images_per_language": 0}))
            stopwords = root / "stopwords.json"
            stopwords.write_text(json.dumps(["the", "le"]))
            output = root / "expected"
            with contextlib.redirect_stdout(io.StringIO()), \
                    patch("tests.full_suite.reference.snapshot_image", side_effect=download) as fetch:
                summary = calculate(dataset, output, stopwords, require_coverage=False)
            result = {q: [json.loads(line) for line in (output / f"q{q}.jsonl").read_text().splitlines()]
                      for q in range(1, 6)}
            return result, summary, fetch.call_args_list

    def test_empty_input_has_five_empty_expected_outputs(self):
        result, summary, calls = self.reference([])
        self.assertEqual(result, {q: [] for q in range(1, 6)})
        self.assertEqual(summary["historical_articles"], 0)
        self.assertEqual(calls, [])

    def test_no_historical_baseline_does_not_accept_recent_articles(self):
        result, summary, _ = self.reference([article("new", "alpha beta", NEW), article("unknown", "gamma", None)])
        self.assertEqual(result[5], [])
        self.assertEqual(result[4], [["alpha", 1], ["beta", 1], ["gamma", 1]])
        self.assertEqual(summary["coverage"]["q5_no_historical_baseline"], 1)

    def test_zero_average_excludes_missing_abstract_but_counts_empty(self):
        result, summary, _ = self.reference([article("empty", ""), article("missing", None),
            article("zero", "the le 123", NEW), article("positive", "echo echo", NEW)])
        self.assertEqual((summary["historical_words"], summary["historical_articles"]), (0, 1))
        self.assertEqual(result[5], [["positive", 1]])
        self.assertEqual(summary["coverage"]["q5_equal_average"], 1)

    def test_distinct_count_strict_comparison_and_unknown_dates(self):
        result, summary, _ = self.reference([article("old", "alpha alpha beta"),
            article("equal", "one one two", NEW), article("below", "one", NEW),
            article("above", "one two three", NEW), article("unknown", "many more words here", None)])
        self.assertEqual((summary["historical_words"], summary["historical_articles"]), (2, 1))
        self.assertEqual(result[5], [["above", 3]])
        self.assertEqual([summary["coverage"][f"q5_{relation}_average"] for relation in ("below", "equal", "above")], [1, 1, 1])

    def test_q1_date_boundary_language_and_missing_modified_date(self):
        result, _, _ = self.reference([article("old-en"), article("new-en", date=NEW),
            article("old-fr", language="fr"), article("unknown", date=None), article("no-modified", date_modified=None)])
        self.assertEqual(result[1], [["old-en", 1000]])

    def test_q2_filters_before_download_and_excludes_permanent_http_failure(self):
        def fetch(url):
            return ({"success": False, "reason": "HTTP 404"} if url == "bad" else
                    {"success": True, "sha256": "digest", "bytes": 42})
        def bio(url, sections, **fields):
            return article(url, sections=sections, image={"content_url": url}, **fields)
        result, summary, calls = self.reference([
            bio("en", "BIOGRAPHY"), bio("fr", "Biographie", language="fr"), bio("bad", "biography"),
            bio("no-marker", "History"), bio("no-sections", None), bio("recent", "Biography", date=NEW),
            bio("unknown", "Biography", date=None), article("no-image", sections="Biography")], fetch)
        self.assertEqual(result[2], [["en", "en", "digest", 42], ["fr", "fr", "digest", 42]])
        self.assertEqual([c.args[0] for c in calls], ["en", "fr", "bad"])
        self.assertEqual(summary["coverage"]["q2_download_failed"], 1)

    def test_q3_and_q4_count_articles_and_resolve_top20_ties(self):
        words = [f"word{chr(97+i)}" for i in range(25)]
        result, summary, _ = self.reference([article("en", "shared shared " + " ".join(reversed(words))),
                                           article("fr", "shared exclusif", language="fr")])
        self.assertEqual(result[3], [["shared"]])
        self.assertEqual(result[4], [["shared", 2], ["exclusif", 1]] + [[word, 1] for word in words[:18]])
        self.assertEqual(summary["coverage"]["q4_tie_at_cutoff"], 1)
