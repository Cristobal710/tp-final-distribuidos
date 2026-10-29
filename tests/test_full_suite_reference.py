import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from dataset.local import local_rows
from tests.full_suite.reference import calculate, validate
from tests.test_client import write_parquet


class FullSuiteReferenceTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.dataset, self.expected, self.actual = [self.root / name for name in ("dataset", "expected", "actual")]
        self.dataset.mkdir()
        self.actual.mkdir()
        old, recent = datetime(2020, 1, 1), datetime(2021, 1, 1)
        words = " ".join(f"word{chr(97+i)}" for i in range(20))
        terms = " ".join(f"term{chr(97+i)}" for i in range(20))
        rows = []
        for url, language, date, abstract, sections in (
                ("old-en", "en", old, f"the shared repeat repeat olden 123 {words}", "Biography"),
                ("old-fr", "fr", old, "le shared ancien", "BIOGRAPHIE"),
                ("new-high", "en", recent, f"shared {terms}", "Biography"),
                ("new-low", "fr", recent, "petit petit petit", "None"),
                ("unknown", "en", None, None, "Biography")):
            rows.append({"url": url, "name": url, "in_language": {"identifier": language},
                         "date_created": date, "date_modified": datetime(1970, 1, 1, 0, 0, 1),
                         "abstract": abstract, "sections": sections, "image": {"content_url": "https://image/" + url}})
        path = self.dataset / "part.parquet"
        write_parquet(path, rows)
        self.manifest = {"parts": [{"file": path.name}], "rows": 5, "bytes": path.stat().st_size,
                         "images_per_language": 1}
        (self.dataset / "manifest.json").write_text(json.dumps(self.manifest))
        stopwords = self.root / "stopwords.json"
        stopwords.write_text(json.dumps(["the", "le"]))
        image = b"live bytes replaced only in this unit test"
        self.image = image
        snapshot = {"success": True, "bytes": len(image), "sha256": hashlib.sha256(image).hexdigest()}
        with patch("tests.full_suite.reference.snapshot_image", return_value=snapshot):
            self.summary = calculate(self.dataset, self.expected, stopwords)

    def make_actual(self):
        counts = {}
        with sqlite3.connect(self.expected / "expected.sqlite") as db:
            for query in range(1, 6):
                rows = [json.loads(row) for row, n in db.execute(
                    "SELECT row,n FROM expected WHERE query=? ORDER BY row", (query,)) for _ in range(n)]
                if query == 4:
                    rows = json.loads((self.expected / "q4_ordered.json").read_text())
                (self.actual / f"q{query}.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
                counts[str(query)] = len(rows)
        images = self.actual / "images"
        images.mkdir(exist_ok=True)
        (images / f"{hashlib.sha256(self.image).hexdigest()}.bin").write_bytes(self.image)
        (self.actual / "summary.json").write_text(json.dumps({"sent_rows": 5, "all_done": True,
            "finished_queries": [1, 2, 3, 4, 5], "received_rows": counts}))

    def test_independent_distinct_counts_date_boundary_and_live_bytes(self):
        self.assertEqual((self.summary["historical_words"], self.summary["historical_articles"]), (25, 2))
        with sqlite3.connect(self.expected / "expected.sqlite") as db:
            self.assertEqual(json.loads(db.execute("SELECT row FROM expected WHERE query=5").fetchone()[0]),
                             ["new-high", 21])
            self.assertEqual(json.loads(db.execute("SELECT row FROM expected WHERE query=3").fetchone()[0]), ["shared"])
            self.assertEqual(json.loads(db.execute("SELECT row FROM expected WHERE query=1").fetchone()[0]), ["old-en", 1000])
        self.make_actual()
        self.assertTrue(validate(self.dataset, self.expected, self.actual, self.root)["passed"])

    def test_validator_rejects_duplicate_result_even_with_consistent_summary(self):
        self.make_actual()
        path = self.actual / "q1.jsonl"
        path.write_text(path.read_text() * 2)
        summary_path = self.actual / "summary.json"
        summary = json.loads(summary_path.read_text())
        summary["received_rows"]["1"] = 2
        summary_path.write_text(json.dumps(summary))
        with self.assertRaises(AssertionError):
            validate(self.dataset, self.expected, self.actual, self.root)

    def test_validator_checks_q4_order_and_received_image_content(self):
        self.make_actual()
        path = self.actual / "q4.jsonl"
        path.write_text("\n".join(reversed(path.read_text().splitlines())) + "\n")
        with self.assertRaises(AssertionError):
            validate(self.dataset, self.expected, self.actual, self.root)
        self.make_actual()
        next((self.actual / "images").iterdir()).write_bytes(b"corrupt")
        with self.assertRaisesRegex(AssertionError, "Corrupt received image"):
            validate(self.dataset, self.expected, self.actual, self.root)

    def test_local_input_is_preserved_and_manifest_cannot_escape(self):
        path = self.dataset / "part.parquet"
        before = path.read_bytes()
        self.assertEqual(len(list(local_rows(self.dataset, rows_per_read=1))), 5)
        self.assertEqual(path.read_bytes(), before)
        self.manifest["parts"][0]["file"] = "../elsewhere.parquet"
        (self.dataset / "manifest.json").write_text(json.dumps(self.manifest))
        with self.assertRaises(ValueError):
            list(local_rows(self.dataset))
