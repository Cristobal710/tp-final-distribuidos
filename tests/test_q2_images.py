import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from tests.q2_images import cached_image_path, validate_images


class Q2ImageValidationTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.actual = self.root / "actual"
        self.images = self.actual / "images"
        self.images.mkdir(parents=True)
        self.cache = self.root / "cache"
        self.cache.mkdir()
        self.expected = self.root / "q2.csv"
        self.article_url = "https://wiki/person"
        self.image_url = "https://images/person.png"
        with self.expected.open("w", newline="") as source:
            writer = csv.writer(source)
            writer.writerow(["name", "url", "image_url"])
            writer.writerow(["Person", self.article_url, self.image_url])

    def save_image(self, data):
        digest = hashlib.sha256(data).hexdigest()
        received = self.images / f"{digest}.bin"
        received.write_bytes(data)
        cached = cached_image_path(self.cache, self.image_url)
        cached.write_bytes(data)
        row = ["Person", self.article_url, digest, len(data)]
        (self.actual / "q2.jsonl").write_text(json.dumps(row) + "\n")
        return received, cached, row

    def test_raw_file_cache_matches_large_received_image(self):
        self.save_image(b"image bytes" * 104858)
        self.assertEqual(validate_images(self.actual, self.expected, self.cache),
                         {"passed": True, "images_checked": 1, "problems": []})

    def test_corrupt_or_missing_cache_file_fails(self):
        _, cached, _ = self.save_image(b"original")
        cached.write_bytes(b"corrupt")
        self.assertFalse(validate_images(self.actual, self.expected, self.cache)["passed"])
        cached.unlink()
        self.assertFalse(validate_images(self.actual, self.expected, self.cache)["passed"])

    def test_disabled_cache_still_checks_received_image_integrity(self):
        received, cached, _ = self.save_image(b"original")
        cached.unlink()
        self.assertTrue(validate_images(self.actual, self.expected)["passed"])
        received.write_bytes(b"corrupt")
        self.assertFalse(validate_images(self.actual, self.expected)["passed"])

    def test_wrong_size_or_unknown_article_fails(self):
        _, _, row = self.save_image(b"original")
        row[3] += 1
        (self.actual / "q2.jsonl").write_text(json.dumps(row) + "\n")
        self.assertFalse(validate_images(self.actual, self.expected, self.cache)["passed"])
        row[3] -= 1
        row[1] = "unknown article"
        (self.actual / "q2.jsonl").write_text(json.dumps(row) + "\n")
        self.assertFalse(validate_images(self.actual, self.expected, self.cache)["passed"])

    def test_missing_or_malformed_result_file_fails(self):
        self.assertFalse(validate_images(self.actual, self.expected, self.cache)["passed"])
        (self.actual / "q2.jsonl").write_text("invalid JSON\n")
        self.assertFalse(validate_images(self.actual, self.expected, self.cache)["passed"])
