import contextlib
import io
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

from tests.full_suite.images import ImageHostRateLimited
from tests.full_suite.prepare import prepare
from tests.test_client import write_parquet


class FullSuitePrepareTest(unittest.TestCase):
    def test_retry_preserves_completed_language_and_original_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive_path, output = root / "source.zip", root / "dataset"
            with ZipFile(archive_path, "w") as archive:
                for language in ("en", "fr"):
                    source = root / f"{language}.parquet"
                    rows = [{"name": language, "url": language,
                        "date_created": datetime(2020, 1, 1), "abstract": "repeat repeat distinct",
                        "sections": "Biography", "in_language": {"identifier": language},
                        "image": {"content_url": f"https://images/{language}"}}]
                    if language == "fr":
                        rows.append(dict(rows[0], name="fr-ok", url="fr-ok", image={"content_url": "https://images/fr-ok"}))
                    write_parquet(source, rows)
                    archive.write(source, f"{language}wiki/data/part.parquet")
            snapshot = {"success": True, "sha256": "unit-test", "bytes": 1}
            with contextlib.redirect_stdout(io.StringIO()), patch("tests.full_suite.prepare.time.sleep"), \
                    patch("tests.full_suite.prepare.snapshot_image", side_effect=[snapshot, ImageHostRateLimited("cooldown")]):
                with self.assertRaises(ImageHostRateLimited):
                    prepare(archive_path, output, target_bytes=1024, images_per_language=1)
            checkpoint = json.loads((output / "checkpoint.json").read_text())
            self.assertEqual(checkpoint["coverage"]["en_rows"], 1)
            english = output / checkpoint["parts"][0]["file"]
            original = english.read_bytes()
            # A partial next-language file must be discarded without rewriting the completed language.
            (output / "part-0001-fr.parquet").write_bytes(b"interrupted")
            with contextlib.redirect_stdout(io.StringIO()), patch("tests.full_suite.prepare.time.sleep"), \
                    patch("tests.full_suite.prepare.snapshot_image", return_value=snapshot) as fetch:
                manifest = prepare(archive_path, output, target_bytes=1024, images_per_language=1)
            fetch.assert_called_once_with("https://images/fr-ok")
            self.assertEqual(english.read_bytes(), original)
            self.assertEqual(manifest["rows"], 2)
            self.assertFalse((output / "checkpoint.json").exists())
            provenance = [json.loads(line) for line in (output / "provenance.jsonl").read_text().splitlines()]
            self.assertEqual([entry["rows"] for entry in provenance], [[0], [1]])
            with contextlib.redirect_stdout(io.StringIO()), patch("tests.full_suite.prepare.snapshot_image") as fetch:
                self.assertEqual(prepare(archive_path, output, target_bytes=1024, images_per_language=1), manifest)
                fetch.assert_not_called()
            english.write_bytes(b"damaged")
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                prepare(archive_path, output, target_bytes=1024, images_per_language=1)
