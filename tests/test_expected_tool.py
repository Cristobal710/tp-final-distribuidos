import csv
import datetime
import gzip
import hashlib
import os
import tempfile
import json
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from compare import compare_dirs, from_client
from expected import ExpectedResults, iso_utc
from hashes import compare_digests, digests_of, format_digests, read_digests
from subset import read_list, select_articles, write_subset

STOP_WORDS = {"the", "of", "le", "la"}
IMAGE = {"content_url": "https://upload.wikimedia.org/x.jpg", "height": 1, "width": 1}


def article(i, language, created, abstract, sections="[]", image=IMAGE, modified=datetime.datetime(2024, 5, 1)):
    return {
        "name": f"Articulo {i}", "url": f"u{i}", "in_language": {"identifier": language},
        "date_created": created, "date_modified": modified, "abstract": abstract, "sections": sections, "image": image,
    }


def write_csv(directory, name, header, rows):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, name), "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)


def write_jsonl(directory, name, rows):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, name), "w", encoding="utf-8") as file:
        file.writelines(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)


CLIENT_RESULTS = {
    "q1.jsonl": [["u2", 1741767974123], ["u1", 0]],
    "q2.jsonl": [["Ada, condesa", "u1", "404f63", 88890]],
    "q3.jsonl": [["été"]],
    "q4.jsonl": [["sol", 3], ["luna", 2]],
    "q5.jsonl": [["u3", 12]],
}
EXPECTED_RESULTS = {
    "q1.csv": (["url", "date_modified"], [["u1", "1970-01-01T00:00:00+00:00"],
                                          ["u2", "2025-03-12T08:26:14.123000+00:00"]]),
    "q2.csv": (["name", "url", "image_url"], [["Ada, condesa", "u1", "https://img"]]),
    "q3.csv": (["word"], [["été"]]),
    "q4.csv": (["word", "count"], [["sol", "3"], ["luna", "2"]]),
    "q5.csv": (["url", "word_count"], [["u3", "12"]]),
}


def write_client_and_expected(expected_dir, actual_dir):
    for name, rows in CLIENT_RESULTS.items():
        write_jsonl(actual_dir, name, rows)
    for name, (header, rows) in EXPECTED_RESULTS.items():
        write_csv(expected_dir, name, header, rows)


def read_csv(directory, name):
    with open(os.path.join(directory, name), newline="", encoding="utf-8") as file:
        return list(csv.reader(file))[1:]


class ExpectedResultsTest(unittest.TestCase):

    def compute(self, *articles):
        self.directory = tempfile.mkdtemp()
        expected = ExpectedResults(self.directory, STOP_WORDS)
        for value in articles:
            expected.add(value)
        return expected.finish()

    def test_computes_every_query_with_the_rules_of_the_statement(self):
        average = self.compute(
            article(1, "en", datetime.datetime(2005, 1, 1), "The sun of the moon", '[{"name":"Biography"}]'),
            article(2, "fr", datetime.datetime(2019, 6, 1), "Le soleil et la lune sun", '[{"name":"Biographie"}]',
                    image=None),
            article(3, "en", datetime.datetime(2022, 1, 1), "sun moon star comet planet 2020"),
            article(4, "en", None, "sun moon"),
            article(5, "fr", datetime.datetime(2023, 1, 1), "lune"),
            article(6, "en", datetime.datetime(2010, 1, 1), None),
        )
        self.assertEqual(average, 3.0)
        self.assertEqual(read_csv(self.directory, "q1.csv"), [
            ["u1", "2024-05-01T00:00:00+00:00"], ["u6", "2024-05-01T00:00:00+00:00"],
        ])
        self.assertEqual(read_csv(self.directory, "q2.csv"), [["Articulo 1", "u1", IMAGE["content_url"]]])
        self.assertEqual(read_csv(self.directory, "q3.csv"), [["sun"]])
        self.assertEqual(read_csv(self.directory, "q4.csv")[:3], [["sun", "4"], ["moon", "3"], ["lune", "2"]])
        self.assertEqual(read_csv(self.directory, "q5.csv"), [["u3", "5"]])

    def test_q1_dates_of_the_client_are_read_like_the_expected_ones(self):
        for milliseconds in (0, 1609459199999, 1741767974000, 1741767974123):
            moment = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=milliseconds)
            self.assertEqual(iso_utc(moment), from_client(1, ["u", milliseconds])[1])


class CompareTest(unittest.TestCase):

    def setUp(self):
        self.expected = tempfile.mkdtemp()
        self.actual = tempfile.mkdtemp()

    def test_equal_results_in_any_order_are_ok(self):
        write_csv(self.expected, "q3.csv", ["word"], [["sol"], ["luna"]])
        write_csv(self.actual, "q3.csv", ["word"], [["luna"], ["sol"]])
        self.assertEqual(compare_dirs(self.expected, self.actual), (True, ["Q3: OK (2 filas)"]))

    def test_reports_missing_and_extra_rows(self):
        write_csv(self.expected, "q1.csv", ["url", "date_modified"], [["u1", "d"], ["u2", "d"]])
        write_csv(self.actual, "q1.csv", ["url", "date_modified"], [["u1", "d"], ["u3", "d"]])
        ok, report = compare_dirs(self.expected, self.actual)
        self.assertFalse(ok)
        self.assertIn("faltan 1 filas", report[1])
        self.assertIn("('u2', 'd')", report[1])
        self.assertIn("sobran 1 filas", report[2])

    def test_q4_has_to_be_in_the_same_order(self):
        write_csv(self.expected, "q4.csv", ["word", "count"], [["sol", "3"], ["luna", "2"]])
        write_csv(self.actual, "q4.csv", ["word", "count"], [["luna", "2"], ["sol", "3"]])
        self.assertFalse(compare_dirs(self.expected, self.actual)[0])

    def test_q2_only_compares_name_and_url(self):
        write_csv(self.expected, "q2.csv", ["name", "url", "image_url"], [["Ada", "u1", "https://img"]])
        write_csv(self.actual, "q2.csv", ["name", "url", "image"], [["Ada", "u1", "aGVsbG8="]])
        self.assertTrue(compare_dirs(self.expected, self.actual)[0])

    def test_reads_gzipped_expected_files(self):
        write_csv(self.expected, "q3.csv", ["word"], [["sol"], ["été"]])
        source = os.path.join(self.expected, "q3.csv")
        with open(source, "rb") as plain, gzip.open(source + ".gz", "wb") as packed:
            packed.write(plain.read())
        os.remove(source)
        write_csv(self.actual, "q3.csv", ["word"], [["été"], ["sol"]])
        self.assertEqual(compare_dirs(self.expected, self.actual), (True, ["Q3: OK (2 filas)"]))

    def test_reads_the_jsonl_results_of_the_client(self):
        write_client_and_expected(self.expected, self.actual)
        ok, report = compare_dirs(self.expected, self.actual)
        self.assertTrue(ok, report)
        self.assertEqual(len(report), 5)

    def test_a_query_the_system_did_not_answer_is_skipped(self):
        write_csv(self.expected, "q5.csv", ["url", "word_count"], [["u1", "9"]])
        ok, report = compare_dirs(self.expected, self.actual)
        self.assertTrue(ok)
        self.assertIn("se saltea", report[0])


class HashesTest(unittest.TestCase):

    def setUp(self):
        self.expected = tempfile.mkdtemp()
        self.actual = tempfile.mkdtemp()

    def test_same_rows_in_any_order_have_the_same_hash(self):
        write_csv(self.expected, "q3.csv", ["word"], [["sol"], ["luna"], ["été"]])
        write_csv(self.actual, "q3.csv", ["word"], [["été"], ["sol"], ["luna"]])
        self.assertEqual(digests_of(self.expected), digests_of(self.actual))
        self.assertEqual(digests_of(self.actual)[3][0], 3)

    def test_hash_is_the_one_of_the_sorted_file(self):
        write_csv(self.actual, "q1.csv", ["url", "date_modified"], [["u2", "d, 2"], ["u1", "d"]])
        with open(os.path.join(self.actual, "q1.csv"), "rb") as file:
            lines = sorted(file.read().splitlines(keepends=True)[1:])
        self.assertEqual(digests_of(self.actual)[1][1], hashlib.sha256(b"".join(lines)).hexdigest())

    def test_q4_order_changes_the_hash(self):
        write_csv(self.expected, "q4.csv", ["word", "count"], [["sol", "3"], ["luna", "2"]])
        write_csv(self.actual, "q4.csv", ["word", "count"], [["luna", "2"], ["sol", "3"]])
        self.assertNotEqual(digests_of(self.expected), digests_of(self.actual))

    def test_q2_hash_ignores_the_image(self):
        write_csv(self.expected, "q2.csv", ["name", "url", "image_url"], [["Ada, condesa", "u1", "https://img"]])
        write_csv(self.actual, "q2.csv", ["name", "url", "image"], [["Ada, condesa", "u1", "aGVsbG8="]])
        self.assertEqual(digests_of(self.expected), digests_of(self.actual))

    def test_gzipped_files_have_the_same_hash(self):
        write_csv(self.expected, "q3.csv", ["word"], [["sol"], ["été"]])
        source = os.path.join(self.expected, "q3.csv")
        with open(source, "rb") as plain, gzip.open(source + ".gz", "wb") as packed:
            packed.write(plain.read())
        plain_digests = digests_of(self.expected)
        os.remove(source)
        self.assertEqual(digests_of(self.expected), plain_digests)

    def test_jsonl_results_of_the_client_have_the_hashes_of_the_expected_csv(self):
        write_client_and_expected(self.expected, self.actual)
        self.assertEqual(digests_of(self.actual), digests_of(self.expected))

    def test_compares_against_the_saved_hashes(self):
        write_csv(self.expected, "q3.csv", ["word"], [["sol"]])
        write_csv(self.expected, "q5.csv", ["url", "word_count"], [["u1", "9"]])
        write_csv(self.actual, "q3.csv", ["word"], [["luna"]])
        path = os.path.join(self.expected, "hashes.txt")
        with open(path, "w", encoding="utf-8") as file:
            file.write(format_digests(digests_of(self.expected)))
        ok, report = compare_digests(read_digests(path), digests_of(self.actual))
        self.assertFalse(ok)
        self.assertEqual(report[0], "Q3: DIFIERE")
        self.assertIn("se saltea", report[3])


class SubsetTest(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.downloads = []
        self.files = {}
        sources = {
            "en.parquet": [article(1, "en", datetime.datetime(2005, 1, 1), "sun", '[{"name":"Biography"}]'),
                           article(2, "en", None, "moon")],
            "fr.parquet": [article(3, "fr", datetime.datetime(2023, 1, 1), "lune", image=None)],
        }
        for name, articles in sources.items():
            self.files[name] = os.path.join(self.directory, name)
            pq.write_table(pa.Table.from_pylist(articles), self.files[name])

    def download(self, file_path):
        self.downloads.append(file_path)
        return self.files[file_path]

    def test_reads_the_list_skipping_comments_and_blank_lines(self):
        path = os.path.join(self.directory, "articles.txt")
        with open(path, "w", encoding="utf-8") as file:
            file.write("# Q1\nen.parquet u1\n\n# Q5\nfr.parquet u3\n")
        self.assertEqual(read_list(path), [("en.parquet", "u1"), ("fr.parquet", "u3")])

    def test_selects_the_listed_articles_in_the_order_of_the_list(self):
        table = select_articles([("fr.parquet", "u3"), ("en.parquet", "u2"), ("en.parquet", "u1")], self.download)
        self.assertEqual(table["url"].to_pylist(), ["u3", "u2", "u1"])
        self.assertEqual(sorted(self.downloads), ["en.parquet", "fr.parquet"])

    def test_fails_if_an_article_is_not_in_its_file(self):
        with self.assertRaises(ValueError):
            select_articles([("en.parquet", "u1"), ("en.parquet", "u3")], self.download)

    def test_writes_the_parquet_and_a_readable_json(self):
        table = select_articles([("en.parquet", "u1"), ("fr.parquet", "u3")], self.download)
        write_subset(self.directory, table)
        self.assertEqual(pq.read_table(os.path.join(self.directory, "articles.parquet")).to_pylist(), table.to_pylist())
        with open(os.path.join(self.directory, "articles.json"), encoding="utf-8") as file:
            readable = json.load(file)
        self.assertEqual(readable[0]["sections"], [{"name": "Biography"}])
        self.assertEqual(readable[0]["date_created"], "2005-01-01T00:00:00")
        self.assertIsNone(readable[1]["image"])


if __name__ == "__main__":
    unittest.main()
