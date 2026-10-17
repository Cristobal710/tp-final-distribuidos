import os
import socket
import tempfile
import threading
import unittest
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

import requests
from kagglehub.exceptions import KaggleApiHTTPError

from common.protocol import articles, external
from common.protocol.articles import Article
from dataset import KaggleDataset, read_rows
from sender import Client

IMAGE = pa.struct([("content_url", pa.string()), ("height", pa.int64()), ("width", pa.int64())])
LANGUAGE = pa.struct([("identifier", pa.string())])
SCHEMA = pa.schema([
    ("abstract", pa.string()),
    ("date_created", pa.timestamp("ms")),
    ("date_modified", pa.timestamp("ms")),
    ("image", IMAGE),
    ("in_language", LANGUAGE),
    ("name", pa.string()),
    ("references", pa.string()),
    ("sections", pa.string()),
    ("url", pa.string()),
])


def ms(year, month=1, day=1):
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


def write_parquet(path, rows):
    columns = {name: [row.get(name) for row in rows] for name in SCHEMA.names}
    pq.write_table(pa.table(columns, schema=SCHEMA), path, row_group_size=3)


ROWS = [
    {"name": "Ada", "url": "u1", "date_created": datetime(2001, 12, 10), "date_modified": datetime(2024, 9, 1),
     "abstract": "Ada fue una matemática", "sections": "[]", "references": "x" * 1000,
     "in_language": {"identifier": "en"},
     "image": {"content_url": "https://upload.wikimedia.org/ada.jpg", "height": 10, "width": 10}},
    {"name": "Sin fecha de creación", "url": "u2", "date_created": None, "date_modified": datetime(2022, 9, 3),
     "abstract": None, "sections": None, "image": None, "in_language": None},
    {"name": "Château", "url": "u3", "date_created": datetime(2021, 1, 1), "date_modified": datetime(2021, 1, 2),
     "abstract": "L'homme", "sections": '[{"name":"Biographie"}]', "in_language": {"identifier": "fr"},
     "image": {"content_url": None, "height": None, "width": None}},
    {"name": "Cuarto", "url": "u4", "date_created": datetime(2020, 12, 31), "date_modified": datetime(2025, 1, 1),
     "abstract": "a b c", "sections": "[]", "image": None, "in_language": {"identifier": "en"}},
]

EXPECTED = [
    Article("Ada", "u1", {"identifier": "en"}, ms(2001, 12, 10), ms(2024, 9, 1), "Ada fue una matemática", "[]",
            {"content_url": "https://upload.wikimedia.org/ada.jpg", "height": 10, "width": 10}),
    Article("Sin fecha de creación", "u2", None, None, ms(2022, 9, 3), None, None, None),
    Article("Château", "u3", {"identifier": "fr"}, ms(2021), ms(2021, 1, 2), "L'homme", '[{"name":"Biographie"}]',
            {"content_url": None, "height": None, "width": None}),
    Article("Cuarto", "u4", {"identifier": "en"}, ms(2020, 12, 31), ms(2025), "a b c", "[]", None),
]


PATTERNS = ["enwiki/data/enwiki_namespace_0_{:05}.parquet", "frwiki/data/frwiki_namespace_0_{:05}.parquet"]


def kaggle(tmp, download, patterns=PATTERNS, files_per_pattern=0):
    return KaggleDataset(tmp, "wikimedia-foundation/wikipedia-structured-contents", patterns, files_per_pattern,
                         files_ahead=1, poll_seconds=0.05, stop_timeout_seconds=5, download=download)


def parquet_files(directory):
    return sorted(f for _, _, files in os.walk(directory) for f in files if f.endswith(".parquet"))


class FakeKaggle:

    def __init__(self, existing):
        self.existing = existing
        self.requests = []

    def download(self, dataset, path, output_dir):
        self.requests.append((dataset, path, output_dir))
        if path not in self.existing:
            raise not_found()
        local_path = os.path.join(output_dir, *path.split("/"))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(b"parquet")
        return local_path


def not_found():
    response = requests.Response()
    response.status_code = 404
    return KaggleApiHTTPError("404 Client Error", response=response)


class ReaderTest(unittest.TestCase):

    def test_reads_only_the_requested_columns_without_transforming_them(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.parquet")
            write_parquet(path, ROWS)
            rows = list(read_rows(path, columns=["url", "image", "date_created"], rows_per_read=2))
            self.assertEqual([set(r) for r in rows], [{"url", "image", "date_created"}] * 4)
            self.assertEqual(rows[0]["date_created"], datetime(2001, 12, 10))
            self.assertEqual(rows[0]["image"], ROWS[0]["image"])

    def test_reads_every_column_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.parquet")
            write_parquet(path, ROWS)
            rows = list(read_rows(path))
            self.assertEqual(set(rows[0]), set(SCHEMA.names))
            self.assertEqual(rows[0]["references"], "x" * 1000)

    def test_downloads_each_file_like_the_notebook(self):
        existing = {"enwiki/data/enwiki_namespace_0_00000.parquet", "enwiki/data/enwiki_namespace_0_00001.parquet",
                    "enwiki/data/enwiki_namespace_0_00002.parquet", "frwiki/data/frwiki_namespace_0_00000.parquet"}
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeKaggle(existing)
            self.assertEqual([p for p, _ in kaggle(tmp, fake.download, files_per_pattern=2).files()], [
                "enwiki/data/enwiki_namespace_0_00000.parquet",
                "enwiki/data/enwiki_namespace_0_00001.parquet",
                "frwiki/data/frwiki_namespace_0_00000.parquet",
            ])
            self.assertTrue(all(d == "wikimedia-foundation/wikipedia-structured-contents" for d, _, _ in fake.requests))
            self.assertTrue(all(out == tmp for _, _, out in fake.requests))
            self.assertEqual([p for p, _ in kaggle(tmp, fake.download).files()], sorted(existing))

    def test_each_file_is_deleted_after_sending_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeKaggle({f"enwiki/data/enwiki_namespace_0_{i:05}.parquet" for i in range(4)})
            seen = []
            for _, local_path in kaggle(tmp, fake.download, patterns=PATTERNS[:1]).files():
                self.assertTrue(os.path.exists(local_path))
                self.assertLessEqual(len(parquet_files(tmp)), 3)
                seen.append(local_path)
            self.assertEqual(len(seen), 4)
            self.assertEqual(parquet_files(tmp), [])

    def test_stopping_early_deletes_what_was_downloaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeKaggle({f"enwiki/data/enwiki_namespace_0_{i:05}.parquet" for i in range(10)})
            files = kaggle(tmp, fake.download, patterns=PATTERNS[:1]).files()
            next(files)
            files.close()
            self.assertEqual(parquet_files(tmp), [])
            self.assertLess(len(fake.requests), 10)

    def test_other_errors_are_not_hidden(self):
        def download(dataset, path, output_dir):
            response = requests.Response()
            response.status_code = 500
            raise KaggleApiHTTPError("500 Server Error", response=response)

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(KaggleApiHTTPError):
                list(kaggle(tmp, download).files())


class FakeGateway(threading.Thread):

    def __init__(self):
        super().__init__(daemon=True)
        self.server = socket.create_server(("127.0.0.1", 0))
        self.port = self.server.getsockname()[1]
        self.messages = []

    def run(self):
        conn, _ = self.server.accept()
        with conn:
            while True:
                msg_type, payload = external.receive_message(conn)
                self.messages.append((msg_type, payload))
                if msg_type == external.MsgType.END_OF_DATA:
                    return


class ClientTest(unittest.TestCase):

    def send(self, rows, max_batch_rows=50, max_batch_bytes=4000):
        gateway = FakeGateway()
        gateway.start()
        client = Client("127.0.0.1", gateway.port, max_batch_rows, max_batch_bytes, connect_attempts=1,
                        connect_retry_seconds=0)
        client.connect()
        client.send_rows(iter(rows))
        client.close()
        gateway.join(timeout=10)
        gateway.server.close()
        types = [t for t, _ in gateway.messages]
        self.assertEqual(types[-1], external.MsgType.END_OF_DATA)
        self.assertTrue(all(t == external.MsgType.ARTICLES for t in types[:-1]))
        return client, [p for _, p in gateway.messages[:-1]]

    def test_sends_every_column_and_the_gateway_keeps_the_needed_ones(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.parquet")
            write_parquet(path, ROWS)
            client, batches = self.send(list(read_rows(path)))
        self.assertIn("references", batches[0]["columns"])
        self.assertEqual([a for b in batches for a in articles.decode_batch(b)], EXPECTED)

    def test_sends_every_row_in_batches(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.parquet")
            write_parquet(path, ROWS)
            rows = list(read_rows(path, columns=list(Article._fields)))
        many = [dict(rows[i % 4], url=f"u{i}") for i in range(1000)]
        client, batches = self.send(many)
        decoded = [articles.decode_batch(b) for b in batches]
        self.assertEqual([a.url for b in decoded for a in b], [f"u{i}" for i in range(1000)])
        self.assertTrue(all(1 <= len(b) <= 50 for b in decoded))
        self.assertEqual(client.sent_rows, 1000)
        self.assertEqual(client.sent_batches, len(batches))

    def test_closes_the_batch_when_the_columns_change(self):
        first = [{"name": "a", "url": "u1", "extra": 1}] * 3
        second = [{"url": "u2", "name": "b"}] * 2
        client, batches = self.send(first + second, max_batch_rows=10, max_batch_bytes=10_000)
        self.assertEqual([b["columns"] for b in batches], [["name", "url", "extra"], ["url", "name"]])
        self.assertEqual([len(b["rows"]) for b in batches], [3, 2])


if __name__ == "__main__":
    unittest.main()
