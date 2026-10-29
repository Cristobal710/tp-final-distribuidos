"""Independent streaming oracle; no production query/processing code is imported."""
import argparse
import hashlib
import json
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from tests.full_suite.images import snapshot_image

BOUNDARY = datetime(2021, 1, 1)
EPOCH = datetime(1970, 1, 1)


def canonical(row):
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"))


def milliseconds(value):
    delta = value - EPOCH
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def add_expected(connection, query, rows):
    counts = Counter(canonical(row) for row in rows)
    connection.executemany(
        "INSERT INTO expected VALUES(?,?,?) ON CONFLICT(query,row) DO UPDATE SET n=n+excluded.n",
        ((query, row, count) for row, count in counts.items()),
    )


def calculate(dataset, output, stopwords_path, require_coverage=True):
    dataset, output = Path(dataset), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((dataset / "manifest.json").read_text())
    stopwords = set(json.loads(Path(stopwords_path).read_text()))
    db_path = output / "expected.sqlite"
    for suffix in ("", "-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)
    db = sqlite3.connect(db_path)
    db.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA cache_size=-131072;
        CREATE TABLE expected(query INTEGER, row TEXT, n INTEGER, PRIMARY KEY(query,row)) WITHOUT ROWID;
        CREATE TABLE words(word TEXT PRIMARY KEY, articles INTEGER, en INTEGER, fr INTEGER) WITHOUT ROWID;
        CREATE TABLE candidates(url TEXT, words INTEGER);
        CREATE TABLE images(name TEXT, url TEXT, image_url TEXT, language TEXT);
    """)
    historical_words = historical_articles = total_rows = 0
    coverage = Counter()
    examples = {}

    def record(name, url):
        coverage[name] += 1
        sample = examples.setdefault(name, [])
        if len(sample) < 3 and url not in sample:
            sample.append(url)

    try:
        for part in manifest["parts"]:
            parquet = pq.ParquetFile(dataset / part["file"])
            for batch in parquet.iter_batches(batch_size=1024):
                q1, candidates, images, words = [], [], [], {}
                for row in batch.to_pylist():
                    total_rows += 1
                    created = row["date_created"]
                    historical = created is not None and created < BOUNDARY
                    recent = created is not None and created >= BOUNDARY
                    language = (row["in_language"] or {}).get("identifier")
                    if created is None:
                        record("missing_creation_date", row["url"])
                    elif created == BOUNDARY:
                        record("exact_2021_boundary", row["url"])
                    if historical and language == "en" and row["date_modified"] is not None:
                        q1.append([row["url"], milliseconds(row["date_modified"])])
                    if historical and language == "fr":
                        record("q1_excluded_language", row["url"])
                    if recent and language == "en":
                        record("q1_excluded_recent", row["url"])
                    if historical and language == "en" and row["date_modified"] is None:
                        record("q1_missing_modified_date", row["url"])
                    sections = row["sections"]
                    image_url = (row["image"] or {}).get("content_url")
                    biography = isinstance(sections, str) and any(
                        marker in sections.casefold() for marker in ("biography", "biographie"))
                    if historical and biography and image_url is not None:
                        images.append((row["name"], row["url"], image_url, language))
                        record("q2_biography" if "biography" in sections.casefold() else "q2_biographie", row["url"])
                        if not any(marker in sections for marker in ("biography", "biographie")):
                            record("q2_case_insensitive", row["url"])
                    if historical and sections is not None and image_url is not None and not biography:
                        record("q2_no_biography_marker", row["url"])
                    if historical and sections is None:
                        record("q2_missing_sections", row["url"])
                    if historical and biography and image_url is None:
                        record("q2_missing_image", row["url"])
                    if not historical and biography and image_url is not None:
                        record("q2_excluded_date", row["url"])
                    if row["abstract"] is None:
                        record("missing_abstract", row["url"])
                        continue
                    if row["abstract"] == "":
                        record("empty_abstract", row["url"])
                    tokens = re.findall(r"\b\w+\b", row["abstract"].lower())
                    cleaned = [token for token in tokens if token not in stopwords and not token.isdecimal()]
                    distinct = set(cleaned)
                    if len(cleaned) > len(distinct):
                        record("repeated_words", row["url"])
                    if any(token in stopwords for token in tokens):
                        record("stopwords_removed", row["url"])
                    if any(token.isdecimal() for token in tokens):
                        record("numbers_removed", row["url"])
                    if not distinct:
                        record("cleaned_empty_abstract", row["url"])
                    if historical and not distinct:
                        record("q5_zero_word_historical", row["url"])
                    for word in distinct:
                        entry = words.setdefault(word, [0, 0, 0])
                        entry[0] += 1
                        entry[1] |= language == "en"
                        entry[2] |= language == "fr"
                    if historical:
                        historical_words += len(distinct)
                        historical_articles += 1
                    elif recent:
                        candidates.append((row["url"], len(distinct)))
                add_expected(db, 1, q1)
                db.executemany("INSERT INTO candidates VALUES(?,?)", candidates)
                db.executemany("INSERT INTO images VALUES(?,?,?,?)", images)
                db.executemany("""INSERT INTO words VALUES(?,?,?,?) ON CONFLICT(word) DO UPDATE SET
                    articles=articles+excluded.articles,en=max(en,excluded.en),fr=max(fr,excluded.fr)""",
                               ((word, *counts) for word, counts in words.items()))
                db.commit()
            parquet.close()
            print(f"Reference: {total_rows:,}/{manifest['rows']:,} rows", flush=True)
        if total_rows != manifest["rows"]:
            raise AssertionError("Partition row count does not match manifest")
        shared = db.execute("SELECT word FROM words WHERE en=1 AND fr=1")
        while rows := shared.fetchmany(4096):
            add_expected(db, 3, rows)
        ranked = [list(row) for row in db.execute("SELECT word,articles FROM words ORDER BY articles DESC,word COLLATE BINARY LIMIT 21")]
        top = ranked[:20]
        coverage["q4_tied_counts_in_top20"] = sum(a[1] == b[1] for a, b in zip(top, top[1:]))
        coverage["q4_tie_at_cutoff"] = int(len(ranked) == 21 and ranked[19][1] == ranked[20][1])
        add_expected(db, 4, top)
        (output / "q4_ordered.json").write_text(json.dumps(top, ensure_ascii=False, indent=2))
        candidates = db.execute("SELECT url,words FROM candidates")
        while rows := candidates.fetchmany(4096):
            matched = []
            for url, count in rows:
                matches = historical_articles > 0 and count * historical_articles > historical_words
                coverage["q5_above" if matches else "q5_not_above"] += 1
                relation = ("no_historical_baseline" if historical_articles == 0 else
                            "equal_average" if count * historical_articles == historical_words else
                            "below_average" if count * historical_articles < historical_words else "above_average")
                record(f"q5_{relation}", url)
                if matches:
                    matched.append([url, count])
            add_expected(db, 5, matched)
        snapshots = {}
        for name, url, image_url, language in db.execute("SELECT name,url,image_url,language FROM images"):
            if image_url not in snapshots:
                snapshots[image_url] = snapshot_image(image_url)
                print(f"Reference live image {len(snapshots)}: {snapshots[image_url]['success']}", flush=True)
            image = snapshots[image_url]
            if image["success"]:
                add_expected(db, 2, [[name, url, image["sha256"], image["bytes"]]])
                coverage[f"q2_{language}_success"] += 1
            else:
                record("q2_download_failed", url)
        for query in range(1, 6):
            coverage[f"q{query}_rows"] = db.execute("SELECT coalesce(sum(n),0) FROM expected WHERE query=?", (query,)).fetchone()[0]
            if require_coverage and coverage[f"q{query}_rows"] == 0:
                raise AssertionError(f"Partition gives no positive coverage for Q{query}")
        for language in ("en", "fr"):
            if require_coverage and coverage[f"q2_{language}_success"] < manifest["images_per_language"]:
                raise AssertionError(f"Insufficient live Q2 success coverage for {language}")
        if require_coverage and (coverage["q5_not_above"] == 0 or len(top) != 20):
            raise AssertionError("Missing negative Q5 or top-20 coverage")
        for language in ("en", "fr"):
            exclusive = db.execute(f"SELECT count(*) FROM words WHERE {language}=1 AND {'fr' if language=='en' else 'en'}=0").fetchone()[0]
            coverage[f"q3_{language}_exclusive"] = exclusive
            if require_coverage and exclusive == 0:
                raise AssertionError(f"No exclusive {language} words for Q3 negative coverage")
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        for query in range(1, 6):
            with (output / f"q{query}.jsonl").open("w") as target:
                if query == 4:
                    for row in top:
                        target.write(canonical(row) + "\n")
                else:
                    for row, count in db.execute("SELECT row,n FROM expected WHERE query=? ORDER BY row", (query,)):
                        for _ in range(count):
                            target.write(row + "\n")
        summary = {"rows": total_rows, "historical_words": historical_words, "historical_articles": historical_articles,
                   "partition_manifest_sha256": hashlib.sha256((dataset / "manifest.json").read_bytes()).hexdigest(),
                   "stopwords_sha256": hashlib.sha256(Path(stopwords_path).read_bytes()).hexdigest(), "coverage": dict(coverage)}
        (output / "images.json").write_text(json.dumps(snapshots, indent=2))
        (output / "summary.json").write_text(json.dumps(summary, indent=2))
        scenarios = ["missing_creation_date", "exact_2021_boundary", "q1_excluded_language", "q1_excluded_recent",
            "q1_missing_modified_date", "q2_biography", "q2_biographie", "q2_case_insensitive", "q2_no_biography_marker",
            "q2_missing_sections", "q2_missing_image", "q2_excluded_date", "q2_download_failed", "missing_abstract",
            "empty_abstract", "cleaned_empty_abstract", "repeated_words", "numbers_removed", "stopwords_removed",
            "q4_tied_counts_in_top20", "q4_tie_at_cutoff", "q5_zero_word_historical", "q5_equal_average",
            "q5_below_average", "q5_above_average", "q5_no_historical_baseline"]
        (output / "scenarios.json").write_text(json.dumps({name: {
            "observed": coverage[name], "examples": examples.get(name, []),
            "status": "covered" if coverage[name] else "absent_from_this_partition",
        } for name in scenarios}, ensure_ascii=False, indent=2))
        print(json.dumps(summary), flush=True)
        return summary
    finally:
        db.close()


def validate(dataset, expected, actual, output):
    dataset, expected, actual, output = map(Path, (dataset, expected, actual, output))
    manifest = json.loads((dataset / "manifest.json").read_text())
    reference_summary = json.loads((expected / "summary.json").read_text())
    if reference_summary["partition_manifest_sha256"] != hashlib.sha256((dataset / "manifest.json").read_bytes()).hexdigest():
        raise AssertionError("Reference belongs to a different partition manifest")
    summary = json.loads((actual / "summary.json").read_text())
    if summary["sent_rows"] != manifest["rows"] or not summary["all_done"] or summary["finished_queries"] != [1, 2, 3, 4, 5]:
        raise AssertionError("Client input or completion summary does not match the partition")
    db_path = output / "comparison.sqlite"
    db_path.unlink(missing_ok=True)
    db = sqlite3.connect(db_path)
    report = {"passed": True, "partition_bytes": manifest["bytes"], "input_rows": manifest["rows"], "queries": {}}
    try:
        db.execute("CREATE TABLE actual(query INTEGER,row TEXT,n INTEGER,PRIMARY KEY(query,row)) WITHOUT ROWID")
        db.execute("ATTACH DATABASE ? AS reference", (str(expected / "expected.sqlite"),))
        for query in range(1, 6):
            count, pending, ordered = 0, Counter(), []
            with open(actual / f"q{query}.jsonl") as source:
                for line in source:
                    row = json.loads(line)
                    if query == 2:
                        name, url, digest, size = row
                        data = (actual / "images" / f"{digest}.bin").read_bytes()
                        if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                            raise AssertionError(f"Corrupt received image: {url}")
                    if query == 4:
                        ordered.append(row)
                    pending[canonical(row)] += 1
                    count += 1
                    if len(pending) >= 4096:
                        db.executemany("INSERT INTO actual VALUES(?,?,?) ON CONFLICT(query,row) DO UPDATE SET n=n+excluded.n",
                                       ((query, row, n) for row, n in pending.items()))
                        pending.clear()
            db.executemany("INSERT INTO actual VALUES(?,?,?) ON CONFLICT(query,row) DO UPDATE SET n=n+excluded.n",
                           ((query, row, n) for row, n in pending.items()))
            if count != summary["received_rows"][str(query)]:
                raise AssertionError(f"Receiver row count mismatch for Q{query}")
            missing = list(db.execute("""SELECT e.row,e.n-coalesce(a.n,0) FROM reference.expected e
                LEFT JOIN actual a ON a.query=e.query AND a.row=e.row
                WHERE e.query=? AND e.n>coalesce(a.n,0) LIMIT 10""", (query,)))
            extra = list(db.execute("""SELECT a.row,a.n-coalesce(e.n,0) FROM actual a
                LEFT JOIN reference.expected e ON a.query=e.query AND a.row=e.row
                WHERE a.query=? AND a.n>coalesce(e.n,0) LIMIT 10""", (query,)))
            order_ok = query != 4 or ordered == json.loads((expected / "q4_ordered.json").read_text())
            passed = not missing and not extra and order_ok
            report["queries"][str(query)] = {"passed": passed, "actual_rows": count,
                "expected_rows": db.execute("SELECT sum(n) FROM reference.expected WHERE query=?", (query,)).fetchone()[0],
                "missing_examples": missing, "unexpected_examples": extra, "order_correct": order_ok}
            report["passed"] &= passed
        db.commit()
        (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps(report, ensure_ascii=False), flush=True)
        if not report["passed"]:
            raise AssertionError("Full-suite output differs from the independent reference; see report.json")
        return report
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("calculate", "validate"))
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stopwords")
    parser.add_argument("--expected")
    parser.add_argument("--actual")
    args = parser.parse_args()
    if args.mode == "calculate":
        calculate(args.dataset, args.output, args.stopwords)
    else:
        validate(args.dataset, args.expected, args.actual, args.output)
