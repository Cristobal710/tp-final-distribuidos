"""Independent streaming oracle; no production query/processing code is imported."""
import argparse
import hashlib
import heapq
import json
import re
from collections import Counter
from contextlib import ExitStack
from datetime import datetime
from itertools import groupby
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from tests.full_suite.images import snapshot_image
from tests.full_suite.files import compare_files, sort_file

BOUNDARY = datetime(2021, 1, 1)
EPOCH = datetime(1970, 1, 1)


def canonical(row):
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"))


def milliseconds(value):
    delta = value - EPOCH
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def add_expected(outputs, counts, query, rows):
    for row in rows:
        outputs[query].write(canonical(row) + "\n")
        counts[query] += 1


def word_counts(source, directory):
    target = directory / "words-sorted.jsonl"
    sort_file(source, target)
    with target.open(encoding="utf-8") as input_file:
        entries = (json.loads(line) for line in input_file)
        for word, group in groupby(entries, key=lambda row: row[0]):
            articles, en, fr = 0, False, False
            for _, count, english, french in group:
                articles += count
                en |= bool(english)
                fr |= bool(french)
            yield word, articles, en, fr


def calculate(dataset, output, stopwords_path, require_coverage=True):
    dataset, output = Path(dataset), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((dataset / "manifest.json").read_text())
    stopwords = set(json.loads(Path(stopwords_path).read_text()))
    historical_words = historical_articles = total_rows = 0
    coverage = Counter()
    counts = Counter()
    examples = {}

    def record(name, url):
        coverage[name] += 1
        sample = examples.setdefault(name, [])
        if len(sample) < 3 and url not in sample:
            sample.append(url)

    with TemporaryDirectory(prefix=".reference-", dir=output) as directory, ExitStack() as stack:
        directory = Path(directory)
        outputs = {query: stack.enter_context((directory / f"q{query}.jsonl").open("w", encoding="utf-8"))
                   for query in range(1, 6)}
        words_file = stack.enter_context((directory / "words.jsonl").open("w", encoding="utf-8"))
        candidates_file = stack.enter_context((directory / "candidates.jsonl").open("w", encoding="utf-8"))
        images_file = stack.enter_context((directory / "images.jsonl").open("w", encoding="utf-8"))
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
                add_expected(outputs, counts, 1, q1)
                candidates_file.writelines(canonical(row) + "\n" for row in candidates)
                images_file.writelines(canonical(row) + "\n" for row in images)
                words_file.writelines(canonical([word, *values]) + "\n" for word, values in words.items())
            parquet.close()
            print(f"Reference: {total_rows:,}/{manifest['rows']:,} rows", flush=True)
        if total_rows != manifest["rows"]:
            raise AssertionError("Partition row count does not match manifest")
        words_file.close()
        candidates_file.close()
        images_file.close()
        coverage["q3_en_exclusive"] = coverage["q3_fr_exclusive"] = 0

        def ranked_words():
            for word, articles, en, fr in word_counts(directory / "words.jsonl", directory):
                if en and fr:
                    add_expected(outputs, counts, 3, [[word]])
                elif en:
                    coverage["q3_en_exclusive"] += 1
                elif fr:
                    coverage["q3_fr_exclusive"] += 1
                yield word, articles

        ranked = [list(row) for row in heapq.nsmallest(21, ranked_words(), key=lambda row: (-row[1], row[0]))]
        top = ranked[:20]
        coverage["q4_tied_counts_in_top20"] = sum(a[1] == b[1] for a, b in zip(top, top[1:]))
        coverage["q4_tie_at_cutoff"] = int(len(ranked) == 21 and ranked[19][1] == ranked[20][1])
        add_expected(outputs, counts, 4, top)
        (output / "q4_ordered.json").write_text(json.dumps(top, ensure_ascii=False, indent=2))
        with (directory / "candidates.jsonl").open(encoding="utf-8") as source:
            for line in source:
                url, count = json.loads(line)
                matches = historical_articles > 0 and count * historical_articles > historical_words
                coverage["q5_above" if matches else "q5_not_above"] += 1
                relation = ("no_historical_baseline" if historical_articles == 0 else
                            "equal_average" if count * historical_articles == historical_words else
                            "below_average" if count * historical_articles < historical_words else "above_average")
                record(f"q5_{relation}", url)
                if matches:
                    add_expected(outputs, counts, 5, [[url, count]])
        snapshots = {}
        with (directory / "images.jsonl").open(encoding="utf-8") as source:
            for line in source:
                name, url, image_url, language = json.loads(line)
                if image_url not in snapshots:
                    snapshots[image_url] = snapshot_image(image_url)
                    print(f"Reference live image {len(snapshots)}: {snapshots[image_url]['success']}", flush=True)
                image = snapshots[image_url]
                if image["success"]:
                    add_expected(outputs, counts, 2, [[name, url, image["sha256"], image["bytes"]]])
                    coverage[f"q2_{language}_success"] += 1
                else:
                    record("q2_download_failed", url)
        for query in range(1, 6):
            coverage[f"q{query}_rows"] = counts[query]
            if require_coverage and coverage[f"q{query}_rows"] == 0:
                raise AssertionError(f"Partition gives no positive coverage for Q{query}")
        for language in ("en", "fr"):
            if require_coverage and coverage[f"q2_{language}_success"] < manifest["images_per_language"]:
                raise AssertionError(f"Insufficient live Q2 success coverage for {language}")
        if require_coverage and (coverage["q5_not_above"] == 0 or len(top) != 20):
            raise AssertionError("Missing negative Q5 or top-20 coverage")
        for language in ("en", "fr"):
            if require_coverage and coverage[f"q3_{language}_exclusive"] == 0:
                raise AssertionError(f"No exclusive {language} words for Q3 negative coverage")
        for query in range(1, 6):
            outputs[query].close()
            source = directory / f"q{query}.jsonl"
            target = output / source.name
            if query == 4:
                copyfile(source, target)
            else:
                sort_file(source, target)
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


def validate(dataset, expected, actual, output):
    dataset, expected, actual, output = map(Path, (dataset, expected, actual, output))
    manifest = json.loads((dataset / "manifest.json").read_text())
    reference_summary = json.loads((expected / "summary.json").read_text())
    if reference_summary["partition_manifest_sha256"] != hashlib.sha256((dataset / "manifest.json").read_bytes()).hexdigest():
        raise AssertionError("Reference belongs to a different partition manifest")
    summary = json.loads((actual / "summary.json").read_text())
    if summary["sent_rows"] != manifest["rows"] or not summary["all_done"] or summary["finished_queries"] != [1, 2, 3, 4, 5]:
        raise AssertionError("Client input or completion summary does not match the partition")
    output.mkdir(parents=True, exist_ok=True)
    report = {"passed": True, "partition_bytes": manifest["bytes"], "input_rows": manifest["rows"], "queries": {}}
    with TemporaryDirectory(prefix=".comparison-", dir=output) as directory:
        directory = Path(directory)
        for query in range(1, 6):
            count, order_ok = 0, True
            ordered = json.loads((expected / "q4_ordered.json").read_text()) if query == 4 else None
            normalized = directory / f"q{query}.jsonl"
            with (actual / normalized.name).open(encoding="utf-8") as source, normalized.open("w", encoding="utf-8") as target:
                for line in source:
                    row = json.loads(line)
                    if query == 2:
                        name, url, digest, size = row
                        data = (actual / "images" / f"{digest}.bin").read_bytes()
                        if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                            raise AssertionError(f"Corrupt received image: {url}")
                    if query == 4:
                        if count >= len(ordered) or row != ordered[count]:
                            order_ok = False
                    target.write(canonical(row) + "\n")
                    count += 1
            if query == 4:
                order_ok &= count == len(ordered)
            if count != summary["received_rows"][str(query)]:
                raise AssertionError(f"Receiver row count mismatch for Q{query}")
            expected_rows, missing, extra = compare_files(expected / normalized.name, normalized, directory)
            passed = not missing and not extra and order_ok
            report["queries"][str(query)] = {"passed": passed, "actual_rows": count,
                "expected_rows": expected_rows,
                "missing_examples": missing, "unexpected_examples": extra, "order_correct": order_ok}
            report["passed"] &= passed
        (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps(report, ensure_ascii=False), flush=True)
        if not report["passed"]:
            raise AssertionError("Full-suite output differs from the independent reference; see report.json")
        return report


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
