import collections
import csv
import datetime
import gzip
import json
import os
import re
import sys

FILE_REGEX = re.compile(r"q(\d)\.(csv|csv\.gz|jsonl)")
EXTENSIONS = ("csv", "csv.gz", "jsonl")
ORDERED_QUERIES = {4}
KEY_COLUMNS = {2: 2}
SAMPLES = 5
EPOCH_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def from_client(query, row):
    if query == 1:
        url, milliseconds = row
        return (url, (EPOCH_UTC + datetime.timedelta(milliseconds=milliseconds)).isoformat())
    return tuple(str(value) for value in row)


def read_rows(path, query):
    if path.endswith(".jsonl"):
        with open(path, encoding="utf-8") as file:
            return [from_client(query, json.loads(line)) for line in file if line.strip()]
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", newline="", encoding="utf-8") as file:
        return [tuple(row) for row in list(csv.reader(file))[1:]]


def find_file(directory, query):
    for extension in EXTENSIONS:
        path = os.path.join(directory, f"q{query}.{extension}")
        if os.path.exists(path):
            return path
    return None


def compare_query(query, expected, actual):
    columns = KEY_COLUMNS.get(query)
    if columns is not None:
        expected = [row[:columns] for row in expected]
        actual = [row[:columns] for row in actual]
    if query in ORDERED_QUERIES:
        if expected == actual:
            return []
        return [f"esperado: {expected}", f"obtenido: {actual}"]
    expected_rows = collections.Counter(expected)
    actual_rows = collections.Counter(actual)
    missing = expected_rows - actual_rows
    extra = actual_rows - expected_rows
    problems = []
    if missing:
        problems.append(f"faltan {sum(missing.values())} filas, por ejemplo {list(missing)[:SAMPLES]}")
    if extra:
        problems.append(f"sobran {sum(extra.values())} filas, por ejemplo {list(extra)[:SAMPLES]}")
    return problems


def compare_dirs(expected_dir, actual_dir):
    report, all_ok = [], True
    for name in sorted(os.listdir(expected_dir)):
        match = FILE_REGEX.fullmatch(name)
        if match is None:
            continue
        query = int(match.group(1))
        expected = read_rows(os.path.join(expected_dir, name), query)
        actual_path = find_file(actual_dir, query)
        if actual_path is None:
            report.append(f"Q{query}: sin resultado del sistema ({len(expected)} filas esperadas), se saltea")
            continue
        actual = read_rows(actual_path, query)
        problems = compare_query(query, expected, actual)
        if problems:
            all_ok = False
            report.append(f"Q{query}: DIFIERE ({len(actual)} filas obtenidas, {len(expected)} esperadas)")
            report.extend(f"    {problem}" for problem in problems)
        else:
            report.append(f"Q{query}: OK ({len(actual)} filas)")
    return all_ok, report


def main():
    if len(sys.argv) != 3:
        print("Uso: compare.py <directorio esperado> <directorio del cliente>")
        return 2
    all_ok, report = compare_dirs(sys.argv[1], sys.argv[2])
    print("\n".join(report))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
