import csv
import hashlib
import io
import os
import sys

from compare import FILE_REGEX, KEY_COLUMNS, ORDERED_QUERIES, read_rows

HASHES_FILE = "hashes.txt"


def csv_line(row):
    buffer = io.StringIO()
    csv.writer(buffer).writerow(row)
    return buffer.getvalue().encode("utf-8")


def digest_of(path, query):
    columns = KEY_COLUMNS.get(query)
    lines = [csv_line(row[:columns]) for row in read_rows(path, query)]
    if query not in ORDERED_QUERIES:
        lines.sort()
    return len(lines), hashlib.sha256(b"".join(lines)).hexdigest()


def digests_of(directory):
    digests = {}
    for name in sorted(os.listdir(directory)):
        match = FILE_REGEX.fullmatch(name)
        if match is not None:
            query = int(match.group(1))
            digests[query] = digest_of(os.path.join(directory, name), query)
    return digests


def format_digests(digests):
    return "".join(f"q{query} {rows} {digest}\n" for query, (rows, digest) in sorted(digests.items()))


def read_digests(path):
    digests = {}
    with open(path, encoding="utf-8") as file:
        for line in file:
            name, rows, digest = line.split()
            digests[int(name[1:])] = (int(rows), digest)
    return digests


def compare_digests(expected, actual):
    report, all_ok = [], True
    for query, (expected_rows, expected_digest) in sorted(expected.items()):
        if query not in actual:
            report.append(f"Q{query}: sin resultado del sistema, se saltea")
            continue
        actual_rows, actual_digest = actual[query]
        ok = actual_digest == expected_digest
        all_ok = all_ok and ok
        report.append(f"Q{query}: {'OK' if ok else 'DIFIERE'}")
        report.append(f"    esperado {expected_digest} ({expected_rows} filas)")
        report.append(f"    obtenido {actual_digest} ({actual_rows} filas)")
    return all_ok, report


def main():
    if len(sys.argv) not in (2, 3):
        print("Uso: hashes.py <directorio de resultados> [archivo de hashes esperados]")
        return 2
    directory = sys.argv[1]
    digests = digests_of(directory)
    with open(os.path.join(directory, HASHES_FILE), "w", encoding="utf-8") as file:
        file.write(format_digests(digests))
    if len(sys.argv) == 2:
        print(format_digests(digests), end="")
        return 0
    all_ok, report = compare_digests(read_digests(sys.argv[2]), digests)
    print("\n".join(report))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
