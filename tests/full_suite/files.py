import heapq
from contextlib import ExitStack
from itertools import groupby
from pathlib import Path
from tempfile import TemporaryDirectory


def _merge(paths, target):
    with ExitStack() as stack:
        sources = [stack.enter_context(path.open(encoding="utf-8")) for path in paths]
        output = stack.enter_context(target.open("w", encoding="utf-8"))
        output.writelines(heapq.merge(*sources))


def sort_file(source, target, *, chunk_bytes=8 * 1024 * 1024, merge_fan_in=32):
    if chunk_bytes < 1 or merge_fan_in < 2:
        raise ValueError("Invalid file sort configuration")
    source, target = Path(source), Path(target)
    with TemporaryDirectory(prefix=".sort-", dir=target.parent) as directory:
        directory = Path(directory)
        paths, chunk, size = [], [], 0

        def flush():
            path = directory / f"chunk-{len(paths)}.jsonl"
            with path.open("w", encoding="utf-8") as output:
                output.writelines(sorted(chunk))
            paths.append(path)
            chunk.clear()

        with source.open(encoding="utf-8") as input_file:
            for line in input_file:
                if not line.endswith("\n"):
                    line += "\n"
                chunk.append(line)
                size += len(line.encode("utf-8"))
                if size >= chunk_bytes:
                    flush()
                    size = 0
        if chunk:
            flush()
        generation = 0
        while len(paths) > merge_fan_in:
            merged = []
            for start in range(0, len(paths), merge_fan_in):
                group = paths[start:start + merge_fan_in]
                path = directory / f"merge-{generation}-{len(merged)}.jsonl"
                _merge(group, path)
                merged.append(path)
                for item in group:
                    item.unlink()
            paths = merged
            generation += 1
        _merge(paths, target)


def _counts(source):
    for line, group in groupby(source):
        yield line.rstrip("\n"), sum(1 for _ in group)


def compare_files(expected, actual, directory):
    directory = Path(directory)
    expected_sorted, actual_sorted = directory / "expected.jsonl", directory / "actual.jsonl"
    sort_file(expected, expected_sorted)
    sort_file(actual, actual_sorted)
    missing, extra, expected_rows = [], [], 0
    with expected_sorted.open(encoding="utf-8") as expected_file, actual_sorted.open(encoding="utf-8") as actual_file:
        expected_counts, actual_counts = _counts(expected_file), _counts(actual_file)
        left, right = next(expected_counts, None), next(actual_counts, None)
        while left is not None or right is not None:
            if right is None or (left is not None and left[0] < right[0]):
                expected_rows += left[1]
                if len(missing) < 10:
                    missing.append(left)
                left = next(expected_counts, None)
            elif left is None or right[0] < left[0]:
                if len(extra) < 10:
                    extra.append(right)
                right = next(actual_counts, None)
            else:
                expected_rows += left[1]
                difference = left[1] - right[1]
                if difference > 0 and len(missing) < 10:
                    missing.append((left[0], difference))
                elif difference < 0 and len(extra) < 10:
                    extra.append((right[0], -difference))
                left, right = next(expected_counts, None), next(actual_counts, None)
    return expected_rows, missing, extra
