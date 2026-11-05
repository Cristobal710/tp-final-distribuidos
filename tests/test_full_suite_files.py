import tempfile
import unittest
from pathlib import Path

from tests.full_suite.files import compare_files, sort_file


class FullSuiteFilesTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_sort_preserves_duplicates_and_unicode_across_multiple_merge_passes(self):
        rows = ['["é"]', '["z"]', '["猫"]', '["a"]', '["é"]', '["a"]'] * 5
        source, target = self.root / "source.jsonl", self.root / "target.jsonl"
        source.write_text("\n".join(rows), encoding="utf-8")
        sort_file(source, target, chunk_bytes=1, merge_fan_in=2)
        self.assertEqual(target.read_text(encoding="utf-8").splitlines(), sorted(rows))
        self.assertEqual(set(self.root.iterdir()), {source, target})

    def test_empty_files_sort_and_compare(self):
        source, target = self.root / "empty.jsonl", self.root / "sorted.jsonl"
        source.touch()
        sort_file(source, target)
        self.assertEqual(target.read_bytes(), b"")
        self.assertEqual(compare_files(source, target, self.root), (0, [], []))

    def test_comparison_counts_duplicates_independently_of_order(self):
        expected, actual = self.root / "left.jsonl", self.root / "right.jsonl"
        expected.write_text('a\na\nb\nc\n')
        actual.write_text('d\nb\nb\na\n')
        self.assertEqual(compare_files(expected, actual, self.root),
                         (4, [("a", 1), ("c", 1)], [("b", 1), ("d", 1)]))
        actual.write_text('c\nb\na\na\n')
        self.assertEqual(compare_files(expected, actual, self.root), (4, [], []))

    def test_diagnostic_examples_are_bounded_without_truncating_expected_count(self):
        expected, actual = self.root / "left.jsonl", self.root / "right.jsonl"
        expected.write_text("".join(f"e-{i:02}\n" * 2 for i in range(30)))
        actual.write_text("".join(f"a-{i:02}\n" for i in range(30)))
        count, missing, extra = compare_files(expected, actual, self.root)
        self.assertEqual((count, len(missing), len(extra)), (60, 10, 10))
        self.assertEqual(missing[0], ("e-00", 2))
        self.assertEqual(extra[0], ("a-00", 1))

    def test_invalid_sort_limits_are_rejected(self):
        for options in ({"chunk_bytes": 0}, {"merge_fan_in": 1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                sort_file(self.root / "source", self.root / "target", **options)
