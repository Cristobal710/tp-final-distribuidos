import unittest

from common.batching import BatchBuilder


def collect(builder, items):
    batches = []
    for item in items:
        closed = builder.add(item)
        if closed:
            batches.append(closed)
    last = builder.flush()
    if last:
        batches.append(last)
    return batches


class BatchBuilderTest(unittest.TestCase):

    def test_closes_by_item_count(self):
        batches = collect(BatchBuilder(max_items=3, max_bytes=1000), [b"x"] * 7)
        self.assertEqual([len(b) for b in batches], [3, 3, 1])

    def test_closes_by_size_without_splitting(self):
        items = [b"a" * 40, b"b" * 40, b"c" * 40]
        batches = collect(BatchBuilder(max_items=10, max_bytes=100), items)
        self.assertEqual(batches, [[items[0], items[1]], [items[2]]])

    def test_item_bigger_than_limit_goes_alone(self):
        items = [b"a" * 10, b"b" * 500, b"c" * 10]
        batches = collect(BatchBuilder(max_items=10, max_bytes=100), items)
        self.assertEqual(batches, [[items[0]], [items[1]], [items[2]]])

    def test_keeps_order_and_every_item(self):
        items = [bytes([i % 256]) * (i % 37 + 1) for i in range(500)]
        batches = collect(BatchBuilder(max_items=13, max_bytes=120), items)
        self.assertEqual([i for b in batches for i in b], items)
        for b in batches:
            self.assertLessEqual(len(b), 13)
            if len(b) > 1:
                self.assertLessEqual(sum(map(len, b)), 120)

    def test_flush_empty(self):
        self.assertIsNone(BatchBuilder(1, 1).flush())


if __name__ == "__main__":
    unittest.main()
