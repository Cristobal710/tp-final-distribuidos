import os
import re
import tempfile
import unittest

from generate_compose import COMPOSE_FILE, COUNT_KEYS, COUNTS_FILE, read_counts, render

BASE = {key: 2 for key in COUNT_KEYS} | {"JOIN_Q3_AMOUNT": 1, "TOP20_AMOUNT": 1}


def services_of(compose):
    return re.findall(r"^  ([a-z_0-9]+):$", compose, re.MULTILINE)


def amounts_of(compose, key):
    return set(re.findall(rf"^      {key}: (\d+)$", compose, re.MULTILINE))


class GenerateComposeTest(unittest.TestCase):

    def write_counts(self, text):
        path = os.path.join(tempfile.mkdtemp(), "nodes.env")
        with open(path, "w", encoding="utf-8") as file:
            file.write(text)
        return path

    def test_the_compose_of_the_repo_is_the_generated_one(self):
        with open(COMPOSE_FILE, encoding="utf-8") as file:
            self.assertEqual(file.read(), render(read_counts(COUNTS_FILE)))

    def test_changing_a_count_adds_the_instances_and_updates_every_service(self):
        compose = render(BASE | {"CLASSIFIER_AMOUNT": 3})
        self.assertIn("classifier_2", services_of(compose))
        self.assertNotIn("classifier_3", services_of(compose))
        self.assertEqual(amounts_of(compose, "CLASSIFIER_AMOUNT"), {"3"})
        self.assertEqual(compose.count("CLASSIFIER_AMOUNT: 3"), 3 + 2 + 2 + 1)
        self.assertIn("CLASSIFIER_ID: 2", compose)

    def test_every_count_reaches_all_the_services_that_use_it(self):
        counts = {key: 3 for key in COUNT_KEYS}
        compose = render(counts)
        for key in COUNT_KEYS:
            self.assertLessEqual(amounts_of(compose, key), {"3"})
        self.assertEqual(len(services_of(compose)), 2 + 3 * len(COUNT_KEYS) + 2)

    def test_lowering_a_count_removes_the_instances_with_the_highest_id(self):
        services = services_of(render(BASE | {"WORD_COUNTER_AMOUNT": 1}))
        self.assertIn("word_counter_0", services)
        self.assertNotIn("word_counter_1", services)

    def test_image_downloaders_keep_their_cache(self):
        compose = render(BASE | {"IMAGE_DOWNLOADER_AMOUNT": 3})
        self.assertEqual(compose.count("./.cache/q2:/cache/q2"), 3)

    def test_reads_the_counts_skipping_comments_and_blank_lines(self):
        text = "# cantidades\n\n" + "".join(f"{key}=4\n" for key in COUNT_KEYS)
        self.assertEqual(read_counts(self.write_counts(text)), {key: 4 for key in COUNT_KEYS})

    def test_rejects_a_missing_or_unknown_node(self):
        keys = sorted(COUNT_KEYS)
        for text in ("".join(f"{key}=1\n" for key in keys[1:]),
                     "".join(f"{key}=1\n" for key in keys) + "OTRO_AMOUNT=1\n"):
            with self.assertRaises(ValueError):
                read_counts(self.write_counts(text))

    def test_rejects_invalid_amounts(self):
        for value in ("0", "-1", "dos", ""):
            text = "".join(f"{key}=1\n" for key in sorted(COUNT_KEYS)[1:]) + f"{sorted(COUNT_KEYS)[0]}={value}\n"
            with self.assertRaises(ValueError):
                read_counts(self.write_counts(text))


if __name__ == "__main__":
    unittest.main()
