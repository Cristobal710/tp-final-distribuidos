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
        self.assertEqual(len(services_of(compose)), 2 + 3 * len(COUNT_KEYS) + 3)

    def test_lowering_a_count_removes_the_instances_with_the_highest_id(self):
        services = services_of(render(BASE | {"WORD_COUNTER_AMOUNT": 1}))
        self.assertIn("word_counter_0", services)
        self.assertNotIn("word_counter_1", services)

    def test_image_downloaders_keep_their_cache(self):
        compose = render(BASE | {"IMAGE_DOWNLOADER_AMOUNT": 3})
        self.assertEqual(compose.count("./.cache/q2:/cache/q2"), 3)
        self.assertEqual(compose.count("DOWNLOAD_INTERVAL_SECONDS: ${DOWNLOAD_INTERVAL_SECONDS:-0}"), 3)
        self.assertEqual(compose.count("IMAGE_CACHE_DIR: ${IMAGE_CACHE_DIR-/cache/q2}"), 3)
        self.assertNotIn("IMAGE_CACHE_MAX_BYTES", compose)

    def test_monitors_watch_every_node_and_can_restart_them(self):
        compose = render(BASE | {"MONITOR_AMOUNT": 3, "TOP20_AMOUNT": 2})
        self.assertIn("monitor_2", services_of(compose))
        self.assertNotIn("monitor_3", services_of(compose))
        self.assertEqual(compose.count("- /var/run/docker.sock:/var/run/docker.sock"), 3 + 1)
        watched = set(re.findall(r"^      WATCHED_NODES: (.*)$", compose, re.MULTILINE))
        self.assertEqual(len(watched), 1)
        nodes = watched.pop().split(",")
        expected = [name for name in services_of(compose)
                    if name not in ("rabbitmq", "gateway", "client_0", "expected", "chaos_monkey")
                    and not name.startswith("monitor_")]
        self.assertEqual(nodes, expected)
        self.assertIn("top20_1", nodes)
        self.assertEqual(amounts_of(compose, "MONITOR_AMOUNT"), {"3"})

    def test_the_chaos_monkey_only_runs_when_asked_and_never_kills_the_gateway_rabbitmq_or_clients(self):
        compose = render(BASE | {"MONITOR_AMOUNT": 3})
        block = compose.split("  chaos_monkey:\n", 1)[1].split("\n\n", 1)[0]
        self.assertIn("profiles: [chaos]", block)
        self.assertIn("- /var/run/docker.sock:/var/run/docker.sock", block)
        targets = re.search(r"CHAOS_TARGETS: \$\{CHAOS_TARGETS:-(.*)\}", block).group(1).split(",")
        nodes = re.search(r"^      WATCHED_NODES: (.*)$", compose, re.MULTILINE).group(1).split(",")
        self.assertEqual(targets, nodes + ["monitor_0", "monitor_1", "monitor_2"])
        for untouchable in ("gateway", "rabbitmq", "client_0", "expected", "chaos_monkey"):
            self.assertNotIn(untouchable, targets)

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
