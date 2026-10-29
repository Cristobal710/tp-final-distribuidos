import random
import re
import tempfile
import unittest
from collections import Counter, deque
from pathlib import Path

from average import Average
from classifier import classify
from common.eof import UpstreamTracker
from common.protocol import internal
from common.protocol.articles import Article
from common.storage import ClientFiles
from comparator import Comparator
from q5_output import Q5Output
from tests.q2_fakes import ConsumerQueue, GroupControl, OutputQueue, noop
from text_processor import TextProcessor
from words_output import WordsOutput

START_OF_2021 = 1609459200000
STOP_WORDS = {"the", "a", "of", "le", "la", "de", "et"}


def article(url, abstract, created=1000, language="en"):
    return Article(url, url, {"identifier": language}, created, 2000, abstract, "[]", None)


def sample_articles():
    return [
        article("old-en", "the sun sun moon"),
        article("old-fr", "le soleil soleil", language="fr"),
        article("old-empty", "", created=START_OF_2021 - 1),
        article("new-tie", "sun sun", created=START_OF_2021),
        article("new-match", "sun moon moon", created=START_OF_2021 + 1),
        article("new-fr", "le ciel bleu nuage", created=START_OF_2021, language="fr"),
        article("unknown-period", "many different words", created=None),
        article("missing-abstract", None),
    ]


def clean_words(abstract):
    if abstract is None:
        return set()
    return {word for word in re.findall(r"\b\w+\b", abstract.lower())
            if word not in STOP_WORDS and not word.isdecimal()}


def oracle(articles):
    historical = [len(clean_words(a.abstract)) for a in articles
                  if a.abstract is not None and a.date_created is not None and a.date_created < START_OF_2021]
    if not historical:
        return []
    total = sum(historical)
    return [(a.url, len(clean_words(a.abstract))) for a in articles
            if a.abstract is not None and a.date_created is not None and a.date_created >= START_OF_2021
            and len(clean_words(a.abstract)) * len(historical) > total]


def classifier_streams(client_id, gateway_id, articles, batch_size):
    batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
    streams = []
    for classifier in range(2):
        mine = batches[classifier::2]
        stream = []
        for batch in mine:
            texts = classify(batch)[0]
            if texts:
                stream.append(internal.encode_texts(client_id, gateway_id, texts))
        stream.append(internal.encode_eof(
            client_id, gateway_id, f"classifier_{classifier}", len(mine), len(batches), len(stream),
        ))
        streams.append(stream)
    return streams


def interleave(streams, rng):
    streams = [deque(stream) for stream in streams]
    output = []
    while any(streams):
        stream = rng.choice([stream for stream in streams if stream])
        output.append(stream.popleft())
    return output


def delay_data(messages, kind):
    for i, message in enumerate(messages):
        if internal.decode(message)[0] == kind:
            messages.append(messages.pop(i))
            return


class Q5PipelineTest(unittest.TestCase):

    def run_pipeline(self, clients, processors=2, average_amount=2, comparator_amount=2,
                     batch_size=2, seed=0, order="random", delay_text=False, delay_stats=False, delay_candidates=False):
        rng = random.Random(seed)
        average_queues = [OutputQueue() for _ in range(average_amount)]
        candidate_queues = [OutputQueue() for _ in range(comparator_amount)]
        word_queues = [OutputQueue(), OutputQueue()]
        control = GroupControl()
        text_processors = []
        for i in range(processors):
            outputs = [
                WordsOutput("palabras", word_queues, 2, 1000),
                Q5Output("promedio", average_queues, "comparacion", candidate_queues, 2, 1000),
            ]
            text_processors.append(TextProcessor(i, ConsumerQueue(), None, control, outputs, 2, STOP_WORDS, 1))
        control.workers = text_processors
        messages = interleave([stream for client, gateway, articles in clients
                               for stream in classifier_streams(client, gateway, articles, batch_size)], rng)
        if delay_text:
            delay_data(messages, internal.MsgType.TEXTS)
        for i, message in enumerate(messages):
            worker = text_processors[i % processors]
            worker._text_queue.deliver(worker, message)
            for peer in text_processors:
                peer._text_queue.pump()

        # Q3/Q4 conserva su salida mientras Q5 participa en el mismo Text processor.
        for client, _, articles in clients:
            expected_words = Counter()
            for a in articles:
                if a.abstract is not None:
                    expected_words.update(clean_words(a.abstract))
            actual_words = Counter()
            for queue in word_queues:
                for value in queue.decoded(internal.MsgType.WORDS):
                    if value["client_id"] == client:
                        for row in value["words"]:
                            actual_words[row.word] += row.articles
            self.assertEqual(actual_words, expected_words)

        final_queues = [OutputQueue() for _ in range(comparator_amount)]
        averages = [Average(i, ConsumerQueue(), final_queues, processors) for i in range(average_amount)]
        stats_inputs = [list(queue.messages) for queue in average_queues]
        candidate_inputs = [list(queue.messages) for queue in candidate_queues]
        if delay_stats:
            for messages in stats_inputs:
                delay_data(messages, internal.MsgType.PARTIAL_AVERAGE)
        if delay_candidates:
            for messages in candidate_inputs:
                delay_data(messages, internal.MsgType.CANDIDATES)
        stats_inputs = [deque(messages) for messages in stats_inputs]
        candidate_inputs = [deque(messages) for messages in candidate_inputs]
        results = {}

        def open_results(gateway_id):
            return results.setdefault(gateway_id, OutputQueue())

        with tempfile.TemporaryDirectory() as directory:
            comparators = [
                Comparator(i, ConsumerQueue(), ConsumerQueue(), ClientFiles(directory, f"comparator_{i}"),
                           open_results, processors, 2, 1000, drain_rows=1)
                for i in range(comparator_amount)
            ]
            positions = [0] * comparator_amount
            waiting = [False] * comparator_amount
            try:
                while True:
                    events = [("stats", i) for i, messages in enumerate(stats_inputs) if messages]
                    events += [("candidate", i) for i, messages in enumerate(candidate_inputs) if messages]
                    for i, worker in enumerate(comparators):
                        if not waiting[i] and positions[i] < len(final_queues[i].messages):
                            events.append(("average", i))
                        if not worker._input_queue.pending.empty():
                            events.append(("callback", i))
                        if not worker._average_input.pending.empty():
                            events.append(("ack", i))
                    if not events:
                        break
                    if order == "average_first":
                        priority = {"stats": 0, "average": 1, "callback": 2, "ack": 3, "candidate": 4}
                        kind, i = min(events, key=lambda event: priority[event[0]])
                    elif order == "candidates_first":
                        priority = {"candidate": 0, "stats": 1, "average": 2, "callback": 3, "ack": 4}
                        kind, i = min(events, key=lambda event: priority[event[0]])
                    else:
                        kind, i = rng.choice(events)
                    if kind == "stats":
                        averages[i]._input_queue.deliver(averages[i], stats_inputs[i].popleft())
                    elif kind == "candidate":
                        comparators[i]._input_queue.deliver(comparators[i], candidate_inputs[i].popleft())
                    elif kind == "average":
                        waiting[i] = True
                        message = final_queues[i].messages[positions[i]]
                        positions[i] += 1

                        def ack(index=i):
                            waiting[index] = False
                            comparators[index]._average_input.ack()

                        comparators[i]._on_average_message(message, ack, noop)
                    elif kind == "callback":
                        comparators[i]._input_queue.pending.get_nowait()()
                    else:
                        comparators[i]._average_input.pending.get_nowait()()

                self.assertFalse(any(waiting))
                self.assertEqual(list(Path(directory).glob("*.jsonl")), [])
                self.assertTrue(all(not worker._totals for worker in averages))
                self.assertTrue(all(not worker._baselines and not worker._drains for worker in comparators))
                for client, gateway, articles in clients:
                    actual, eofs = [], []
                    tracker = UpstreamTracker(comparator_amount, check_total=False, check_received=True)
                    for message in results[gateway].messages:
                        kind, value = internal.decode(message)
                        if value["client_id"] != client:
                            continue
                        self.assertEqual((value["gateway_id"], value["query"]), (gateway, 5))
                        if kind == internal.MsgType.RESULTS:
                            actual.extend(tuple(row) for row in value["rows"])
                            tracker.add_message(client)
                        elif kind == internal.MsgType.EOF:
                            self.assertEqual(value["processed"], value["total"])
                            eofs.append(value)
                            tracker.add_eof(value)
                    self.assertCountEqual(actual, oracle(articles))
                    self.assertEqual(len(eofs), comparator_amount)
                    self.assertTrue(tracker.is_complete(client), tracker._clients)
            finally:
                for worker in comparators:
                    worker.close()

    def test_pipeline_matches_direct_computation(self):
        self.run_pipeline([("c1", 0, sample_articles())])

    def test_average_can_arrive_before_candidates(self):
        self.run_pipeline([("c1", 0, sample_articles())], order="average_first")

    def test_candidates_and_eofs_can_arrive_before_average(self):
        self.run_pipeline([("c1", 0, sample_articles())], order="candidates_first")

    def test_multiple_clients_and_gateways_are_isolated(self):
        clients = [("a", 0, sample_articles()), ("b", 7, sample_articles() * 2), ("c", 0, sample_articles()[3:])]
        self.run_pipeline(clients, average_amount=3, comparator_amount=3)

    def test_empty_no_historical_and_no_candidate_inputs_complete(self):
        clients = [("empty", 0, []), ("new-only", 0, sample_articles()[3:]),
                   ("old-only", 3, sample_articles()[:3]),
                   ("no-date", 3, [article("unknown", "sun", created=None)]),
                   ("no-abstract", 0, [article("missing", None)])]
        self.run_pipeline(clients)

    def test_zero_average_and_strict_equality(self):
        articles = [article("old-zero", "the 123"), article("new-zero", "the", START_OF_2021),
                    article("new-one", "sun sun", START_OF_2021)]
        self.run_pipeline([("c1", 0, articles)])

    def test_weighted_average_is_independent_of_batch_sizes(self):
        articles = [article("long-old", "one two three four five")]
        articles += [article(f"old-{i}", "one") for i in range(8)]
        articles += [article("new", "one two", START_OF_2021)]
        for batch_size in (1, 3, 8):
            with self.subTest(batch_size=batch_size):
                self.run_pipeline([("c1", 0, articles)], batch_size=batch_size)

    def test_result_is_independent_of_replica_counts_and_interleaving(self):
        for processors, averages, comparators in ((1, 1, 1), (2, 3, 2), (3, 2, 4)):
            for seed in range(3):
                with self.subTest(processors=processors, averages=averages, comparators=comparators, seed=seed):
                    self.run_pipeline([("c1", 0, sample_articles() * 2)], processors, averages, comparators, seed=seed)

    def test_text_batch_processed_after_group_eof_is_included(self):
        self.run_pipeline([("c1", 0, sample_articles())], delay_text=True)

    def test_statistics_after_eofs_are_included_before_final_average(self):
        self.run_pipeline([("c1", 0, sample_articles())], delay_stats=True)

    def test_candidates_after_eofs_are_processed_before_completion(self):
        self.run_pipeline([("c1", 0, sample_articles())], delay_candidates=True)
