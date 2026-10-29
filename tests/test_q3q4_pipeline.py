import random
import tempfile
import unittest
from collections import Counter

from common.protocol import internal
from common.protocol.internal import MsgType
from common.storage import ClientFiles
from join_q3 import JoinQ3
from tests.q3q4_fakes import FakeControlInput, FakeInputQueue, FakeTextQueue, noop
from text_processor import TextProcessor
from top20 import Top20
from word_counter import WordCounter
from words import extract_words
from words_output import WordsOutput

STOP_WORDS = {"the", "a", "of", "le", "la", "de", "et"}
VOCABULARY = [f"palabra{i}" for i in range(60)] + sorted(STOP_WORDS) + ["2020", "1.5", "été", "Éclair"]
TEXT_PROCESSORS, WORD_COUNTERS, TOP_SIZE = 2, 3, 20


# Cola que guarda los mensajes tal como se mandaron, para dárselos despues al nodo que sigue.
class RawQueue:

    def __init__(self):
        self.messages = []

    def send(self, message):
        self.messages.append(message)

    def close(self):
        pass


# Salida de control que le entrega cada mensaje a todos los Text processor, como el exchange de control.
class GroupControl:

    def __init__(self):
        self.processors = []

    def send(self, message):
        for processor in self.processors:
            processor._on_control_message(message, noop, noop)

    def close(self):
        pass


def make_articles(rng, amount):
    articles = []
    for _ in range(amount):
        words = [rng.choice(VOCABULARY) for _ in range(rng.randint(0, 12))]
        articles.append((" ".join(words).capitalize() + ".", rng.choice(["en", "fr", "en", "fr", "es"])))
    return articles


# Calcula Q3 y Q4 directo sobre los articulos, como el notebook de referencia.
def oracle(articles):
    english, french, counter = set(), set(), Counter()
    for abstract, language in articles:
        words = set(extract_words(abstract, STOP_WORDS))
        if language == "en":
            english |= words
        elif language == "fr":
            french |= words
        counter.update(words)
    top = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:TOP_SIZE]
    return sorted(english & french), top


# Arma lo que mandarian dos clasificadores a la cola de texto para un cliente: sus textos y su EOF cada uno.
def classifier_streams(client_id, gateway_id, articles, batch_size):
    batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
    streams = []
    for classifier in range(2):
        mine = batches[classifier::2]
        stream = [
            internal.encode_texts(client_id, gateway_id, [
                internal.Text(f"https://wiki/{classifier}/{i}", abstract, True, language) for abstract, language in batch
            ])
            for i, batch in enumerate(mine)
        ]
        stream.append(internal.encode_eof(
            client_id, gateway_id, f"classifier_{classifier}", len(mine), len(batches), len(mine)
        ))
        streams.append(stream)
    return streams


# Junta varios flujos en uno al azar, respetando el orden de cada flujo.
def interleave(streams, rng):
    streams = [list(stream) for stream in streams]
    merged = []
    while any(streams):
        stream = rng.choice([s for s in streams if s])
        merged.append(stream.pop(0))
    return merged


class PipelineTest(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def run_pipeline(self, texto_messages, delay_last_data_message=False):
        shard_queues = self.shard_queues = [RawQueue() for _ in range(WORD_COUNTERS)]
        control = GroupControl()
        self.processors = []
        for processor_id in range(TEXT_PROCESSORS):
            output = WordsOutput("palabras", shard_queues, 7, 1_000_000)
            queue = FakeTextQueue([], FakeControlInput())
            processor = TextProcessor(processor_id, queue, FakeControlInput(), control, [output], 2, STOP_WORDS, 1)
            self.processors.append(processor)
        control.processors = self.processors
        delayed = None
        messages = list(texto_messages)
        if delay_last_data_message:
            index = max(i for i, m in enumerate(messages) if internal.decode(m)[0] == MsgType.TEXTS)
            delayed = messages.pop(index)
        for i, message in enumerate(messages):
            processor = self.processors[i % TEXT_PROCESSORS]
            processor._text_queue._run_pending()
            processor._on_message(message, noop, noop)
        for processor in self.processors:
            processor._text_queue._run_pending()
        if delayed is not None:
            late_processor = self.processors[0]
            late_processor._on_message(delayed, noop, noop)
        join_queues, top_queues, results = [RawQueue()], [RawQueue()], {}

        def open_results_queue(gateway_id):
            results.setdefault(gateway_id, RawQueue())
            return results[gateway_id]

        for counter_id in range(WORD_COUNTERS):
            counter = WordCounter(
                counter_id, FakeInputQueue(shard_queues[counter_id].messages),
                [ClientFiles(self.directory.name, f"wc{counter_id}_{bucket}") for bucket in range(4)], join_queues, top_queues,
                TEXT_PROCESSORS, TOP_SIZE, 5, 1_000_000,
            )
            with self.assertLogs(level="INFO"):
                counter.run()
        join = JoinQ3(0, FakeInputQueue(join_queues[0].messages), ClientFiles(self.directory.name, "join"),
                      open_results_queue, WORD_COUNTERS, 4, 1_000_000)
        top = Top20(0, FakeInputQueue(top_queues[0].messages), open_results_queue, WORD_COUNTERS, TOP_SIZE)
        with self.assertLogs(level="INFO"):
            join.run()
            top.run()
        return results

    # Devuelve, por cliente, las palabras de Q3 y el top de Q4 que llegaron y los EOF de cada consulta.
    def collect(self, results, gateway_id):
        q3, q4, eofs = [], [], {}
        for message in results[gateway_id].messages:
            msg_type, value = internal.decode(message)
            if msg_type == MsgType.RESULTS:
                (q3 if value["query"] == 3 else q4).append(value)
            else:
                eofs.setdefault(value["query"], []).append(value)
        words = sorted(row[0] for value in q3 for row in value["rows"])
        top = [tuple(row) for value in q4 for row in value["rows"]]
        return words, top, eofs, len(q3), len(q4)

    def check_client(self, results, gateway_id, articles):
        words, top, eofs, q3_messages, q4_messages = self.collect(results, gateway_id)
        expected_words, expected_top = oracle(articles)
        self.assertEqual(words, expected_words)
        self.assertEqual(top, expected_top)
        self.assertEqual([e["sent"] for e in eofs[3]], [q3_messages])
        self.assertEqual([e["sent"] for e in eofs[4]], [q4_messages])

    def test_one_client_matches_the_direct_computation(self):
        rng = random.Random(1)
        articles = make_articles(rng, 80)
        streams = classifier_streams("c1", 0, articles, 3)
        results = self.run_pipeline(interleave(streams, rng))
        self.check_client(results, 0, articles)
        self.assertTrue(oracle(articles)[0])

    def test_several_clients_at_the_same_time_do_not_mix(self):
        rng = random.Random(2)
        corpus = {gateway: make_articles(rng, 50 + 10 * gateway) for gateway in range(3)}
        streams = []
        for gateway, articles in corpus.items():
            streams += classifier_streams(f"c{gateway}", gateway, articles, 4)
        results = self.run_pipeline(interleave(streams, rng))
        for gateway, articles in corpus.items():
            self.check_client(results, gateway, articles)

    def test_a_text_processed_after_its_group_announced_the_end_is_still_counted(self):
        rng = random.Random(3)
        articles = make_articles(rng, 40)
        streams = classifier_streams("c1", 0, articles, 3)
        results = self.run_pipeline(interleave(streams, rng), delay_last_data_message=True)
        kinds = [internal.decode(m)[0] for queue in self.shard_queues for m in queue.messages]
        self.assertIn(MsgType.LATE_EOF, kinds)
        self.check_client(results, 0, articles)

    def test_a_client_without_articles_still_gets_both_eofs(self):
        streams = classifier_streams("c1", 0, [], 3)
        results = self.run_pipeline(streams[0] + streams[1])
        words, top, eofs, _, _ = self.collect(results, 0)
        self.assertEqual((words, top), ([], []))
        self.assertEqual(sorted(eofs), [3, 4])

    def test_the_result_does_not_depend_on_how_many_instances_or_the_arrival_order(self):
        rng = random.Random(4)
        articles = make_articles(rng, 60)
        for seed in range(5):
            streams = classifier_streams("c1", 0, articles, 2 + seed)
            results = self.run_pipeline(interleave(streams, random.Random(seed)))
            self.check_client(results, 0, articles)


if __name__ == "__main__":
    unittest.main()
