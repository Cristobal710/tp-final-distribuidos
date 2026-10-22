import unittest

from common import partition
from common.protocol import internal
from common.protocol.internal import MsgType
from tests.q3q4_fakes import FakeControlInput, FakeControlOutput, FakeOutputQueue, FakeTextQueue
from text_processor import TextProcessor
from words import extract_words
from words_output import WordsOutput

STOP_WORDS = {"the", "a", "le", "la", "de"}


def text(i, abstract, language="en", until_2020=True):
    return internal.Text(f"https://{language}.wikipedia.org/wiki/{i}", abstract, until_2020, language)


def texts_message(client_id, *texts, gateway_id=0):
    return internal.encode_texts(client_id, gateway_id, list(texts))


def eof_message(client_id, origin, processed, total, sent, gateway_id=0):
    return internal.encode_eof(client_id, gateway_id, origin, processed, total, sent)


def late_eof_message(client_id, origin, processed, sent, gateway_id=0):
    return internal.encode_late_eof(client_id, gateway_id, origin, processed, sent)


class ExtractWordsTest(unittest.TestCase):

    def test_lowercases_and_splits_on_anything_that_is_not_a_word(self):
        self.assertEqual(extract_words("Hello, World! Hello-again", set()), ["hello", "world", "hello", "again"])

    def test_removes_stop_words_and_numbers(self):
        self.assertEqual(extract_words("The 2020 war of 1.5 and +3 a3d", {"the", "of", "and"}), ["war", "a3d"])

    def test_keeps_accents_and_repeated_words(self):
        self.assertEqual(extract_words("Été été ÉTÉ", set()), ["été", "été", "été"])

    def test_missing_abstract_has_no_words(self):
        self.assertEqual(extract_words(None, set()), [])
        self.assertEqual(extract_words("", set()), [])


class WordsOutputTest(unittest.TestCase):

    def make(self, shards=2, max_rows=100, max_bytes=1_000_000):
        self.queues = [FakeOutputQueue() for _ in range(shards)]
        return WordsOutput("palabras", self.queues, max_rows, max_bytes)

    def processed(self, language, words):
        from text_processor import ProcessedText
        return ProcessedText("url", True, language, words)

    def words_by_shard(self):
        return {
            shard: {row.word: row for value in queue.data() for row in value["words"]}
            for shard, queue in enumerate(self.queues)
        }

    def test_each_word_goes_to_the_shard_of_its_hash_with_article_count_and_languages(self):
        output = self.make()
        output.send_batch("c1", 0, [
            self.processed("en", ["sun", "sun", "moon"]),
            self.processed("fr", ["sun", "lune"]),
            self.processed("en", ["moon"]),
        ])
        found = {}
        for shard, words in self.words_by_shard().items():
            for word, row in words.items():
                self.assertEqual(partition.shard_of(word, 2), shard)
                found[word] = row
        self.assertEqual(found, {
            "sun": internal.Word("sun", 2, True, True),
            "moon": internal.Word("moon", 2, True, False),
            "lune": internal.Word("lune", 1, False, True),
        })

    def test_other_languages_count_as_articles_but_have_no_language_flag(self):
        output = self.make(shards=1)
        output.send_batch("c1", 0, [self.processed("es", ["hola"]), self.processed(None, ["hola"])])
        self.assertEqual(self.words_by_shard()[0], {"hola": internal.Word("hola", 2, False, False)})

    def test_returns_how_many_messages_it_sent_to_each_queue(self):
        output = self.make(shards=2, max_rows=2)
        sent = output.send_batch("c1", 0, [self.processed("en", [f"palabra{i}" for i in range(30)])])
        for shard, queue in enumerate(self.queues):
            self.assertEqual(sent.get(f"palabras.{shard}", 0), len(queue.data()))
            self.assertTrue(all(len(value["words"]) <= 2 for value in queue.data()))
        self.assertEqual(sum(len(value["words"]) for queue in self.queues for value in queue.data()), 30)

    def test_sends_no_message_without_words(self):
        output = self.make()
        self.assertEqual(output.send_batch("c1", 0, [self.processed("en", [])]), {})
        self.assertEqual([queue.sent for queue in self.queues], [[], []])

    def test_eofs_go_to_every_shard_with_what_each_one_received(self):
        output = self.make(shards=3)
        output.send_eofs({"palabras.1": 4}, lambda count, query=None: internal.encode_eof("c1", 0, "tp", 5, 6, count, query))
        self.assertEqual(
            [[value["sent"] for _, value in queue.eofs()] for queue in self.queues], [[0], [4], [0]]
        )


class TextProcessorTest(unittest.TestCase):

    def run_processor(self, *events, classifier_amount=2, shards=2, in_flight=1):
        control_input = FakeControlInput()
        self.control_output = FakeControlOutput([control_input])
        self.input = FakeTextQueue(events, control_input)
        self.shards = [FakeOutputQueue() for _ in range(shards)]
        output = WordsOutput("palabras", self.shards, 100, 1_000_000)
        self.processor = TextProcessor(
            2, self.input, control_input, self.control_output, [output], classifier_amount, STOP_WORDS, in_flight
        )
        with self.assertLogs(level="INFO") as logs:
            self.processor.run()
        return "\n".join(logs.output)

    def all_eofs(self):
        return [queue.eofs() for queue in self.shards]

    def test_removes_stop_words_before_sending(self):
        self.run_processor(texts_message("c1", text(1, "The sun and the moon of 2020")), classifier_amount=1)
        words = {row.word for queue in self.shards for value in queue.data() for row in value["words"]}
        self.assertEqual(words, {"sun", "and", "moon", "of"})

    def test_forwards_every_classifier_eof_and_late_eof_to_the_group(self):
        eof = eof_message("c1", "classifier_0", 1, 2, 1)
        late = late_eof_message("c1", "classifier_1", 1, 1)
        self.run_processor(eof, late)
        self.assertEqual(self.control_output.sent, [eof, late])
        self.assertEqual(self.input.acks, 2)

    def test_sends_its_eof_to_every_shard_once_it_has_all_the_classifier_eofs(self):
        self.run_processor(
            texts_message("c1", text(1, "sun moon")),
            texts_message("c1", text(2, "sun lune", "fr")),
            eof_message("c1", "classifier_0", 1, 2, 1),
            eof_message("c1", "classifier_1", 1, 2, 1),
        )
        for shard, eofs in enumerate(self.all_eofs()):
            [(msg_type, value)] = eofs
            self.assertEqual(msg_type, MsgType.EOF)
            self.assertEqual(
                (value["client_id"], value["gateway_id"], value["origin"], value["processed"], value["total"]),
                ("c1", 0, "text_processor_2", 2, 2),
            )
            self.assertEqual(value["sent"], len(self.shards[shard].data()))

    def test_eof_goes_after_the_words_of_that_shard(self):
        self.run_processor(
            texts_message("c1", text(1, "sun moon star")),
            eof_message("c1", "classifier_0", 1, 1, 1),
            classifier_amount=1,
        )
        for queue in self.shards:
            self.assertEqual(queue.sent[-1][0], MsgType.EOF)

    def test_waits_for_the_late_eof_before_finishing(self):
        self.run_processor(
            texts_message("c1", text(1, "sun")),
            eof_message("c1", "classifier_0", 1, 2, 1),
            eof_message("c1", "classifier_1", 0, 2, 0),
        )
        self.assertEqual(self.all_eofs(), [[], []])
        self.run_processor(
            texts_message("c1", text(1, "sun")),
            eof_message("c1", "classifier_0", 1, 2, 1),
            eof_message("c1", "classifier_1", 0, 2, 0),
            texts_message("c1", text(2, "moon")),
            late_eof_message("c1", "classifier_1", 1, 1),
        )
        for [(_, value)] in self.all_eofs():
            self.assertEqual((value["processed"], value["total"]), (2, 2))

    def test_a_batch_processed_after_the_eof_is_announced_with_a_late_eof(self):
        self.run_processor(
            eof_message("c1", "classifier_0", 1, 1, 1),
            texts_message("c1", text(1, "sun moon")),
            classifier_amount=1,
        )
        for shard, queue in enumerate(self.shards):
            kinds = [msg_type for msg_type, _ in queue.sent]
            self.assertEqual(kinds[0], MsgType.EOF)
            [(_, eof)] = [(k, v) for k, v in queue.sent if k == MsgType.EOF]
            self.assertEqual(eof["processed"], 0)
            if queue.data():
                [(msg_type, late)] = queue.eofs()[1:]
                self.assertEqual(msg_type, MsgType.LATE_EOF)
                self.assertEqual((late["processed"], late["sent"]), (1, len(queue.data())))
            else:
                self.assertEqual(queue.eofs()[1][1]["sent"], 0)

    def test_a_client_without_texts_still_sends_an_empty_eof_to_every_shard(self):
        self.run_processor(
            eof_message("c1", "classifier_0", 0, 0, 0),
            eof_message("c1", "classifier_1", 0, 0, 0),
        )
        for [(msg_type, value)] in self.all_eofs():
            self.assertEqual((value["processed"], value["total"], value["sent"]), (0, 0, 0))

    def test_clients_are_independent(self):
        self.run_processor(
            texts_message("c1", text(1, "sun")),
            texts_message("c2", text(2, "moon"), gateway_id=1),
            eof_message("c2", "classifier_0", 1, 1, 1, gateway_id=1),
            eof_message("c1", "classifier_0", 1, 1, 1),
            classifier_amount=1,
        )
        for queue in self.shards:
            eofs = {value["client_id"]: value for _, value in queue.eofs()}
            self.assertEqual(set(eofs), {"c1", "c2"})
            self.assertEqual((eofs["c2"]["gateway_id"], eofs["c2"]["processed"]), (1, 1))

    def test_a_finished_client_is_forgotten_once_there_are_no_batches_in_flight(self):
        self.run_processor(
            eof_message("c1", "classifier_0", 0, 0, 0),
            texts_message("c9", text(1, "sun")),
            texts_message("c1", text(2, "moon")),
            classifier_amount=1,
        )
        for queue in self.shards:
            self.assertEqual([value["client_id"] for _, value in queue.eofs()], ["c1"])

    def test_invalid_messages_are_discarded_and_acked(self):
        self.run_processor(b"no es un mensaje", classifier_amount=1)
        self.assertEqual(self.input.acks, 1)
        self.assertEqual(self.all_eofs(), [[], []])


if __name__ == "__main__":
    unittest.main()
