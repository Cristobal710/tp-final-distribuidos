import threading
import unittest

from common.protocol import internal
from common.protocol.articles import Article
from classifier import Classifier

DATE_2005 = 1104537600000
DATE_2020_LAST_MS = 1609459199999
DATE_2021 = 1609459200000
DATE_2023 = 1672531200000
IMAGE = {"content_url": "https://upload.wikimedia.org/x.jpg", "height": 10, "width": 10}
EOF = internal.MsgType.EOF
LATE_EOF = internal.MsgType.LATE_EOF


def article(i, language="fr", date_created=DATE_2005, date_modified=DATE_2023, image=IMAGE):
    return Article(f"Articulo {i}", f"https://{language}.wikipedia.org/wiki/{i}", {"identifier": language},
                   date_created, date_modified, f"Resumen {i}", f'[{{"name":"Seccion {i}"}}]', image)


def gateway_eof(client_id, batches, gateway_id=0):
    return internal.encode_eof(client_id, gateway_id, f"gateway_{gateway_id}", batches, batches, batches)


def control(message):
    return ("control", message)


def noop():
    pass


class FakeControlInput:

    def __init__(self):
        self.callback = None
        self.ready = threading.Event()
        self.released = threading.Event()

    def start_consuming(self, callback):
        self.callback = callback
        self.ready.set()
        self.released.wait()

    def stop_consuming(self):
        self.released.set()

    def deliver(self, message):
        self.callback(message, noop, noop)


class FakeControlOutput:

    def __init__(self, control_input):
        self.control_input = control_input
        self.sent = []

    def send(self, message):
        self.sent.append(message)
        self.control_input.deliver(message)


class FakeClassificationQueue:

    def __init__(self, events, control_input):
        self.events = list(events)
        self.control_input = control_input
        self.pending = []
        self.acks = 0
        self.nacks = 0
        self.stopped = False

    def start_consuming(self, callback):
        self.control_input.ready.wait(timeout=5)
        for event in self.events:
            self._run_pending()
            if isinstance(event, tuple):
                self.control_input.deliver(event[1])
            else:
                callback(event, self._ack, self._nack)
        self._run_pending()

    def stop_consuming(self):
        self.stopped = True

    def run_in_consumer_thread(self, callback):
        self.pending.append(callback)

    def _run_pending(self):
        while self.pending:
            self.pending.pop(0)()

    def _ack(self):
        self.acks += 1

    def _nack(self):
        self.nacks += 1


class FakeOutputQueue:

    def __init__(self):
        self.sent = []
        self.closed = False

    def send(self, message):
        self.sent.append(internal.decode(message))

    def close(self):
        self.closed = True

    def data(self):
        return [value for msg_type, value in self.sent if msg_type not in (EOF, LATE_EOF)]

    def eofs(self):
        return [(msg_type, value) for msg_type, value in self.sent if msg_type in (EOF, LATE_EOF)]


class ClassifierTest(unittest.TestCase):

    def run_classifier(self, *events):
        control_input = FakeControlInput()
        self.control_output = FakeControlOutput(control_input)
        self.input = FakeClassificationQueue(events, control_input)
        self.texts = FakeOutputQueue()
        self.bios = FakeOutputQueue()
        self.results = {}

        def open_results_queue(gateway_id):
            self.results[gateway_id] = FakeOutputQueue()
            return self.results[gateway_id]

        self.classifier = Classifier(3, self.input, control_input, self.control_output,
                                     self.texts, self.bios, open_results_queue, 1)
        with self.assertLogs(level="INFO") as logs:
            self.classifier.run()
        return "\n".join(logs.output)

    def test_every_article_goes_to_texto_with_its_period_and_language(self):
        self.run_classifier(internal.encode_articles("c1", 0, [
            article(1, "en"),
            article(2, "fr", date_created=DATE_2023),
        ]))
        [value] = self.texts.data()
        self.assertEqual((value["client_id"], value["gateway_id"]), ("c1", 0))
        self.assertEqual(value["texts"], [
            internal.Text("https://en.wikipedia.org/wiki/1", "Resumen 1", True, "en"),
            internal.Text("https://fr.wikipedia.org/wiki/2", "Resumen 2", False, "fr"),
        ])

    def test_only_articles_until_2020_with_sections_and_image_go_to_bio(self):
        self.run_classifier(internal.encode_articles("c1", 0, [
            article(1, "fr"),
            article(2, "en", date_created=DATE_2021),
            article(3, "en", image=None),
            article(4, "en")._replace(sections=None),
            article(5, "en", date_created=DATE_2020_LAST_MS),
        ]))
        [value] = self.bios.data()
        self.assertEqual(value["bios"], [
            internal.Bio("Articulo 1", "https://fr.wikipedia.org/wiki/1", '[{"name":"Seccion 1"}]', IMAGE["content_url"]),
            internal.Bio("Articulo 5", "https://en.wikipedia.org/wiki/5", '[{"name":"Seccion 5"}]', IMAGE["content_url"]),
        ])

    def test_only_english_articles_until_2020_go_to_q1_of_their_gateway(self):
        self.run_classifier(
            internal.encode_articles("c1", 0, [article(1, "en"), article(2, "fr"), article(3, "en", date_created=DATE_2023)]),
            internal.encode_articles("c2", 1, [article(4, "en", date_created=DATE_2020_LAST_MS)]),
        )
        [value] = self.results[0].data()
        self.assertEqual((value["client_id"], value["query"]), ("c1", 1))
        self.assertEqual(value["rows"], [["https://en.wikipedia.org/wiki/1", DATE_2023]])
        [value] = self.results[1].data()
        self.assertEqual((value["client_id"], value["rows"]), ("c2", [["https://en.wikipedia.org/wiki/4", DATE_2023]]))

    def test_without_date_created_goes_to_texto_without_period_and_nowhere_else(self):
        self.run_classifier(internal.encode_articles("c1", 0, [
            article(1, "en", date_created=None, date_modified=DATE_2005),
        ]))
        [value] = self.texts.data()
        self.assertEqual(value["texts"], [internal.Text("https://en.wikipedia.org/wiki/1", "Resumen 1", None, "en")])
        self.assertEqual(self.bios.sent, [])
        self.assertEqual(self.results, {})

    def test_without_abstract_does_not_go_to_texto(self):
        self.run_classifier(internal.encode_articles("c1", 0, [
            article(1, "en")._replace(abstract=None),
            article(2, "en"),
        ]))
        [value] = self.texts.data()
        self.assertEqual([text.url for text in value["texts"]], ["https://en.wikipedia.org/wiki/2"])
        self.assertEqual(len(self.bios.data()[0]["bios"]), 2)

    def test_q1_ignores_articles_without_date_modified(self):
        self.run_classifier(internal.encode_articles("c1", 0, [
            article(1, "en", date_modified=None),
            article(2, "en"),
        ]))
        [value] = self.results[0].data()
        self.assertEqual(value["rows"], [["https://en.wikipedia.org/wiki/2", DATE_2023]])

    def test_sends_nothing_where_no_article_applies(self):
        self.run_classifier(internal.encode_articles("c1", 0, [article(1, "fr", date_created=DATE_2023)]))
        self.assertEqual(len(self.texts.data()), 1)
        self.assertEqual(self.bios.sent, [])
        self.assertEqual(self.results, {})

    def test_gateway_eof_is_forwarded_to_every_classifier(self):
        eof = gateway_eof("c1", 1)
        self.run_classifier(internal.encode_articles("c1", 0, [article(1, "en")]), eof)
        self.assertEqual(self.control_output.sent, [eof])

    def test_sends_its_eof_with_processed_total_and_sent_to_each_destination(self):
        self.run_classifier(
            internal.encode_articles("c1", 0, [article(1, "en")]),
            internal.encode_articles("c1", 0, [article(2, "fr", date_created=DATE_2023)]),
            control(gateway_eof("c1", 5)),
        )
        [(msg_type, value)] = self.texts.eofs()
        self.assertEqual(msg_type, EOF)
        self.assertEqual((value["origin"], value["processed"], value["total"], value["sent"]), ("classifier_3", 2, 5, 2))
        self.assertNotIn("query", value)
        [(_, value)] = self.bios.eofs()
        self.assertEqual((value["processed"], value["total"], value["sent"]), (2, 5, 1))
        [(_, value)] = self.results[0].eofs()
        self.assertEqual((value["processed"], value["total"], value["sent"], value["query"]), (2, 5, 1, 1))

    def test_sends_its_eof_even_without_batches_of_that_client(self):
        self.run_classifier(control(gateway_eof("c1", 4, gateway_id=2)))
        [(_, value)] = self.texts.eofs()
        self.assertEqual((value["processed"], value["total"], value["sent"]), (0, 4, 0))
        [(_, value)] = self.results[2].eofs()
        self.assertEqual((value["sent"], value["query"]), (0, 1))

    def test_batch_after_its_eof_is_sent_with_a_late_eof(self):
        self.run_classifier(
            internal.encode_articles("c1", 0, [article(1, "en")]),
            control(gateway_eof("c1", 2)),
            internal.encode_articles("c1", 0, [article(2, "fr", date_created=DATE_2023)]),
        )
        types = [msg_type for msg_type, _ in self.texts.sent]
        self.assertEqual(types, [internal.MsgType.TEXTS, EOF, internal.MsgType.TEXTS, LATE_EOF])
        late = self.texts.sent[-1][1]
        self.assertEqual((late["origin"], late["processed"], late["sent"]), ("classifier_3", 1, 1))
        [_, (msg_type, late)] = self.bios.eofs()
        self.assertEqual((msg_type, late["sent"]), (LATE_EOF, 0))
        [_, (msg_type, late)] = self.results[0].eofs()
        self.assertEqual((msg_type, late["sent"], late["query"]), (LATE_EOF, 0, 1))

    def test_eof_of_one_client_does_not_affect_another(self):
        self.run_classifier(
            internal.encode_articles("c1", 0, [article(1)]),
            internal.encode_articles("c2", 0, [article(2)]),
            control(gateway_eof("c1", 1)),
            internal.encode_articles("c2", 0, [article(3)]),
            control(gateway_eof("c2", 2)),
        )
        eofs = [(value["client_id"], msg_type, value["processed"]) for msg_type, value in self.texts.eofs()]
        self.assertEqual(eofs, [("c1", EOF, 1), ("c2", EOF, 2)])

    def test_acks_each_message(self):
        self.run_classifier(internal.encode_articles("c1", 0, [article(1)]), gateway_eof("c1", 1))
        self.assertEqual((self.input.acks, self.input.nacks), (2, 0))

    def test_invalid_messages_are_discarded(self):
        self.run_classifier(b"no es un mensaje", gateway_eof("c1", 0))
        self.assertEqual(self.input.acks, 2)

    def test_close_closes_results_queues(self):
        self.run_classifier(internal.encode_articles("c1", 0, [article(1, "en")]))
        self.classifier.close()
        self.assertTrue(self.results[0].closed)

    def test_stop_stops_consuming(self):
        control_input = FakeControlInput()
        queue = FakeClassificationQueue([], control_input)
        Classifier(0, queue, control_input, FakeControlOutput(control_input),
                   FakeOutputQueue(), FakeOutputQueue(), FakeOutputQueue, 1).stop()
        self.assertTrue(queue.stopped)


if __name__ == "__main__":
    unittest.main()
