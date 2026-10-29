import random
import unittest

from classifier import classify
from common.eof import UpstreamTracker
from common.protocol import internal
from common.protocol.articles import Article
from download import DownloadError
from filter_biography import FilterBiography
from image_downloader import ImageDownloader
from tests.q2_fakes import ConsumerQueue, GroupControl, OutputQueue


def make_articles():
    return [
        Article("Ada", "https://wiki/Ada", {"identifier": "en"}, 1000, 2000, "Ada...",
                '[{"name":"Biography"}]', {"content_url": "https://images/ada"}),
        Article("Emilie", "https://wiki/Emilie", {"identifier": "fr"}, 1000, 2000, "Emilie...",
                '[{"name":"BIOGRAPHIE"}]', {"content_url": "https://images/emilie"}),
        Article("History", "https://wiki/History", {}, 1000, 2000, "History...",
                '[{"name":"History"}]', {"content_url": "https://images/history"}),
        Article("Recent", "https://wiki/Recent", {}, 1700000000000, 1700000000000, "Recent...",
                "Biography", {"content_url": "https://images/recent"}),
        Article("No image", "https://wiki/NoImage", {}, 1000, 2000, "No image...", "Biography", None),
        Article("Broken", "https://wiki/Broken", {}, 1000, 2000, "Broken...",
                "Biography", {"content_url": "broken"}),
    ]


def classifier_streams(client_id, gateway_id, articles, batch_size):
    batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
    streams = []
    for classifier in range(2):
        mine = batches[classifier::2]
        stream = []
        for batch in mine:
            bios = classify(batch)[1]
            if bios:
                stream.append(internal.encode_bios(client_id, gateway_id, bios))
        stream.append(internal.encode_eof(
            client_id, gateway_id, f"classifier_{classifier}", len(mine), len(batches), len(stream),
        ))
        streams.append(stream)
    return streams


def interleave(streams, rng):
    streams = [list(stream) for stream in streams]
    messages = []
    while any(streams):
        stream = rng.choice([stream for stream in streams if stream])
        messages.append(stream.pop(0))
    return messages


class Q2PipelineTest(unittest.TestCase):

    def run_pipeline(self, clients, filters=2, downloaders=2, batch_size=2, seed=0, delay_bio=False, delay_image=False):
        rng = random.Random(seed)
        images, filter_control = OutputQueue(), GroupControl()
        filter_workers = [
            FilterBiography(i, ConsumerQueue(), None, filter_control, images, 2, 2, 1000)
            for i in range(filters)
        ]
        filter_control.workers = filter_workers
        streams = [stream for client, gateway, articles in clients
                   for stream in classifier_streams(client, gateway, articles, batch_size)]
        messages = interleave(streams, rng)
        if delay_bio:
            index = next(i for i, message in enumerate(messages) if internal.decode(message)[0] == internal.MsgType.BIOS)
            messages.append(messages.pop(index))
        for i, message in enumerate(messages):
            worker = filter_workers[i % filters]
            worker._bio_queue.deliver(worker, message)
            for peer in filter_workers:
                peer._bio_queue.pump()
        results, downloader_control = {}, GroupControl()

        def fetch(url, *, stop_event):
            if url == "broken":
                raise DownloadError("404")
            return url.encode("utf-8") + bytes(range(256))

        def open_results(gateway_id):
            return results.setdefault(gateway_id, OutputQueue())

        workers = [
            ImageDownloader(i, ConsumerQueue(), None, downloader_control, open_results, fetch, filters, 10_000)
            for i in range(downloaders)
        ]
        downloader_control.workers = workers
        messages = list(images.messages)
        if delay_image:
            index = next(i for i, message in enumerate(messages) if internal.decode(message)[0] == internal.MsgType.IMAGES)
            messages.append(messages.pop(index))
        try:
            for i, message in enumerate(messages):
                worker = workers[i % downloaders]
                worker._input_queue.deliver(worker, message)
                if worker._active is not None:
                    worker._input_queue.until(lambda: worker._active is None)
                for peer in workers:
                    peer._input_queue.pump()
            for client, gateway, articles in clients:
                expected = [(a.name, a.url, a.image["content_url"].encode("utf-8") + bytes(range(256)))
                            for a in articles if a.name in ("Ada", "Emilie")]
                actual = []
                tracker = UpstreamTracker(downloaders, check_total=True, check_received=True)
                for message in results[gateway].messages:
                    kind, value = internal.decode(message)
                    if value["client_id"] != client:
                        continue
                    self.assertEqual((value["query"], value["gateway_id"]), (2, gateway))
                    if kind == internal.MsgType.RESULTS:
                        actual.extend((name, url, bytes(data)) for name, url, data in value["rows"])
                        tracker.add_message(client)
                    elif kind == internal.MsgType.EOF:
                        tracker.add_eof(value)
                    elif kind == internal.MsgType.LATE_EOF:
                        tracker.add_late_eof(value)
                self.assertCountEqual(actual, expected)
                self.assertTrue(tracker.is_complete(client), (client, tracker._clients))
        finally:
            for worker in workers:
                worker.close()

    def test_classifier_to_q2_result_and_completion(self):
        self.run_pipeline([("c1", 0, make_articles())])

    def test_clients_are_isolated_even_when_sharing_a_gateway(self):
        self.run_pipeline([("a", 0, make_articles()), ("b", 4, make_articles()), ("c", 0, make_articles())])

    def test_no_matches_and_empty_input_complete(self):
        self.run_pipeline([("empty", 0, []), ("none", 0, make_articles()[2:])])

    def test_result_is_independent_of_replica_counts_and_message_order(self):
        for filters, downloaders in ((1, 1), (2, 3), (3, 2)):
            for seed in range(3):
                with self.subTest(filters=filters, downloaders=downloaders, seed=seed):
                    self.run_pipeline([("c1", 0, make_articles() * 3)], filters, downloaders, seed + 1, seed)

    def test_bio_batch_delayed_beyond_upstream_eof(self):
        self.run_pipeline([("c1", 0, make_articles())], delay_bio=True)

    def test_image_batch_delayed_beyond_upstream_eof(self):
        self.run_pipeline([("c1", 0, make_articles())], delay_image=True)
