import collections
import contextlib
import csv
import datetime
import logging
import os
import re
import shutil
import sys
import tempfile

from dataset import KaggleDataset, read_rows
from dataset.local import local_rows

COLUMNS = ["name", "url", "in_language", "date_created", "date_modified", "abstract", "sections", "image"]
START_OF_2021 = datetime.datetime(2021, 1, 1)
WORD_REGEX = re.compile(r"\b\w+\b")
NUMBER_REGEX = re.compile(r"[+-]?((\d+\.?\d*)|(\.\d+))")
BIOGRAPHY_REGEX = re.compile(r"biography|biographie", re.IGNORECASE)
TOP_SIZE = 20
HEADERS = {
    1: ["url", "date_modified"],
    2: ["name", "url", "image_url"],
    3: ["word"],
    4: ["word", "count"],
    5: ["url", "word_count"],
}


def load_stop_words():
    from nltk.corpus import stopwords
    return set(stopwords.words("english")) | set(stopwords.words("french"))


def unique_words(abstract, stop_words):
    return {
        word for word in WORD_REGEX.findall(abstract.lower())
        if word not in stop_words and not NUMBER_REGEX.fullmatch(word)
    }


def iso_utc(moment):
    return moment.replace(tzinfo=datetime.timezone.utc).isoformat()


def articles_of(dataset, rows_per_pattern, rows_per_read):
    sent = {}
    with contextlib.closing(dataset.files()) as files:
        for pattern, file_path, local_path in files:
            if sent.get(pattern, 0) == rows_per_pattern > 0:
                continue
            logging.info(f"Leyendo {file_path}")
            for row in read_rows(local_path, COLUMNS, rows_per_read):
                yield row
                sent[pattern] = sent.get(pattern, 0) + 1
                if sent[pattern] == rows_per_pattern:
                    dataset.skip_pattern(pattern)
                    break


class ExpectedResults:

    def __init__(self, output_dir, stop_words):
        os.makedirs(output_dir, exist_ok=True)
        self._output_dir = output_dir
        self._stop_words = stop_words
        self._files = {query: self._open(query) for query in (1, 2)}
        self._writers = {query: csv.writer(file) for query, file in self._files.items()}
        for query, writer in self._writers.items():
            writer.writerow(HEADERS[query])
        self._english = set()
        self._french = set()
        self._articles_per_word = collections.Counter()
        self._words_until_2020 = 0
        self._articles_until_2020 = 0
        self._since_2021 = []
        self.articles = 0

    def add(self, article):
        self.articles += 1
        created = article["date_created"]
        until_2020 = None if created is None else created < START_OF_2021
        language = (article["in_language"] or {}).get("identifier")
        if until_2020 and language == "en" and article["date_modified"] is not None:
            self._writers[1].writerow([article["url"], iso_utc(article["date_modified"])])
        image_url = (article["image"] or {}).get("content_url")
        sections = article["sections"]
        if until_2020 and image_url and sections and BIOGRAPHY_REGEX.search(sections):
            self._writers[2].writerow([article["name"], article["url"], image_url])
        if article["abstract"] is None:
            return
        words = unique_words(article["abstract"], self._stop_words)
        if language == "en":
            self._english |= words
        elif language == "fr":
            self._french |= words
        self._articles_per_word.update(words)
        if until_2020 is True:
            self._words_until_2020 += len(words)
            self._articles_until_2020 += 1
        elif until_2020 is False:
            self._since_2021.append((article["url"], len(words)))

    def finish(self):
        for file in self._files.values():
            file.close()
        self._write(3, ([word] for word in sorted(self._english & self._french)))
        top = sorted(self._articles_per_word.items(), key=lambda item: (-item[1], item[0]))[:TOP_SIZE]
        self._write(4, ([word, count] for word, count in top))
        average = self._words_until_2020 / self._articles_until_2020 if self._articles_until_2020 else 0
        self._write(5, ([url, count] for url, count in self._since_2021 if count > average))
        return average

    def _open(self, query):
        return open(os.path.join(self._output_dir, f"q{query}.csv"), "w", newline="", encoding="utf-8")

    def _write(self, query, rows):
        with self._open(query) as file:
            writer = csv.writer(file)
            writer.writerow(HEADERS[query])
            writer.writerows(rows)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    output_dir = os.environ["OUTPUT_DIR"]
    os.makedirs(os.environ["DOWNLOAD_DIR"], exist_ok=True)
    download_dir = tempfile.mkdtemp(prefix="expected-", dir=os.environ["DOWNLOAD_DIR"])
    patterns = [pattern.strip() for pattern in os.environ["DATASET_FILE_PATTERNS"].split(",") if pattern.strip()]
    dataset = KaggleDataset(
        download_dir,
        os.environ["KAGGLE_DATASET"],
        patterns,
        int(os.environ["DATASET_FILES_PER_PATTERN"]),
        int(os.environ["DOWNLOAD_FILES_AHEAD"]),
        float(os.environ["DOWNLOAD_POLL_SECONDS"]),
        float(os.environ["DOWNLOAD_STOP_TIMEOUT_SECONDS"]),
    )
    expected = ExpectedResults(output_dir, load_stop_words())
    rows_per_pattern = int(os.environ["DATASET_ROWS_PER_PATTERN"])
    rows_per_read = int(os.environ["PARQUET_ROWS_PER_READ"])
    try:
        local_dir = os.environ["LOCAL_DATASET_DIR"]
        articles = (local_rows(local_dir, COLUMNS, rows_per_read) if local_dir
                    else articles_of(dataset, rows_per_pattern, rows_per_read))
        for article in articles:
            expected.add(article)
        average = expected.finish()
    finally:
        shutil.rmtree(download_dir, ignore_errors=True)
    logging.info(f"{expected.articles} artículos procesados, promedio de palabras hasta 2020: {average:.2f}")
    logging.info(f"Resultados esperados en {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
