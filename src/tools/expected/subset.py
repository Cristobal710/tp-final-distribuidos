import collections
import datetime
import json
import os
import shutil
import sys
import tempfile

import kagglehub
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from expected import COLUMNS

LIST_FILE = "articles.txt"
PARQUET_FILE = "articles.parquet"
JSON_FILE = "articles.json"
COMMENT = "#"


def read_list(path):
    entries = []
    with open(path, encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith(COMMENT):
                file_path, url = line.split()
                entries.append((file_path, url))
    return entries


def select_articles(entries, download):
    urls_by_file = collections.defaultdict(set)
    for file_path, url in entries:
        urls_by_file[file_path].add(url)
    tables = []
    for file_path, urls in urls_by_file.items():
        table = pq.read_table(download(file_path), columns=COLUMNS)
        tables.append(table.filter(pc.is_in(table["url"], value_set=pa.array(sorted(urls)))))
    table = pa.concat_tables(tables, promote_options="default")
    positions = {url: index for index, url in enumerate(table["url"].to_pylist())}
    missing = [url for _, url in entries if url not in positions]
    if missing:
        raise ValueError(f"No se encontraron en el dataset: {missing}")
    return table.take([positions[url] for _, url in entries])


def readable(article):
    value = dict(article)
    for key, field in value.items():
        if isinstance(field, datetime.datetime):
            value[key] = field.isoformat()
    if value["sections"] is not None:
        value["sections"] = json.loads(value["sections"])
    return value


def write_subset(directory, table):
    pq.write_table(table, os.path.join(directory, PARQUET_FILE))
    with open(os.path.join(directory, JSON_FILE), "w", encoding="utf-8") as file:
        json.dump([readable(article) for article in table.to_pylist()], file, ensure_ascii=False, indent=2)
        file.write("\n")


def main():
    if len(sys.argv) != 2:
        print("Uso: subset.py <directorio del subset>")
        return 2
    directory = sys.argv[1]
    os.makedirs(os.environ["DOWNLOAD_DIR"], exist_ok=True)
    download_dir = tempfile.mkdtemp(prefix="subset-", dir=os.environ["DOWNLOAD_DIR"])
    try:
        table = select_articles(
            read_list(os.path.join(directory, LIST_FILE)),
            lambda file_path: kagglehub.dataset_download(
                os.environ["KAGGLE_DATASET"], path=file_path, output_dir=download_dir
            ),
        )
    finally:
        shutil.rmtree(download_dir, ignore_errors=True)
    write_subset(directory, table)
    print(f"{directory}: {table.num_rows} artículos")
    return 0


if __name__ == "__main__":
    sys.exit(main())
