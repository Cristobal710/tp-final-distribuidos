import contextlib
import logging
import os
import shutil
import signal
import sys
import tempfile
import json
import threading
from pathlib import Path

from sender import Client
from dataset import KaggleDataset, read_rows
from dataset.local import local_rows
from receiver import ResultReceiver

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
KAGGLE_DATASET = os.environ["KAGGLE_DATASET"]
DATASET_FILE_PATTERNS = os.environ["DATASET_FILE_PATTERNS"]
DATASET_FILES_PER_PATTERN = int(os.environ["DATASET_FILES_PER_PATTERN"])
DATASET_ROWS_PER_PATTERN = int(os.environ["DATASET_ROWS_PER_PATTERN"])
CLIENT_COLUMNS = os.environ["CLIENT_COLUMNS"]
PARQUET_ROWS_PER_READ = int(os.environ["PARQUET_ROWS_PER_READ"])
MAX_BATCH_ROWS = int(os.environ["MAX_BATCH_ROWS"])
MAX_BATCH_BYTES = int(os.environ["MAX_BATCH_BYTES"])
CONNECT_ATTEMPTS = int(os.environ["CONNECT_ATTEMPTS"])
CONNECT_RETRY_SECONDS = float(os.environ["CONNECT_RETRY_SECONDS"])
DOWNLOAD_DIR = os.environ["DOWNLOAD_DIR"]
DOWNLOAD_FILES_AHEAD = int(os.environ["DOWNLOAD_FILES_AHEAD"])
DOWNLOAD_POLL_SECONDS = float(os.environ["DOWNLOAD_POLL_SECONDS"])
DOWNLOAD_STOP_TIMEOUT_SECONDS = float(os.environ["DOWNLOAD_STOP_TIMEOUT_SECONDS"])
LOCAL_DATASET_DIR = os.environ.get("LOCAL_DATASET_DIR", "")
RESULT_DIR = os.environ.get("RESULT_DIR", "/tmp/results")
RESULT_TIMEOUT_SECONDS = float(os.environ.get("RESULT_TIMEOUT_SECONDS", "7200"))


# Pasa un texto separado por comas a una lista, sin espacios ni elementos vacios.
def parse_list(value):
    return [item.strip() for item in value.split(",") if item.strip()]


# Devuelve las filas de los archivos a medida que se van bajando, hasta el tope de filas de cada patron.
def rows_of(dataset, client):
    columns = parse_list(CLIENT_COLUMNS) or None
    sent = {}
    with contextlib.closing(dataset.files()) as files:
        for pattern, file_path, local_path in files:
            if client.stopped:
                return
            if sent.get(pattern, 0) == DATASET_ROWS_PER_PATTERN > 0:
                continue
            logging.info(f"Mandando {file_path}")
            for row in read_rows(local_path, columns, PARQUET_ROWS_PER_READ):
                yield row
                sent[pattern] = sent.get(pattern, 0) + 1
                if sent[pattern] == DATASET_ROWS_PER_PATTERN:
                    dataset.skip_pattern(pattern)
                    break


# Baja el dataset y lo manda al gateway, cortando limpio ante SIGTERM y borrando lo bajado al final.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    download_dir = None
    dataset = None
    if not LOCAL_DATASET_DIR:
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        download_dir = tempfile.mkdtemp(prefix="dataset-", dir=DOWNLOAD_DIR)
        dataset = KaggleDataset(
            download_dir, KAGGLE_DATASET, parse_list(DATASET_FILE_PATTERNS), DATASET_FILES_PER_PATTERN,
            DOWNLOAD_FILES_AHEAD, DOWNLOAD_POLL_SECONDS, DOWNLOAD_STOP_TIMEOUT_SECONDS,
        )
    client = Client(
        SERVER_HOST, SERVER_PORT, MAX_BATCH_ROWS, MAX_BATCH_BYTES, CONNECT_ATTEMPTS, CONNECT_RETRY_SECONDS
    )
    termination_requested = threading.Event()

    def stop(signum, frame):
        termination_requested.set()
        client.stop()

    signal.signal(signal.SIGTERM, stop)
    receiver = None

    try:
        client.connect()
        receiver = ResultReceiver(client.socket, RESULT_DIR, client.stop)
        receiver.start()
        rows = (local_rows(LOCAL_DATASET_DIR, parse_list(CLIENT_COLUMNS) or None, PARQUET_ROWS_PER_READ)
                if LOCAL_DATASET_DIR else rows_of(dataset, client))
        client.send_rows(rows)
        receiver.wait(RESULT_TIMEOUT_SECONDS)
        Path(RESULT_DIR, "summary.json").write_text(json.dumps({
            "sent_rows": client.sent_rows, "sent_batches": client.sent_batches,
            "received_rows": receiver.rows, "received_messages": receiver.messages,
            "finished_queries": sorted(receiver.finished_queries), "all_done": receiver.all_done,
        }, indent=2))
    except OSError as e:
        if termination_requested.is_set():
            logging.info("Envío cortado por SIGTERM")
            return 0
        logging.error(f"Se perdió la conexión con el gateway: {e}")
        return 1
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        return 2
    finally:
        client.stop()
        if receiver is not None and receiver.ident is not None:
            receiver.join(timeout=5)
        client.close()
        if download_dir is not None:
            shutil.rmtree(download_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
