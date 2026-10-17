import contextlib
import logging
import os
import shutil
import signal
import sys
import tempfile

from sender import Client
from dataset import KaggleDataset, read_rows

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
KAGGLE_DATASET = os.environ["KAGGLE_DATASET"]
DATASET_FILE_PATTERNS = os.environ["DATASET_FILE_PATTERNS"]
DATASET_FILES_PER_PATTERN = int(os.environ["DATASET_FILES_PER_PATTERN"])
DATASET_MAX_ROWS = int(os.environ["DATASET_MAX_ROWS"])
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


# Pasa un texto separado por comas a una lista, sin espacios ni elementos vacios.
def parse_list(value):
    return [item.strip() for item in value.split(",") if item.strip()]


# Devuelve las filas de todos los archivos a medida que se van bajando.
def rows_of(dataset, client):
    columns = parse_list(CLIENT_COLUMNS) or None
    sent = 0
    with contextlib.closing(dataset.files()) as files:
        for file_path, local_path in files:
            if client.stopped:
                return
            logging.info(f"Mandando {file_path}")
            for row in read_rows(local_path, columns, PARQUET_ROWS_PER_READ):
                if sent == DATASET_MAX_ROWS > 0:
                    return
                yield row
                sent += 1


# Baja el dataset y lo manda al gateway, cortando limpio ante SIGTERM y borrando lo bajado al final.
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    download_dir = tempfile.mkdtemp(prefix="dataset-", dir=DOWNLOAD_DIR)
    dataset = KaggleDataset(
        download_dir,
        KAGGLE_DATASET,
        parse_list(DATASET_FILE_PATTERNS),
        DATASET_FILES_PER_PATTERN,
        DOWNLOAD_FILES_AHEAD,
        DOWNLOAD_POLL_SECONDS,
        DOWNLOAD_STOP_TIMEOUT_SECONDS,
    )
    client = Client(
        SERVER_HOST, SERVER_PORT, MAX_BATCH_ROWS, MAX_BATCH_BYTES, CONNECT_ATTEMPTS, CONNECT_RETRY_SECONDS
    )
    signal.signal(signal.SIGTERM, lambda signum, frame: client.stop())

    try:
        client.connect()
        client.send_rows(rows_of(dataset, client))
    except OSError as e:
        if client.stopped:
            logging.info("Envío cortado por SIGTERM")
            return 0
        logging.error(f"Se perdió la conexión con el gateway: {e}")
        return 1
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        return 2
    finally:
        client.close()
        shutil.rmtree(download_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
