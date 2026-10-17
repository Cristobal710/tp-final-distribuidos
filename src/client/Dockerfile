FROM python:3.12-slim

WORKDIR /app

COPY client/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    SERVER_PORT=5678 \
    KAGGLE_DATASET=wikimedia-foundation/wikipedia-structured-contents \
    DATASET_FILE_PATTERNS=enwiki/data/enwiki_namespace_0_{:05}.parquet,frwiki/data/frwiki_namespace_0_{:05}.parquet \
    DATASET_FILES_PER_PATTERN=0 \
    CLIENT_COLUMNS=name,url,in_language,date_created,date_modified,abstract,sections,image \
    PARQUET_ROWS_PER_READ=2048 \
    MAX_BATCH_ROWS=200 \
    MAX_BATCH_BYTES=1048576 \
    DOWNLOAD_DIR=/tmp/downloads \
    DOWNLOAD_FILES_AHEAD=1 \
    DOWNLOAD_POLL_SECONDS=0.5 \
    DOWNLOAD_STOP_TIMEOUT_SECONDS=5

COPY common ./common
COPY client .

ENTRYPOINT ["python3", "main.py"]
