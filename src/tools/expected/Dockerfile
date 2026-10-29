FROM python:3.12-slim

WORKDIR /app

COPY tools/expected/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
RUN python3 -m nltk.downloader -d /usr/share/nltk_data stopwords

ENV PYTHONUNBUFFERED=1 \
    NLTK_DATA=/usr/share/nltk_data \
    LOCAL_DATASET_DIR= \
    KAGGLE_DATASET=wikimedia-foundation/wikipedia-structured-contents \
    DATASET_FILE_PATTERNS=enwiki/data/enwiki_namespace_0_{:05}.parquet,frwiki/data/frwiki_namespace_0_{:05}.parquet \
    DATASET_FILES_PER_PATTERN=0 \
    DATASET_ROWS_PER_PATTERN=0 \
    PARQUET_ROWS_PER_READ=2048 \
    DOWNLOAD_DIR=/tmp/downloads \
    DOWNLOAD_FILES_AHEAD=1 \
    DOWNLOAD_POLL_SECONDS=0.5 \
    DOWNLOAD_STOP_TIMEOUT_SECONDS=5 \
    OUTPUT_DIR=/results/expected

COPY client/dataset ./dataset
COPY tools/expected .

ENTRYPOINT ["python3", "expected.py"]
