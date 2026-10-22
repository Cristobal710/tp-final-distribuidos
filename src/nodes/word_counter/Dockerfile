FROM python:3.12-alpine

WORKDIR /app

COPY nodes/word_counter/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    WORD_COUNTER_ID=0 \
    TEXT_PROCESSOR_AMOUNT=1 \
    JOIN_Q3_AMOUNT=1 \
    TOP20_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    WORDS_QUEUE_PREFIX=palabras \
    JOIN_Q3_QUEUE_PREFIX=join_q3 \
    TOP20_QUEUE_PREFIX=top20 \
    DATA_DIR=/data \
    TOP_SIZE=20 \
    MAX_BATCH_ROWS=2000 \
    MAX_BATCH_BYTES=1048576 \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/word_counter .

ENTRYPOINT ["python3", "main.py"]
