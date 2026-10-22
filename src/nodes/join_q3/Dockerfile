FROM python:3.12-alpine

WORKDIR /app

COPY nodes/join_q3/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    JOIN_Q3_ID=0 \
    WORD_COUNTER_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    JOIN_Q3_QUEUE_PREFIX=join_q3 \
    RESULTS_QUEUE_PREFIX=resultados \
    DATA_DIR=/data \
    MAX_BATCH_ROWS=2000 \
    MAX_BATCH_BYTES=1048576 \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/join_q3 .

ENTRYPOINT ["python3", "main.py"]
