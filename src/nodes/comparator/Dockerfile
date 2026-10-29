FROM python:3.12-alpine

WORKDIR /app

COPY nodes/comparator/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    COMPARATOR_ID=0 \
    COMPARATOR_AMOUNT=1 \
    TEXT_PROCESSOR_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    COMPARISON_QUEUE_PREFIX=comparacion \
    FINAL_AVERAGE_QUEUE_PREFIX=promedio_final \
    RESULTS_QUEUE_PREFIX=resultados \
    DATA_DIR=/data \
    MAX_BATCH_ROWS=2000 \
    MAX_BATCH_BYTES=1048576 \
    DRAIN_BATCH_ROWS=1000 \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/comparator .

ENTRYPOINT ["python3", "main.py"]
