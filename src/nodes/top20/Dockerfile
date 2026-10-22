FROM python:3.12-alpine

WORKDIR /app

COPY nodes/top20/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    TOP20_ID=0 \
    WORD_COUNTER_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    TOP20_QUEUE_PREFIX=top20 \
    RESULTS_QUEUE_PREFIX=resultados \
    TOP_SIZE=20 \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/top20 .

ENTRYPOINT ["python3", "main.py"]
