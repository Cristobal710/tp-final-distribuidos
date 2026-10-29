FROM python:3.12-alpine

WORKDIR /app

COPY nodes/average/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    AVERAGE_ID=0 \
    AVERAGE_AMOUNT=1 \
    TEXT_PROCESSOR_AMOUNT=1 \
    COMPARATOR_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    AVERAGE_QUEUE_PREFIX=promedio \
    FINAL_AVERAGE_QUEUE_PREFIX=promedio_final \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/average .

ENTRYPOINT ["python3", "main.py"]
