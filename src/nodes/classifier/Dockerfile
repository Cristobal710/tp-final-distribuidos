FROM python:3.12-alpine

WORKDIR /app

COPY nodes/classifier/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    CLASSIFIER_ID=0 \
    CLASSIFIER_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    CLASSIFICATION_QUEUE=clasificacion \
    CONTROL_EXCHANGE=control_clasificador \
    TEXT_QUEUE=texto \
    BIO_QUEUE=bio \
    RESULTS_QUEUE_PREFIX=resultados \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/classifier .

ENTRYPOINT ["python3", "main.py"]
