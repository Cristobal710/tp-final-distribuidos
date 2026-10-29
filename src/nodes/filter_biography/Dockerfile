FROM python:3.12-alpine

WORKDIR /app

COPY nodes/filter_biography/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    FILTER_BIOGRAPHY_ID=0 \
    FILTER_BIOGRAPHY_AMOUNT=1 \
    CLASSIFIER_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    BIO_QUEUE=bio \
    IMAGES_QUEUE=imagenes \
    CONTROL_EXCHANGE=control_filter_biography \
    MAX_BATCH_ROWS=200 \
    MAX_BATCH_BYTES=1048576 \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/filter_biography .

ENTRYPOINT ["python3", "main.py"]
