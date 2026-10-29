FROM python:3.12-alpine

WORKDIR /app

COPY nodes/image_downloader/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    IMAGE_DOWNLOADER_ID=0 \
    IMAGE_DOWNLOADER_AMOUNT=1 \
    FILTER_BIOGRAPHY_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    IMAGES_QUEUE=imagenes \
    CONTROL_EXCHANGE=control_image_downloader \
    RESULTS_QUEUE_PREFIX=resultados \
    DOWNLOAD_TIMEOUT_SECONDS=10 \
    DOWNLOAD_ATTEMPTS=3 \
    DOWNLOAD_RETRY_SECONDS=1 \
    MAX_IMAGE_BYTES=1048576 \
    MAX_RESULT_BYTES=8388608 \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/image_downloader .

ENTRYPOINT ["python3", "main.py"]
