FROM python:3.12-alpine

WORKDIR /app

COPY gateway/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    SERVER_HOST=0.0.0.0 \
    SERVER_PORT=5678 \
    GATEWAY_ID=0 \
    RABBITMQ_HOST=rabbitmq \
    CLASSIFICATION_QUEUE=clasificacion

COPY common ./common
COPY gateway .

ENTRYPOINT ["python3", "main.py"]
