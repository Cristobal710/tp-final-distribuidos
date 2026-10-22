FROM python:3.12-slim

WORKDIR /app

COPY nodes/text_processor/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
RUN python3 -m nltk.downloader -d /usr/share/nltk_data stopwords

ENV PYTHONUNBUFFERED=1 \
    NLTK_DATA=/usr/share/nltk_data \
    TEXT_PROCESSOR_ID=0 \
    TEXT_PROCESSOR_AMOUNT=1 \
    CLASSIFIER_AMOUNT=1 \
    WORD_COUNTER_AMOUNT=1 \
    RABBITMQ_HOST=rabbitmq \
    TEXT_QUEUE=texto \
    CONTROL_EXCHANGE=control_text_processor \
    WORDS_QUEUE_PREFIX=palabras \
    MAX_BATCH_ROWS=2000 \
    MAX_BATCH_BYTES=1048576 \
    PUBLISHER_IDLE_SECONDS=30

COPY common ./common
COPY nodes/text_processor .

ENTRYPOINT ["python3", "main.py"]
