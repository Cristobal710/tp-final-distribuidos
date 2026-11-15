FROM python:3.12-alpine

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    CHAOS_TARGETS= \
    CHAOS_INTERVAL_SECONDS=30 \
    CHAOS_KILLS_PER_ROUND=1 \
    CHAOS_SEED= \
    DOCKER_SOCKET=/var/run/docker.sock

COPY chaos_monkey .

ENTRYPOINT ["python3", "main.py"]
