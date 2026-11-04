import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
COUNTS_FILE = os.path.join(ROOT, "nodes.env")
COMPOSE_FILE = os.path.join(ROOT, "docker-compose.yaml")

HEADER = """x-dataset: &dataset
  DATASET_ROWS_PER_PATTERN: ${DATASET_ROWS_PER_PATTERN:-0}

services:
  rabbitmq:
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    image: tp-rabbitmq
    container_name: rabbitmq
    healthcheck:
      test: rabbitmq-diagnostics check_port_connectivity
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 50s
    ports:
      - 5672:5672
      - 15672:15672

  gateway:
    build:
      context: ./src
      dockerfile: gateway/Dockerfile
    image: tp-gateway
    container_name: gateway
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      GATEWAY_ID: 0
      CLASSIFIER_AMOUNT: {CLASSIFIER_AMOUNT}
      IMAGE_DOWNLOADER_AMOUNT: {IMAGE_DOWNLOADER_AMOUNT}
      COMPARATOR_AMOUNT: {COMPARATOR_AMOUNT}

"""

FOOTER = """  client_0:
    build:
      context: ./src
      dockerfile: client/Dockerfile
    image: tp-client
    container_name: client_0
    depends_on:
      - gateway
    environment:
      <<: *dataset
      SERVER_HOST: gateway
      MAX_BATCH_ROWS: 100
      RESULT_DIR: /results
    volumes:
      - ./results/client_0:/results

  expected:
    profiles: [tools]
    build:
      context: ./src
      dockerfile: tools/expected/Dockerfile
    image: tp-expected
    environment:
      <<: *dataset
    volumes:
      - ./results:/results
      - ./expected:/expected
      - ./datasets:/datasets
"""

IMAGE_DOWNLOADER_EXTRA = """      DOWNLOAD_INTERVAL_SECONDS: ${DOWNLOAD_INTERVAL_SECONDS:-1.5}
      IMAGE_CACHE_DIR: /cache/q2
      IMAGE_CACHE_MAX_BYTES: ${IMAGE_CACHE_MAX_BYTES:-268435456}
    volumes:
      - ./.cache/q2:/cache/q2
"""

NODES = [
    ("classifier", ["CLASSIFIER_AMOUNT"], ""),
    ("filter_biography", ["FILTER_BIOGRAPHY_AMOUNT", "CLASSIFIER_AMOUNT"], ""),
    ("image_downloader", ["IMAGE_DOWNLOADER_AMOUNT", "FILTER_BIOGRAPHY_AMOUNT"], IMAGE_DOWNLOADER_EXTRA),
    ("text_processor", ["TEXT_PROCESSOR_AMOUNT", "CLASSIFIER_AMOUNT", "WORD_COUNTER_AMOUNT", "AVERAGE_AMOUNT",
                        "COMPARATOR_AMOUNT"], ""),
    ("average", ["AVERAGE_AMOUNT", "TEXT_PROCESSOR_AMOUNT", "COMPARATOR_AMOUNT"], ""),
    ("comparator", ["COMPARATOR_AMOUNT", "TEXT_PROCESSOR_AMOUNT"], ""),
    ("word_counter", ["TEXT_PROCESSOR_AMOUNT", "JOIN_Q3_AMOUNT", "TOP20_AMOUNT"], ""),
    ("join_q3", ["WORD_COUNTER_AMOUNT"], ""),
    ("top20", ["WORD_COUNTER_AMOUNT"], ""),
]
COUNT_KEYS = {f"{name.upper()}_AMOUNT" for name, _, _ in NODES}


def read_counts(path):
    counts = {}
    with open(path, encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#"):
                key, _, value = line.partition("=")
                counts[key.strip()] = value.strip()
    if set(counts) != COUNT_KEYS:
        raise ValueError(f"nodes.env tiene que definir exactamente {sorted(COUNT_KEYS)}")
    for key, value in counts.items():
        if not value.isdigit() or int(value) < 1:
            raise ValueError(f"{key} tiene que ser un entero mayor a 0: {value!r}")
    return {key: int(value) for key, value in counts.items()}


def service(name, index, amounts, extra, counts):
    variables = "".join(f"      {amount}: {counts[amount]}\n" for amount in amounts)
    return (f"  {name}_{index}:\n"
            f"    build:\n"
            f"      context: ./src\n"
            f"      dockerfile: nodes/{name}/Dockerfile\n"
            f"    container_name: {name}_{index}\n"
            f"    depends_on:\n"
            f"      rabbitmq:\n"
            f"        condition: service_healthy\n"
            f"    environment:\n"
            f"      {name.upper()}_ID: {index}\n"
            f"{variables}{extra}\n")


def render(counts):
    header = HEADER
    for key in ("CLASSIFIER_AMOUNT", "IMAGE_DOWNLOADER_AMOUNT", "COMPARATOR_AMOUNT"):
        header = header.replace(f"{{{key}}}", str(counts[key]))
    services = "".join(
        service(name, index, amounts, extra, counts)
        for name, amounts, extra in NODES
        for index in range(counts[f"{name.upper()}_AMOUNT"])
    )
    return header + services + FOOTER


def main():
    try:
        compose = render(read_counts(COUNTS_FILE))
    except ValueError as e:
        print(e, file=sys.stderr)
        return 1
    with open(COMPOSE_FILE, "w", newline="\n", encoding="utf-8") as file:
        file.write(compose)
    return 0


if __name__ == "__main__":
    sys.exit(main())
