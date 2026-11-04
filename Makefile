SHELL := /bin/bash

compose:
	python3 src/tools/compose/generate_compose.py
.PHONY: compose

up: compose
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs --follow
.PHONY: logs

CLIENT ?= client_0
EXPECTED ?= /expected
TOOLS = COMPOSE_IGNORE_ORPHANS=1 docker compose -f docker-compose.yaml --profile tools
CLEAN_RESULTS = MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint rm expected -rf

run: compose
	$(CLEAN_RESULTS) /results/$(CLIENT)
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker logs --follow $(CLIENT)
	test "$$(docker wait $(CLIENT))" = 0
.PHONY: run

hashes:
	$(TOOLS) build --quiet expected
	MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected hashes.py /results/$(CLIENT) $(EXPECTED)/hashes.txt
.PHONY: hashes

expected-hashes:
	$(TOOLS) build --quiet expected
	MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected hashes.py $(EXPECTED)
.PHONY: expected-hashes

SUBSETS_COMPOSE = docker compose -f docker-compose.yaml -f docker-compose.subsets.yaml
SUBSET_CLIENTS = client_0 client_1

subsets:
	$(TOOLS) build --quiet expected
	@for client in $(SUBSET_CLIENTS); do \
		subset=/datasets/$$client; \
		MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected subset.py $$subset && \
		MSYS_NO_PATHCONV=1 $(TOOLS) run --rm -e LOCAL_DATASET_DIR=$$subset -e OUTPUT_DIR=$$subset/expected expected && \
		MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected images.py $$subset/expected && \
		MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected hashes.py $$subset/expected || exit 1; \
	done
.PHONY: subsets

run-subsets: compose
	$(CLEAN_RESULTS) /results/client_0 /results/client_1
	COMPOSE_HTTP_TIMEOUT=300 $(SUBSETS_COMPOSE) up --build --remove-orphans --detach
	@codes=$$(docker wait client_0 client_1); $(SUBSETS_COMPOSE) logs client_0 client_1; test "$$(echo $$codes)" = "0 0"
.PHONY: run-subsets

hashes-subsets:
	$(TOOLS) build --quiet expected
	@status=0; for client in $(SUBSET_CLIENTS); do \
		echo "== $$client"; \
		MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected hashes.py /results/$$client /datasets/$$client/expected/hashes.txt || status=1; \
	done; exit $$status
.PHONY: hashes-subsets

expected:
	docker compose -f docker-compose.yaml --profile tools build expected
	$(CLEAN_RESULTS) /results/expected/q2_images.csv
	docker compose -f docker-compose.yaml --profile tools run --rm expected
	$(if $(filter-out 0,$(DATASET_ROWS_PER_PATTERN)),MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected images.py /results/expected)
	MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected hashes.py /results/expected
.PHONY: expected

pack-expected:
	mkdir -p expected
	for file in results/expected/q*.csv; do gzip -9 -n -c "$$file" > "expected/$$(basename "$$file").gz"; done
	ls -l expected
.PHONY: pack-expected

compare:
	$(TOOLS) build --quiet expected
	MSYS_NO_PATHCONV=1 $(TOOLS) run --rm --entrypoint python3 expected compare.py $(EXPECTED) /results/$(CLIENT)
.PHONY: compare

test:
	docker compose -f docker-compose.yaml build
	MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR):/repo" -w /repo --entrypoint python3 tp-client -m unittest discover -s tests -t . -v
.PHONY: test

test-full:
	python3 -m tests.full_suite.run
.PHONY: test-full

test-publisher:
	python3 -m tests.run_publisher_check
.PHONY: test-publisher
