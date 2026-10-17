SHELL := /bin/bash
PWD := $(shell pwd)

up:
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

test:
	docker compose -f docker-compose.yaml build
	MSYS_NO_PATHCONV=1 docker run --rm -v "$(PWD):/repo" -w /repo --entrypoint python3 tp-client -m unittest discover -s tests -t . -v
.PHONY: test
