GO ?= go
COMPOSE = docker compose -f deployments/compose.yaml

.PHONY: fmt fmt-check lint test race integration demo build run docker-build compose-up compose-down check

fmt:
	$(GO) fmt ./...

# fmt-check requires a POSIX shell (Linux, macOS, WSL or Git Bash).
fmt-check:
	@unformatted=$$(gofmt -l cmd internal tests) || exit 1; test -z "$$unformatted" || (echo "$$unformatted"; exit 1)

lint:
	$(GO) vet ./...

test:
	$(GO) test ./... -count=1

race:
	$(GO) test -race ./... -count=1

integration:
	$(GO) test -tags=integration ./tests/integration/... -count=1 -timeout=10m

# Real API binary + disposable PostgreSQL/RabbitMQ. No existing Compose data.
demo:
	$(GO) test -tags=integration ./tests/integration/... -run TestProcessRecoveryDemo -count=1 -timeout=7m -v

build:
	$(GO) build -trimpath -o bin/api ./cmd/api

run:
	$(GO) run ./cmd/api

docker-build:
	docker build -f deployments/Dockerfile -t transfer-engine:local .

compose-up:
	$(COMPOSE) up --build -d --wait

# Keeps named volumes, including simulated balances and ledger history.
compose-down:
	$(COMPOSE) down

check: fmt-check lint test race build integration docker-build
