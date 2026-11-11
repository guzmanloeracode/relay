# Everyday commands. The release gates live in scripts/verify.sh and deploy/release.sh (there is no hosted CI).
.PHONY: build unit test lint generate verify up down loadtest

build:
	go build -o bin/relay ./cmd/relay

# Fast tests, no Docker.
unit:
	go test -short ./...

# Unit and integration tests (Docker is needed for testcontainers).
test:
	go test -race ./...

lint:
	scripts/verify.sh lint

# Regenerate internal/store/db from the SQL in internal/store.
generate:
	scripts/verify.sh generate

# Every release gate.
verify:
	scripts/verify.sh

up:
	docker compose up -d --build

down:
	docker compose down

loadtest:
	loadtest/run.sh
