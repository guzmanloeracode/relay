#!/usr/bin/env bash
# Relay's release gates (there is no hosted CI; deploy/release.sh runs this before building anything).
#   scripts/verify.sh            format, vet, lint, generated code, tests with -race and the coverage floor
#   scripts/verify.sh lint       golangci-lint only
#   scripts/verify.sh generate   regenerate internal/store/db with sqlc
# Docker is needed: golangci-lint and sqlc run from pinned images, the integration tests use testcontainers.
set -euo pipefail
cd "$(dirname "$0")/.."

LINT_IMAGE=golangci/golangci-lint:v2.15.0-alpine
SQLC_IMAGE=sqlc/sqlc:1.31.1
COVERAGE_FLOOR=80

lint() {
  docker run --rm -v "$PWD:/src" -v relay-golangci-cache:/root/.cache -w /src "$LINT_IMAGE" golangci-lint run ./...
}
sqlc() { docker run --rm -v "$PWD:/src" -w /src -u "$(id -u):$(id -g)" "$SQLC_IMAGE" "$@"; }

case "${1:-all}" in
  lint) lint; exit ;;
  generate) sqlc generate; exit ;;
  all) ;;
  *) echo "usage: $0 [lint|generate]" >&2; exit 2 ;;
esac

step() { printf '\n== %s\n' "$*"; }

step "gofmt"
unformatted="$(gofmt -l cmd internal pkg api)"
[ -z "$unformatted" ] || { echo "not formatted:"; echo "$unformatted"; exit 1; }

step "go vet"
go vet ./...

step "golangci-lint"
lint

step "sqlc: generated code matches the SQL"
sqlc diff

step "go mod tidy"
go mod tidy -diff

step "tests (-race; integration tests on PostgreSQL 18 and Redis 8 in testcontainers)"
profile="$(mktemp)"
trap 'rm -f "$profile"' EXIT
go test -race -count=1 -timeout 15m -coverpkg=./internal/...,./pkg/... -coverprofile="$profile" ./...

step "coverage"
# Measured without the code sqlc generates.
grep -v '/internal/store/db/' "$profile" > "$profile.own"
total="$(go tool cover -func="$profile.own" | awk '/^total:/ {print substr($3, 1, length($3)-1)}')"
rm -f "$profile.own"
echo "statements covered (excluding generated code): ${total}%"
awk -v t="$total" -v f="$COVERAGE_FLOOR" 'BEGIN { exit (t + 0 >= f) ? 0 : 1 }' || { echo "below the ${COVERAGE_FLOOR}% floor"; exit 1; }

printf '\nverify passed\n'
