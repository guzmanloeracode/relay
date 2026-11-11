#!/usr/bin/env bash
# Secret scan of the whole git history and a check of the Go dependencies against the Go vulnerability database.
# Run before publishing a release.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
docker run --rm -v "$ROOT:/repo:ro" zricethezav/gitleaks:v8.30.1 git /repo --no-banner --redact
cd "$ROOT" && go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
