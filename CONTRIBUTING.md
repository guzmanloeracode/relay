# Contributing

Thanks for your interest in Relay.

## Set up

You need Go 1.27 and Docker.

```bash
git clone https://github.com/guzmanloeracode/relay && cd relay
cp .env.example .env && sed -i "s|^RELAY_SECRET_KEY=.*|RELAY_SECRET_KEY=$(openssl rand -base64 32)|" .env
docker compose up -d --build
```

## Before opening a pull request

```bash
scripts/verify.sh
```

It runs the same gates as a release: gofmt, `go vet`, golangci-lint, a check that `internal/store/db` matches the
SQL (`make generate` regenerates it), and every test with `-race`. Integration tests start PostgreSQL 18 and
Redis 8 with testcontainers and check every API response against `api/openapi.yaml`, so a change to the API
changes the spec in the same pull request.

- Database changes are new files in `internal/store/migrations` (forward-only, additive) and queries in
  `internal/store/queries`; run `make generate`.
- Keep commits small, with a one-line conventional subject (`feat:`, `fix:`, `docs:`, `test:`).
- Record significant decisions as an ADR in `docs/adr/`.
