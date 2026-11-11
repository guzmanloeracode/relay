# Runbook

## Deploy

Releases run on the server with `deploy/release.sh` (there is no hosted CI):

1. `scripts/verify.sh`: gofmt, `go vet`, golangci-lint, `sqlc diff` (generated code matches the SQL),
   `go mod tidy -diff`, every test with `-race` (integration tests on PostgreSQL 18 and Redis 8 in
   testcontainers, each API response checked against the OpenAPI document), and the coverage floor.
2. Build `relay:<commit>` from `git archive HEAD`.
3. `deploy/e2e.sh`: a throwaway Compose project runs the new image with its own PostgreSQL, Redis and sink;
   50 events must arrive signed, once each.
4. Tag the running image `relay:rollback`, `docker compose up` the new one (migrations run first).
5. Wait for `/readyz` to report the new commit and the worker to be healthy, check the public URL, and roll back
   automatically if any check fails.

The log is in `/var/log/relay-release.log`. Settings live in `/etc/relay/relay.env` (root only, never in git).

```bash
flock /tmp/fieldops-heavy.lock deploy/release.sh
```

## First admin key

```bash
docker compose -p relay --env-file /etc/relay/relay.env run --rm --no-deps migrate keys create --admin --name ops
```

The key is printed once. Use it to create tenants and service keys through the API; store it in a password
manager.

## Roll back by hand

```bash
TAG=rollback docker compose -p relay --env-file /etc/relay/relay.env up -d --no-build api worker
```

Migrations are forward-only and additive, so the previous image runs against a newer schema.

## Scale workers

```bash
docker compose -p relay --env-file /etc/relay/relay.env up -d --no-build --scale worker=3
```

Workers share the streams through a consumer group and the limits through Redis. Each process holds up to
`WORKER_CONCURRENCY` attempts.

## Backup and restore

PostgreSQL holds everything that matters; Redis can be rebuilt (the sweeper requeues due work).

```bash
docker exec relay-postgres-1 pg_dump -U relay -Fc relay > relay-$(date +%F).dump
docker exec -i relay-postgres-1 pg_restore -U relay -d relay --clean --if-exists < relay-YYYY-MM-DD.dump
```

`RELAY_SECRET_KEY` must be kept with the backup: without it, webhook signing secrets can't be decrypted, and
every webhook endpoint would need `rotate-secret` (and its receiver the new secret).

## When something looks wrong

| Symptom | Look at | Likely cause |
|---|---|---|
| `relay_queue_depth{queue="deliveries"}` keeps growing | worker logs, `relay_deliveries_in_flight` | workers down or saturated: scale workers |
| `relay_queue_depth{queue="parked"}` high | `/v1/tenants/{t}/deliveries?status=pending` | one tenant's endpoint is slow: its own backlog, others are not affected |
| Many `max_attempts` dead letters | the endpoint's `failure_streak`, its deliveries' `last_error` | the receiver is down or rejecting; it is disabled after 24 h of failures |
| Ingestion answers 429 | the tenant's `limits` | raise `ingest_rate` / `ingest_burst` with `PATCH /v1/tenants/{t}` |
| `relay_requeued_total` rising | worker restarts, Redis restarts | expected after a crash; constant growth means Redis is losing data |
| Webhook `last_error` mentions a private address | the endpoint URL | the host resolves to a private IP; Relay refuses to call it |

Redis was flushed or lost: nothing to do; once deliveries are `SWEEP_OVERDUE` (10 min) late the sweeper
requeues them from PostgreSQL.
