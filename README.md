# Relay

**Event delivery for webhooks, email and push.** Your application sends an event once; Relay routes it to each
tenant's endpoints, signs and delivers it, retries for up to a day, keeps what fails in a dead-letter queue you
can replay, and logs every attempt.

[**Live instance**](https://relay.tinggalpake.com) · [API reference](https://relay.tinggalpake.com/docs) ·
[OpenAPI 3.1](api/openapi.yaml) · [Architecture](docs/ARCHITECTURE.md) · [Load test](docs/LOADTEST.md) ·
[Runbook](docs/RUNBOOK.md)

Relay is written in Go, runs on PostgreSQL and Redis with Docker Compose, and is generic: any multi-tenant
application can use it. [FieldOps](https://github.com/guzmanloeracode/fieldops) is its first client.

<!-- LOADTEST-SUMMARY -->

## What it does

- **Ingest** with `POST /v1/events`: API keys with scopes, an `Idempotency-Key` that makes retries safe for 24
  hours, strict validation, and a `202` with the event ID in milliseconds.
- **Route** each event by the tenant's subscriptions (`job.completed`, `job.*`, `*`) to its endpoints on three
  channels: **webhooks**, **email** (Resend or any SMTP server) and **push** (Expo, with receipt checks and
  removal of dead tokens).
- **Sign** webhooks with HMAC-SHA256 and a timestamp following [Standard Webhooks](https://www.standardwebhooks.com),
  with a 10-second timeout, no redirects, and secret rotation without downtime.
- **Retry** 8 times over up to 23 hours with exponential backoff and jitter, honoring `Retry-After`; then the
  **dead-letter queue**, replayed one by one or in bulk. An endpoint that fails for a day (or answers `410 Gone`)
  is **disabled automatically**.
- **Isolate tenants**: a token bucket per tenant on ingestion and on deliveries, and concurrency slots per
  endpoint and per tenant, so a slow endpoint holds only its own slots and never starves the others.
- **Show** every delivery and attempt (status code, latency, error, the start of the response) through the
  delivery log API, and the whole system through Prometheus metrics, a Grafana dashboard, OpenTelemetry traces
  and JSON logs.
- **Survive** crashes: PostgreSQL is the source of truth, Redis only queues IDs, and a sweeper requeues anything
  Redis lost. Workers scale horizontally and drain their attempts on shutdown.

## Quick start

You need Docker.

```bash
git clone https://github.com/guzmanloeracode/relay && cd relay
cp .env.example .env && sed -i "s|^RELAY_SECRET_KEY=.*|RELAY_SECRET_KEY=$(openssl rand -base64 32)|" .env
docker compose up -d --build
docker compose run --rm --no-deps migrate keys create --admin --name local
```

The last command prints an admin key. The API is on `http://localhost:8080` and its reference on
[`/docs`](http://localhost:8080/docs). Add `--profile observability` to `docker compose up` for Prometheus,
Grafana (`http://localhost:3300`) and Jaeger.

## Send your first event

```bash
export RELAY=http://localhost:8080 ADMIN=rk_…   # the key printed above

# A tenant (one of your customers) and a key for your application
curl -s $RELAY/v1/tenants -H "Authorization: Bearer $ADMIN" -d '{"slug":"acme","name":"Acme Air"}'
curl -s $RELAY/v1/tenants/acme/keys -H "Authorization: Bearer $ADMIN" -d '{"name":"backend"}'   # → "key": "rk_…"
export KEY=rk_…

# A webhook endpoint subscribed to job events (the response holds its signing secret, "whsec_…")
curl -s $RELAY/v1/tenants/acme/endpoints -H "Authorization: Bearer $KEY" \
  -d '{"channel":"webhook","url":"https://example.com/hooks/relay","event_types":["job.*"]}'

# An event
curl -s $RELAY/v1/events -H "Authorization: Bearer $KEY" -H "Idempotency-Key: job-1042-completed" \
  -d '{"type":"job.completed","payload":{"job":"J-1042","technician":"Tom"}}'
# → 202 {"id":"evt_01jd…","status":"accepted",…}

# What happened to it
curl -s $RELAY/v1/tenants/acme/events/evt_01jd… -H "Authorization: Bearer $KEY"
```

An event can also carry a human-readable `message` (`title`, `body`, `url`) for the email and push channels, and
`targets` to address it: `users` for push, `emails` for email, or `endpoints` to narrow the fan-out.

```json
{
  "type": "job.assigned",
  "payload": { "job": "J-1043" },
  "message": { "title": "New job J-1043", "body": "12 Oak St, 10:00", "url": "fieldops://jobs/J-1043" },
  "targets": { "users": ["usr_tom"] }
}
```

## Receiving webhooks

Each webhook is a `POST` with the event as JSON and three headers:

```http
webhook-id: evt_01jd8x4m2k7v9q3w5e6r7t8y9z
webhook-timestamp: 1794378000
webhook-signature: v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4=

{"id":"evt_01jd8x4m2k7v9q3w5e6r7t8y9z","type":"job.completed","tenant":"acme",
 "created_at":"2026-11-11T09:30:00Z","data":{"job":"J-1042","technician":"Tom"}}
```

Verify the signature, reject timestamps more than five minutes off, answer `2xx` within 10 seconds, and
deduplicate on `webhook-id` (delivery is at least once). In Go:

```go
import "github.com/guzmanloeracode/relay/pkg/webhook"

func handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	if err := webhook.Verify(secret, r.Header, body, webhook.DefaultTolerance); err != nil {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	// … process once per webhook-id …
	w.WriteHeader(http.StatusNoContent)
}
```

Other languages can use the official [Standard Webhooks libraries](https://github.com/standard-webhooks/standard-webhooks).
`410 Gone` tells Relay to stop: the endpoint is disabled at once.

## Concepts

| | |
|---|---|
| **Tenant** | One of your customers, named by a slug you choose (for FieldOps, an organization ID). Has its own limits. |
| **API key** | `rk_…`, bound to one tenant or valid for all. Scopes: `ingest`, `read`, `manage`, `admin` (tenants and keys). |
| **Endpoint** | A destination on one channel: a webhook URL (with its secret), an email route (default recipients) or a push route. |
| **Subscription** | Event-type patterns (`job.completed`, `job.*`, `*`) that send events to an endpoint. |
| **Delivery** | One event to one endpoint (and one recipient for email and push): `pending` until an attempt settles it: `delivered`, `skipped` or `dead`. |
| **Attempt** | One try of a delivery, logged with its status code, latency, error and response. |

| Attempt | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
|---|---|---|---|---|---|---|---|---|
| Wait before it (at most) | – | 75 s | 3.8 min | 11 min | 34 min | 1.7 h | 5.1 h | 15.2 h |

Each wait is shortened by up to a quarter at random so retries spread out. After the eighth failure the delivery
is dead-lettered.

## Observability

- **Metrics** on a separate port (`:9091/metrics`): ingestion rate and latency, attempts by outcome, delivery
  lag, attempt duration by channel, backlog by queue, deferrals by limit, dead letters by reason, sweeper
  recoveries, disabled endpoints.
- **Dashboard** in `deploy/observability/grafana`, provisioned by the `observability` profile.
- **Traces** through OpenTelemetry (OTLP/HTTP) when `OTEL_EXPORTER_OTLP_ENDPOINT` is set: the API, routing, each
  attempt and Redis calls.
- **Logs** as JSON (`slog`) with request IDs.

## Tests and release gates

```bash
scripts/verify.sh     # what every release runs first
loadtest/run.sh       # the k6 load test on 2 pinned CPUs
```

- Unit tests for the pure parts: routing, backoff, IDs, signatures (against the Standard Webhooks test vector),
  the SSRF guard, the email, push and webhook adapters (against fake Resend, SMTP and Expo servers).
- Integration tests on PostgreSQL 18 and Redis 8 in testcontainers: the full path from `POST /v1/events` to a
  signed webhook, idempotency, validation and tenant isolation, retries into the dead-letter queue and replay,
  automatic disabling, rate and concurrency limits (a slow endpoint doesn't starve another tenant), recovery
  after Redis loses its data or a worker dies mid-attempt, graceful shutdown, three workers without duplicates,
  email and push.
- **Contract tests:** every request and response in the integration tests is validated against
  `api/openapi.yaml`.
- Everything runs with `-race`; the release fails under 80% statement coverage.

There is no hosted CI: `deploy/release.sh` runs these gates, builds the image, smoke-tests it in a throwaway
stack, deploys it and rolls back automatically if the health checks fail ([Runbook](docs/RUNBOOK.md)).

## Configuration

Every setting is an environment variable, documented in [`.env.example`](.env.example). The required ones are
`POSTGRES_PASSWORD`, `REDIS_PASSWORD` and `RELAY_SECRET_KEY` (32 random bytes that encrypt webhook secrets at
rest).

## Design decisions

- [ADR 001](docs/adr/001-redis-streams.md): Redis Streams for the queues, PostgreSQL as the source of truth (and
  why not `SKIP LOCKED` alone)
- [ADR 002](docs/adr/002-standard-webhooks.md): Standard Webhooks signatures
- [ADR 003](docs/adr/003-tenants-and-keys.md): tenants named by the client, keys bound to one tenant or to all

## Roadmap

- FieldOps sends its events through Relay (FieldOps 0.5, through its transactional outbox).
- A realtime channel over WebSocket with short-lived tokens.
- Per-event-type JSON Schemas that tenants can register and receivers can browse.

## License

[MIT](LICENSE). Built by Ahmad Izzat Maulana.
