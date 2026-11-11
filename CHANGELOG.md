# Changelog

## 1.0.0 (2026-11-11)

The first release of Relay: event delivery for webhooks, email and push, with retries, a dead-letter queue and
a delivery log.

- **Ingestion** (`POST /v1/events`): API keys with scopes (`ingest`, `read`, `manage`, `admin`), keys bound to one
  tenant or valid for all, an `Idempotency-Key` that returns the original event for 24 hours (`422` when reused
  with another body), strict validation with RFC 9457 errors, and `202 Accepted` with the event ID.
- **Routing:** per-tenant subscriptions match event types exactly, by prefix (`job.*`) or all (`*`) and send
  each event to its endpoints; `targets` address push users and email recipients or narrow the fan-out to some
  endpoints.
- **Webhooks:** Standard Webhooks signatures (HMAC-SHA256 over the ID, a timestamp and the body), a 10-second
  timeout, no redirects, secret rotation with a 24-hour overlap, secrets encrypted at rest, and an SSRF guard that
  refuses private and reserved addresses when an endpoint is saved and again on every connection.
- **Retries:** 8 attempts over up to 23 hours with exponential backoff and jitter, honoring `Retry-After`; then
  the dead-letter queue, replayed one delivery at a time or in bulk. Endpoints that keep failing for a day are
  disabled automatically (at once on `410 Gone`), and their pending deliveries are dead-lettered.
- **Delivery log API:** events with their deliveries, deliveries filtered by event, endpoint or status, and each
  delivery's attempts with the status code, latency, error and the start of the response.
- **Limits:** token buckets per tenant for ingestion and for deliveries, and concurrency slots per endpoint and per
  tenant. A delivery that can't get a slot waits in a parking lot and is woken when one frees up, so a slow
  endpoint never holds other tenants' work.
- **Push** through the Expo Push API, with receipt checks and removal of tokens Expo reports as no longer
  registered; **email** through Resend or SMTP (or a log-only mailer).
- **Management API** for tenants (limits, disabling), API keys (created once, revoked everywhere at once),
  endpoints, subscriptions and push tokens.
- **Reliability:** PostgreSQL is the source of truth and Redis Streams only queue IDs; a sweeper requeues events,
  deliveries and expired attempts that Redis lost; workers scale horizontally and drain their attempts on
  shutdown.
- **Observability:** Prometheus metrics on a separate port, a provisioned Grafana dashboard, OpenTelemetry traces
  and JSON logs.
- **Operations:** one distroless image for the API, the worker, migrations and key creation; a Docker Compose
  stack with an `observability` profile; release gates and an image smoke test in `deploy/release.sh`; a k6 load
  test on two pinned CPUs.
- **API reference** at `/docs` from the OpenAPI 3.1 document at `/openapi.yaml`, which the integration tests
  enforce on every response.
- **Performance:** events and their deliveries are stored in one transaction shared by concurrent requests (group
  commit), and the worker loads and records deliveries in batches. With Relay on two pinned CPUs, k6 at 500
  events a second for two minutes: every event accepted and delivered, signed and once, no backlog at the end,
  ingest latency p50 21–34 ms and p99 187–281 ms ([load test report](docs/LOADTEST.md)).
