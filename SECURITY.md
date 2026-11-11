# Security policy

Relay sends data to URLs its tenants choose, so it is built to be safe to run as a shared, public service.

## Reporting a vulnerability

Please don't open a public issue. Use GitHub's **Report a vulnerability** (private security advisory) on this
repository. Expect a first reply within 5 working days.

## What is in place

- **Keys:** 238-bit random API keys stored as SHA-256 hashes, scopes per key, keys bound to one tenant see only
  that tenant (others answer `404`), and a revocation reaches every process at once.
- **Webhook secrets:** encrypted at rest with AES-256-GCM; Standard Webhooks signatures with a timestamp, so
  receivers can reject tampered and replayed requests; rotation with a 24-hour overlap.
- **SSRF:** webhook URLs must be public. Private, loopback, link-local, CGNAT and reserved addresses are refused
  when an endpoint is saved and again by the dialer on every connection (after DNS resolution), redirects are
  not followed, and environment proxies are ignored.
- **Input:** strict JSON decoding (unknown fields are errors), size limits on bodies and payloads, validation of
  every field, parameterized queries only (sqlc).
- **Abuse:** a token bucket per tenant on ingestion, 50 requests a second per key elsewhere, and per-endpoint and
  per-tenant concurrency limits on deliveries.
- **Runtime:** distroless, non-root containers with a read-only filesystem, no capabilities and
  `no-new-privileges`; PostgreSQL and Redis are reachable only on the internal Docker network; `/metrics` is on a
  separate port that the reverse proxy doesn't expose.
- **Supply chain:** `deploy/security.sh` runs gitleaks over the full git history and govulncheck over the Go
  dependencies before releases.
