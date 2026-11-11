# ADR 002: Sign webhooks with the Standard Webhooks scheme

**Status:** accepted (Relay 1.0, November 2026)

## Context

Receivers must be able to check that a webhook came from Relay, that its body wasn't changed, and that an old
request isn't being replayed. The requirement (REL-03) is an HMAC-SHA256 signature with a timestamp.

A custom header (`X-Relay-Signature: t=…,v1=…`, as Stripe does) would work, but every receiver would need
Relay-specific code.

## Decision

Follow the [Standard Webhooks](https://www.standardwebhooks.com) specification:

- `webhook-id`: the event ID (`evt_…`), the same on every retry and for every endpoint, so receivers
  deduplicate on it.
- `webhook-timestamp`: Unix seconds when the attempt was signed. Receivers reject timestamps more than five
  minutes from their clock, which blocks replays.
- `webhook-signature`: `v1,<base64 HMAC-SHA256>` over `<id>.<timestamp>.<body>` with the endpoint's secret
  (`whsec_<base64>`). The header can hold several signatures separated by spaces.
- A secret **rotation** keeps the old secret signing too for 24 hours (`SECRET_ROTATION_GRACE`), so the header
  carries two signatures and receivers can switch over without dropping a request.
- Secrets are stored encrypted (AES-256-GCM with `RELAY_SECRET_KEY`), because Relay needs them in clear to sign.

`pkg/webhook` implements signing and verification and is tested against the specification's published test
vector, so receivers written in Go can import it; the official Standard Webhooks libraries work in other
languages.

## Consequences

- Receivers can use existing libraries in many languages instead of Relay-specific code.
- Relay can't store only a hash of a secret; a database leak without `RELAY_SECRET_KEY` doesn't reveal secrets.
- Extra Relay headers (`Relay-Delivery-Id`, `Relay-Attempt`) are informational and not signed.
