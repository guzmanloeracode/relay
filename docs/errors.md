# Error codes

Every error is an RFC 9457 problem (`application/problem+json`) whose `type` links to a section below and whose
`code` is one of these stable values.

```json
{
  "type": "https://github.com/guzmanloeracode/relay/blob/main/docs/errors.md#validation_failed",
  "title": "Validation failed",
  "status": 422,
  "code": "validation_failed",
  "detail": "One or more fields are invalid.",
  "errors": [{ "path": "type", "message": "must be lower-case words separated by dots, such as job.completed" }]
}
```

| Code | Status | Meaning |
|---|---|---|
| <a id="invalid_json"></a>`invalid_json` | 400 | The body isn't valid JSON, or has data after the object. |
| <a id="invalid_body"></a>`invalid_body` | 400 | The body couldn't be read. |
| <a id="unauthenticated"></a>`unauthenticated` | 401 | No `Authorization: Bearer` key, or the key is unknown or revoked. |
| <a id="forbidden"></a>`forbidden` | 403 | The key lacks the scope the route needs. |
| <a id="tenant_disabled"></a>`tenant_disabled` | 403 | The tenant (of the key, or named in the event) is disabled. |
| <a id="not_found"></a>`not_found` | 404 | No such resource for this key. Other tenants' resources are reported as not found too. |
| <a id="method_not_allowed"></a>`method_not_allowed` | 405 | The route exists but not with this method. |
| <a id="not_dead"></a>`not_dead` | 409 | Only deliveries in the dead-letter queue can be replayed. |
| <a id="already_replayed"></a>`already_replayed` | 409 | The dead delivery was replayed before; look for the delivery whose `replay_of` points to it. |
| <a id="endpoint_disabled"></a>`endpoint_disabled` | 409 | The delivery's endpoint is disabled. Enable it, then replay. |
| <a id="not_a_webhook"></a>`not_a_webhook` | 409 | Only webhook endpoints have a signing secret. |
| <a id="slug_taken"></a>`slug_taken` | 409 | Another tenant already has this slug. |
| <a id="body_too_large"></a>`body_too_large` | 413 | The body is over 512 KB (an event's payload is limited to 256 KB). |
| <a id="validation_failed"></a>`validation_failed` | 422 | One or more fields are invalid; `errors[]` lists each one with its path. |
| <a id="idempotency_key_reused"></a>`idempotency_key_reused` | 422 | This `Idempotency-Key` was used with a different body in the last 24 hours. |
| <a id="rate_limited"></a>`rate_limited` | 429 | Too many requests. Wait `Retry-After` seconds. Ingestion has a per-tenant limit; other routes 50 per second per key. |
| <a id="internal"></a>`internal` | 500 | Relay failed. The event, if any, was not accepted; retrying with the same `Idempotency-Key` is safe. |
