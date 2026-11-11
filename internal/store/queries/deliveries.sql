-- name: InsertDeliveries :many
-- Routing is idempotent: an event routed twice (after a crash) keeps one delivery per endpoint and recipient.
INSERT INTO deliveries (id, tenant_id, event_id, endpoint_id, channel, recipient, max_attempts)
SELECT unnest(sqlc.arg('ids')::uuid[]), unnest(sqlc.arg('tenant_ids')::uuid[]), unnest(sqlc.arg('event_ids')::uuid[]),
       unnest(sqlc.arg('endpoint_ids')::uuid[]), unnest(sqlc.arg('channels')::text[]), unnest(sqlc.arg('recipients')::text[]),
       sqlc.arg('max_attempts')::int
ON CONFLICT (event_id, endpoint_id, recipient, replay_of) DO NOTHING
RETURNING id, tenant_id, endpoint_id;

-- name: LoadDelivery :one
-- Reads a due delivery for an attempt. A message for a delivery that is done or not due yet (a stale
-- duplicate) matches nothing, which makes queue redelivery harmless. The worker holds the delivery's slot in
-- Redis while it attempts; the result is written once, by RecordAttempt.
SELECT d.id, d.tenant_id, d.event_id, d.endpoint_id, d.channel, d.recipient, d.attempts, d.max_attempts,
       ev.type AS event_type, ev.payload, ev.message, ev.created_at AS event_created_at,
       en.url, en.secret_enc, en.previous_secret_enc, en.previous_secret_expires_at, en.email_to,
       t.slug AS tenant_slug
FROM deliveries d
JOIN events ev ON ev.id = d.event_id
JOIN endpoints en ON en.id = d.endpoint_id
JOIN tenants t ON t.id = d.tenant_id
WHERE d.id = sqlc.arg('id')
  AND d.status = 'pending'
  AND d.next_attempt_at <= now() + interval '2 seconds'
  AND en.status = 'active';

-- name: RecordAttempt :execrows
-- Records one attempt and the delivery's next state in one statement. It only applies if nobody recorded an
-- attempt since the delivery was loaded (attempts unchanged), so a duplicate attempt never counts twice.
-- outcome 'retry' keeps the delivery pending until next_attempt_at.
WITH upd AS (
    UPDATE deliveries d SET
        status           = CASE sqlc.arg('outcome')::text WHEN 'retry' THEN 'pending' ELSE sqlc.arg('outcome')::text END,
        attempts         = d.attempts + 1,
        next_attempt_at  = sqlc.arg('next_attempt_at')::timestamptz,
        last_status_code = sqlc.narg('status_code')::int,
        last_error       = sqlc.narg('error')::text,
        last_latency_ms  = sqlc.arg('latency_ms')::int,
        dead_reason      = sqlc.narg('dead_reason')::text,
        completed_at     = CASE sqlc.arg('outcome')::text WHEN 'retry' THEN NULL ELSE now() END
    WHERE d.id = sqlc.arg('id') AND d.status = 'pending' AND d.attempts = sqlc.arg('seen_attempts')::int
    RETURNING d.id, d.attempts
)
INSERT INTO delivery_attempts (delivery_id, attempt, started_at, latency_ms, status_code, error, response_body, outcome)
SELECT upd.id, upd.attempts, sqlc.arg('started_at')::timestamptz, sqlc.arg('latency_ms')::int,
       sqlc.narg('status_code')::int, sqlc.narg('error')::text, sqlc.narg('response_body')::text, sqlc.arg('outcome')::text
FROM upd;

-- name: DeadLetterIfEndpointDisabled :execrows
UPDATE deliveries d SET status = 'dead', dead_reason = 'endpoint_disabled', completed_at = now()
WHERE d.id = $1 AND d.status = 'pending'
  AND EXISTS (SELECT 1 FROM endpoints e WHERE e.id = d.endpoint_id AND e.status = 'disabled');

-- name: DeadLetterEndpointBacklog :execrows
UPDATE deliveries SET status = 'dead', dead_reason = sqlc.arg('reason'), completed_at = now()
WHERE endpoint_id = sqlc.arg('endpoint_id') AND status = 'pending';

-- name: GetDelivery :one
SELECT * FROM deliveries WHERE tenant_id = $1 AND id = $2;

-- name: ListDeliveries :many
SELECT * FROM deliveries
WHERE tenant_id = sqlc.arg('tenant_id')
  AND (sqlc.narg('event_id')::uuid IS NULL OR event_id = sqlc.narg('event_id')::uuid)
  AND (sqlc.narg('endpoint_id')::uuid IS NULL OR endpoint_id = sqlc.narg('endpoint_id')::uuid)
  AND (sqlc.narg('status')::text IS NULL OR status = sqlc.narg('status')::text)
  AND (sqlc.narg('before')::uuid IS NULL OR id < sqlc.narg('before')::uuid)
ORDER BY id DESC
LIMIT sqlc.arg('lim');

-- name: ListEventDeliveries :many
SELECT * FROM deliveries WHERE tenant_id = $1 AND event_id = $2 ORDER BY id;

-- name: ListAttempts :many
SELECT * FROM delivery_attempts WHERE delivery_id = $1 ORDER BY attempt, id;

-- name: ListDeadLetters :many
SELECT * FROM deliveries
WHERE tenant_id = sqlc.arg('tenant_id') AND status = 'dead' AND replayed_at IS NULL
  AND (sqlc.narg('endpoint_id')::uuid IS NULL OR endpoint_id = sqlc.narg('endpoint_id')::uuid)
  AND (sqlc.narg('before')::uuid IS NULL OR id < sqlc.narg('before')::uuid)
ORDER BY id DESC
LIMIT sqlc.arg('lim');

-- name: ReplayDelivery :one
-- A replay is a new delivery with a fresh attempt budget; the dead one stays in the log, marked replayed.
WITH orig AS (
    UPDATE deliveries d SET replayed_at = now()
    WHERE d.tenant_id = sqlc.arg('tenant_id') AND d.id = sqlc.arg('id')
      AND d.status = 'dead' AND d.replayed_at IS NULL
      AND EXISTS (SELECT 1 FROM endpoints e WHERE e.id = d.endpoint_id AND e.status = 'active')
    RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.channel, d.recipient
)
INSERT INTO deliveries (id, tenant_id, event_id, endpoint_id, channel, recipient, max_attempts, replay_of)
SELECT sqlc.arg('new_id')::uuid, orig.tenant_id, orig.event_id, orig.endpoint_id, orig.channel, orig.recipient,
       sqlc.arg('max_attempts')::int, orig.id
FROM orig
RETURNING *;

-- name: ReplayDeadLetters :many
WITH picked AS (
    SELECT d.id FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id
    WHERE d.tenant_id = sqlc.arg('tenant_id') AND d.status = 'dead' AND d.replayed_at IS NULL AND e.status = 'active'
      AND (sqlc.narg('endpoint_id')::uuid IS NULL OR d.endpoint_id = sqlc.narg('endpoint_id')::uuid)
    ORDER BY d.id
    LIMIT sqlc.arg('lim')
    FOR UPDATE OF d SKIP LOCKED
), orig AS (
    UPDATE deliveries d SET replayed_at = now() FROM picked WHERE d.id = picked.id
    RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.channel, d.recipient
)
INSERT INTO deliveries (id, tenant_id, event_id, endpoint_id, channel, recipient, max_attempts, replay_of)
SELECT uuidv7(), orig.tenant_id, orig.event_id, orig.endpoint_id, orig.channel, orig.recipient,
       sqlc.arg('max_attempts')::int, orig.id
FROM orig
RETURNING id, tenant_id, endpoint_id;

-- name: RequeueOverdue :many
-- Safety net: due deliveries whose queue message was lost. Bumping next_attempt_at keeps them from being
-- picked again on the next sweep while they wait in the queue.
UPDATE deliveries d SET next_attempt_at = now()
WHERE d.id IN (
    SELECT id FROM deliveries
    WHERE status = 'pending' AND next_attempt_at < now() - make_interval(secs => sqlc.arg('older_than_seconds')::int)
    ORDER BY next_attempt_at
    LIMIT sqlc.arg('lim')
    FOR UPDATE SKIP LOCKED
)
RETURNING d.id, d.tenant_id, d.endpoint_id;

-- name: CountDeliveriesByStatus :many
SELECT status, count(*)::bigint AS n FROM deliveries GROUP BY status;
