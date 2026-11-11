-- name: InsertEvent :one
-- Returns no row when the tenant already used this Idempotency-Key (the caller then loads that event).
INSERT INTO events (id, tenant_id, type, payload, message, targets, idempotency_key, request_hash)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id, created_at;

-- name: GetEventByIdempotencyKey :one
SELECT id, request_hash, created_at FROM events WHERE tenant_id = $1 AND idempotency_key = $2;

-- name: GetEvent :one
SELECT * FROM events WHERE tenant_id = $1 AND id = $2;

-- name: ListEvents :many
SELECT * FROM events
WHERE tenant_id = sqlc.arg('tenant_id')
  AND (sqlc.narg('type')::text IS NULL OR type = sqlc.narg('type')::text)
  AND (sqlc.narg('before')::uuid IS NULL OR id < sqlc.narg('before')::uuid)
ORDER BY id DESC
LIMIT sqlc.arg('lim');

-- name: GetEventsToRoute :many
SELECT * FROM events WHERE id = ANY(sqlc.arg('ids')::uuid[]) AND routed_at IS NULL;

-- name: MarkEventsRouted :exec
UPDATE events e SET routed_at = now(), delivery_count = r.n
FROM (SELECT unnest(sqlc.arg('ids')::uuid[]) AS id, unnest(sqlc.arg('counts')::int[]) AS n) AS r
WHERE e.id = r.id;

-- name: ListUnroutedEvents :many
-- Events whose routing message was lost (a crash between commit and enqueue, or Redis data loss).
SELECT id, tenant_id FROM events
WHERE routed_at IS NULL AND created_at < now() - make_interval(secs => sqlc.arg('older_than_seconds')::int)
ORDER BY created_at
LIMIT sqlc.arg('lim');

-- name: ReleaseIdempotencyKeys :execrows
UPDATE events e SET idempotency_key = NULL
WHERE e.id IN (
    SELECT o.id FROM events o
    WHERE o.created_at < sqlc.arg('before')::timestamptz AND o.idempotency_key IS NOT NULL
    LIMIT sqlc.arg('lim')::int
);

-- name: DeleteEventsBefore :execrows
-- Deliveries and attempts go with their event (ON DELETE CASCADE).
DELETE FROM events e WHERE e.id IN (
    SELECT o.id FROM events o WHERE o.created_at < sqlc.arg('before')::timestamptz LIMIT sqlc.arg('lim')::int
);
