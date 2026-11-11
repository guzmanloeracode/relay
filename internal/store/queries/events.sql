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

-- name: ReleaseIdempotencyKey :execrows
-- Frees an expired key so a new event can use it.
UPDATE events SET idempotency_key = NULL
WHERE tenant_id = sqlc.arg('tenant_id') AND id = sqlc.arg('id') AND idempotency_key = sqlc.arg('key')::text;

-- name: DeleteEventsBefore :execrows
-- Deliveries and attempts go with their event (ON DELETE CASCADE). before is a UUIDv7 made from the cutoff
-- time, so this walks the primary key.
DELETE FROM events e WHERE e.id IN (
    SELECT o.id FROM events o WHERE o.id < sqlc.arg('before')::uuid ORDER BY o.id LIMIT sqlc.arg('lim')::int
);

-- name: InsertEvents :many
-- Many requests' events in one statement (the API's group commit; their deliveries follow in the same
-- transaction). Rows whose Idempotency-Key was used before are skipped; the caller handles those one by one.
-- Empty strings stand for NULL in the optional columns.
INSERT INTO events (id, tenant_id, type, payload, message, targets, idempotency_key, request_hash, delivery_count)
SELECT u.id, u.tenant_id, u.type, u.payload::jsonb, NULLIF(u.message, '')::jsonb, NULLIF(u.targets, '')::jsonb,
       NULLIF(u.idem_key, ''), u.request_hash, u.delivery_count
FROM (
    SELECT unnest(sqlc.arg('ids')::uuid[]) AS id, unnest(sqlc.arg('tenant_ids')::uuid[]) AS tenant_id,
           unnest(sqlc.arg('types')::text[]) AS type, unnest(sqlc.arg('payloads')::text[]) AS payload,
           unnest(sqlc.arg('messages')::text[]) AS message, unnest(sqlc.arg('targets')::text[]) AS targets,
           unnest(sqlc.arg('idem_keys')::text[]) AS idem_key, unnest(sqlc.arg('request_hashes')::bytea[]) AS request_hash,
           unnest(sqlc.arg('delivery_counts')::int[]) AS delivery_count
) AS u
ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id, created_at;
