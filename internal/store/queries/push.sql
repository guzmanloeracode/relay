-- name: UpsertPushToken :one
INSERT INTO push_tokens (id, tenant_id, user_id, token, platform)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, token) DO UPDATE SET user_id = EXCLUDED.user_id, platform = EXCLUDED.platform
RETURNING *;

-- name: ListPushTokens :many
SELECT * FROM push_tokens
WHERE tenant_id = sqlc.arg('tenant_id')
  AND (sqlc.narg('user_id')::text IS NULL OR user_id = sqlc.narg('user_id')::text)
  AND (sqlc.narg('before')::uuid IS NULL OR id < sqlc.narg('before')::uuid)
ORDER BY id DESC
LIMIT sqlc.arg('lim');

-- name: GetUserPushTokens :many
SELECT id, token FROM push_tokens WHERE tenant_id = $1 AND user_id = $2 ORDER BY id;

-- name: DeletePushToken :execrows
DELETE FROM push_tokens WHERE tenant_id = $1 AND id = $2;

-- name: DeletePushTokensByValue :execrows
DELETE FROM push_tokens WHERE tenant_id = sqlc.arg('tenant_id') AND token = ANY(sqlc.arg('tokens')::text[]);

-- name: TouchPushTokens :exec
UPDATE push_tokens SET last_used_at = now() WHERE id = ANY(sqlc.arg('ids')::uuid[]);

-- name: InsertPushTickets :exec
INSERT INTO push_tickets (ticket_id, tenant_id, delivery_id, token, check_after)
SELECT unnest(sqlc.arg('ticket_ids')::text[]), sqlc.arg('tenant_id')::uuid, sqlc.arg('delivery_id')::uuid,
       unnest(sqlc.arg('tokens')::text[]), sqlc.arg('check_after')::timestamptz
ON CONFLICT (ticket_id) DO NOTHING;

-- name: DuePushTickets :many
SELECT ticket_id, tenant_id, token, created_at FROM push_tickets
WHERE checked_at IS NULL AND check_after <= now()
ORDER BY check_after
LIMIT sqlc.arg('lim');

-- name: MarkPushTickets :exec
UPDATE push_tickets p SET checked_at = now(), receipt_status = r.status, receipt_error = NULLIF(r.err, '')
FROM (SELECT unnest(sqlc.arg('ticket_ids')::text[]) AS ticket_id, unnest(sqlc.arg('statuses')::text[]) AS status,
             unnest(sqlc.arg('errors')::text[]) AS err) AS r
WHERE p.ticket_id = r.ticket_id;

-- name: DeletePushTicketsBefore :execrows
DELETE FROM push_tickets WHERE created_at < $1;
