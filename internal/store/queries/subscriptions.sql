-- name: CreateSubscription :one
INSERT INTO subscriptions (id, tenant_id, endpoint_id, event_types)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetSubscription :one
SELECT * FROM subscriptions WHERE tenant_id = $1 AND id = $2;

-- name: ListSubscriptions :many
SELECT * FROM subscriptions
WHERE tenant_id = sqlc.arg('tenant_id')
  AND (sqlc.narg('endpoint_id')::uuid IS NULL OR endpoint_id = sqlc.narg('endpoint_id')::uuid)
  AND (sqlc.narg('before')::uuid IS NULL OR id < sqlc.narg('before')::uuid)
ORDER BY id DESC
LIMIT sqlc.arg('lim');

-- name: UpdateSubscription :one
UPDATE subscriptions SET event_types = $3
WHERE tenant_id = $1 AND id = $2
RETURNING *;

-- name: DeleteSubscription :execrows
DELETE FROM subscriptions WHERE tenant_id = $1 AND id = $2;

-- name: ListRoutes :many
-- Everything the router needs to fan an event out, for one tenant (cached by the worker).
SELECT s.event_types, e.id AS endpoint_id, e.channel, e.email_to
FROM subscriptions s JOIN endpoints e ON e.id = s.endpoint_id
WHERE s.tenant_id = $1 AND e.status = 'active';
