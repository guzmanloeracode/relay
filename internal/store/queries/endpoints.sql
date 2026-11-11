-- name: CreateEndpoint :one
INSERT INTO endpoints (id, tenant_id, channel, description, url, secret_enc, email_to, max_in_flight)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING *;

-- name: GetEndpoint :one
SELECT * FROM endpoints WHERE tenant_id = $1 AND id = $2;

-- name: ListEndpoints :many
SELECT * FROM endpoints
WHERE tenant_id = sqlc.arg('tenant_id')
  AND (sqlc.narg('before')::uuid IS NULL OR id < sqlc.narg('before')::uuid)
ORDER BY id DESC
LIMIT sqlc.arg('lim');

-- name: UpdateEndpoint :one
UPDATE endpoints SET
    description   = COALESCE(sqlc.narg('description'), description),
    url           = CASE WHEN channel = 'webhook' THEN COALESCE(sqlc.narg('url'), url) ELSE url END,
    email_to      = COALESCE(sqlc.narg('email_to')::text[], email_to),
    max_in_flight = COALESCE(sqlc.narg('max_in_flight'), max_in_flight),
    updated_at    = now()
WHERE tenant_id = sqlc.arg('tenant_id') AND id = sqlc.arg('id')
RETURNING *;

-- name: DeleteEndpoint :execrows
DELETE FROM endpoints WHERE tenant_id = $1 AND id = $2;

-- name: RotateEndpointSecret :one
UPDATE endpoints SET
    previous_secret_enc        = secret_enc,
    previous_secret_expires_at = sqlc.arg('previous_expires_at'),
    secret_enc                 = sqlc.arg('secret_enc'),
    updated_at                 = now()
WHERE tenant_id = sqlc.arg('tenant_id') AND id = sqlc.arg('id') AND channel = 'webhook'
RETURNING *;

-- name: EnableEndpoint :one
UPDATE endpoints SET status = 'active', disabled_reason = NULL, disabled_at = NULL,
    failure_streak = 0, failing_since = NULL, updated_at = now()
WHERE tenant_id = $1 AND id = $2
RETURNING *;

-- name: DisableEndpoint :one
-- Returns no row when the endpoint was already disabled, so the caller only reports a change once.
UPDATE endpoints SET status = 'disabled', disabled_reason = sqlc.arg('reason'), disabled_at = now(), updated_at = now()
WHERE id = sqlc.arg('id') AND status = 'active'
RETURNING *;

-- name: RecordEndpointSuccess :exec
UPDATE endpoints SET failure_streak = 0, failing_since = NULL
WHERE id = $1 AND failure_streak > 0;

-- name: RecordEndpointFailure :one
UPDATE endpoints SET failure_streak = failure_streak + 1, failing_since = COALESCE(failing_since, now())
WHERE id = $1
RETURNING failure_streak, failing_since, status;
