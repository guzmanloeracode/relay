-- name: CreateAPIKey :one
INSERT INTO api_keys (id, tenant_id, name, prefix, hash, scopes)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetAPIKeyByHash :one
-- Used on every authenticated request (behind an in-process cache).
SELECT k.id, k.tenant_id, k.scopes, k.revoked_at, t.slug AS tenant_slug, t.disabled_at AS tenant_disabled_at
FROM api_keys k LEFT JOIN tenants t ON t.id = k.tenant_id
WHERE k.hash = $1;

-- name: ListTenantAPIKeys :many
SELECT * FROM api_keys WHERE tenant_id = $1 ORDER BY id DESC;

-- name: ListGlobalAPIKeys :many
SELECT * FROM api_keys WHERE tenant_id IS NULL ORDER BY id DESC;

-- name: RevokeAPIKey :one
UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now())
WHERE id = sqlc.arg('id') AND tenant_id IS NOT DISTINCT FROM sqlc.narg('tenant_id')::uuid
RETURNING *;

-- name: TouchAPIKey :exec
UPDATE api_keys SET last_used_at = now() WHERE id = $1;
