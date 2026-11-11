-- name: CreateTenant :one
INSERT INTO tenants (id, slug, name, ingest_rate, ingest_burst, delivery_rate, max_in_flight)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: GetTenantBySlug :one
SELECT * FROM tenants WHERE slug = $1;

-- name: GetTenant :one
SELECT * FROM tenants WHERE id = $1;

-- name: ListTenants :many
SELECT * FROM tenants
WHERE sqlc.narg('before')::uuid IS NULL OR id < sqlc.narg('before')::uuid
ORDER BY id DESC
LIMIT sqlc.arg('lim');

-- name: UpdateTenant :one
UPDATE tenants SET
    name          = COALESCE(sqlc.narg('name'), name),
    ingest_rate   = COALESCE(sqlc.narg('ingest_rate'), ingest_rate),
    ingest_burst  = COALESCE(sqlc.narg('ingest_burst'), ingest_burst),
    delivery_rate = COALESCE(sqlc.narg('delivery_rate'), delivery_rate),
    max_in_flight = COALESCE(sqlc.narg('max_in_flight'), max_in_flight),
    disabled_at   = CASE
        WHEN sqlc.narg('disabled')::boolean IS NULL THEN disabled_at
        WHEN sqlc.narg('disabled')::boolean THEN COALESCE(disabled_at, now())
        ELSE NULL END,
    updated_at    = now()
WHERE id = sqlc.arg('id')
RETURNING *;

-- name: GetDeliveryLimits :one
-- What the worker needs to apply the tenant's and the endpoint's limits.
SELECT t.delivery_rate, t.max_in_flight AS tenant_max_in_flight, e.max_in_flight AS endpoint_max_in_flight
FROM endpoints e JOIN tenants t ON t.id = e.tenant_id
WHERE e.id = sqlc.arg('endpoint_id') AND t.id = sqlc.arg('tenant_id');
