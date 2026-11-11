-- +goose Up

-- A tenant is one customer of the client application (for FieldOps: one organization). Every other row belongs
-- to exactly one tenant, and every query filters on tenant_id.
CREATE TABLE tenants (
    id            uuid PRIMARY KEY,
    slug          text        NOT NULL UNIQUE,
    name          text        NOT NULL,
    -- Token bucket for POST /v1/events (events per second, and the burst on top).
    ingest_rate   integer     NOT NULL DEFAULT 100 CHECK (ingest_rate > 0),
    ingest_burst  integer     NOT NULL DEFAULT 200 CHECK (ingest_burst > 0),
    -- Token bucket and concurrency cap for outgoing deliveries, across all of the tenant's endpoints.
    delivery_rate integer     NOT NULL DEFAULT 50 CHECK (delivery_rate > 0),
    max_in_flight integer     NOT NULL DEFAULT 16 CHECK (max_in_flight > 0),
    disabled_at   timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- API keys are stored as SHA-256 hashes: they are 192-bit random strings, so a slow hash adds nothing.
-- tenant_id NULL means the key may act for every tenant (a client application's service key, or an admin key).
CREATE TABLE api_keys (
    id           uuid PRIMARY KEY,
    tenant_id    uuid REFERENCES tenants (id) ON DELETE CASCADE,
    name         text        NOT NULL,
    prefix       text        NOT NULL,
    hash         bytea       NOT NULL UNIQUE,
    scopes       text[]      NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at   timestamptz,
    CHECK (NOT ('admin' = ANY (scopes)) OR tenant_id IS NULL)
);
CREATE INDEX api_keys_tenant ON api_keys (tenant_id, id);

-- An endpoint is one destination on one channel: a webhook URL, an email route or a push route.
CREATE TABLE endpoints (
    id              uuid PRIMARY KEY,
    tenant_id       uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    channel         text        NOT NULL CHECK (channel IN ('webhook', 'email', 'push')),
    description     text        NOT NULL DEFAULT '',
    url             text,
    -- Webhook signing secret, encrypted with AES-256-GCM under RELAY_SECRET_KEY.
    secret_enc      bytea,
    -- After a rotation the old secret keeps signing too (a second signature) until it expires.
    previous_secret_enc        bytea,
    previous_secret_expires_at timestamptz,
    -- Default recipients of an email endpoint when the event names none.
    email_to        text[]      NOT NULL DEFAULT '{}',
    max_in_flight   integer     NOT NULL DEFAULT 4 CHECK (max_in_flight BETWEEN 1 AND 64),
    status          text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    disabled_reason text,
    disabled_at     timestamptz,
    failure_streak  integer     NOT NULL DEFAULT 0,
    failing_since   timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK ((channel = 'webhook') = (url IS NOT NULL AND secret_enc IS NOT NULL))
);
CREATE INDEX endpoints_tenant ON endpoints (tenant_id, id);

-- Subscriptions map event types to endpoints: exact types ("job.completed"), a prefix ("job.*") or "*".
CREATE TABLE subscriptions (
    id          uuid PRIMARY KEY,
    tenant_id   uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    endpoint_id uuid        NOT NULL REFERENCES endpoints (id) ON DELETE CASCADE,
    event_types text[]      NOT NULL CHECK (cardinality(event_types) > 0),
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX subscriptions_tenant ON subscriptions (tenant_id, id);
CREATE INDEX subscriptions_endpoint ON subscriptions (endpoint_id);

CREATE TABLE events (
    id              uuid PRIMARY KEY,
    tenant_id       uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    type            text        NOT NULL,
    payload         jsonb       NOT NULL,
    message         jsonb,
    targets         jsonb,
    idempotency_key text,
    request_hash    bytea       NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    routed_at       timestamptz,
    delivery_count  integer     NOT NULL DEFAULT 0
);
-- Keys are released after 24 hours by the cleanup job, so the same key can be used again later.
CREATE UNIQUE INDEX events_idempotency ON events (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX events_tenant ON events (tenant_id, id DESC);
CREATE INDEX events_unrouted ON events (created_at) WHERE routed_at IS NULL;
CREATE INDEX events_created ON events (created_at);

-- One delivery is one event going to one endpoint (and one recipient, for email and push).
-- PostgreSQL holds the state; Redis only carries the IDs that are due.
CREATE TABLE deliveries (
    id               uuid PRIMARY KEY,
    tenant_id        uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    event_id         uuid        NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    endpoint_id      uuid        NOT NULL REFERENCES endpoints (id) ON DELETE CASCADE,
    channel          text        NOT NULL,
    recipient        text        NOT NULL DEFAULT '',
    status           text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivering', 'delivered', 'skipped', 'dead')),
    attempts         integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    lease_until      timestamptz,
    last_status_code integer,
    last_error       text,
    last_latency_ms  integer,
    dead_reason      text,
    replay_of        uuid REFERENCES deliveries (id) ON DELETE SET NULL,
    replayed_at      timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz
);
CREATE UNIQUE INDEX deliveries_route ON deliveries (event_id, endpoint_id, recipient) WHERE replay_of IS NULL;
CREATE INDEX deliveries_tenant ON deliveries (tenant_id, id DESC);
CREATE INDEX deliveries_endpoint ON deliveries (endpoint_id, id DESC);
CREATE INDEX deliveries_event ON deliveries (event_id);
CREATE INDEX deliveries_dead ON deliveries (tenant_id, id DESC) WHERE status = 'dead' AND replayed_at IS NULL;
CREATE INDEX deliveries_due ON deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_leased ON deliveries (lease_until) WHERE status = 'delivering';

CREATE TABLE delivery_attempts (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id   uuid        NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
    attempt       integer     NOT NULL,
    started_at    timestamptz NOT NULL,
    latency_ms    integer     NOT NULL,
    status_code   integer,
    error         text,
    response_body text,
    outcome       text        NOT NULL CHECK (outcome IN ('delivered', 'retry', 'dead', 'skipped'))
);
CREATE INDEX delivery_attempts_delivery ON delivery_attempts (delivery_id, attempt);

-- Expo push tokens per user. A token the Expo service reports as DeviceNotRegistered is deleted.
CREATE TABLE push_tokens (
    id           uuid PRIMARY KEY,
    tenant_id    uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id      text        NOT NULL,
    token        text        NOT NULL,
    platform     text        NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    UNIQUE (tenant_id, token)
);
CREATE INDEX push_tokens_user ON push_tokens (tenant_id, user_id);

-- Expo push tickets waiting for their receipt (Expo publishes receipts within about 15 minutes).
CREATE TABLE push_tickets (
    ticket_id      text PRIMARY KEY,
    tenant_id      uuid        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    delivery_id    uuid REFERENCES deliveries (id) ON DELETE CASCADE,
    token          text        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    check_after    timestamptz NOT NULL,
    checked_at     timestamptz,
    receipt_status text,
    receipt_error  text
);
CREATE INDEX push_tickets_due ON push_tickets (check_after) WHERE checked_at IS NULL;

-- +goose Down
DROP TABLE push_tickets;
DROP TABLE push_tokens;
DROP TABLE delivery_attempts;
DROP TABLE deliveries;
DROP TABLE events;
DROP TABLE subscriptions;
DROP TABLE endpoints;
DROP TABLE api_keys;
DROP TABLE tenants;
