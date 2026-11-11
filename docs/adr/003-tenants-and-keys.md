# ADR 003: Tenants named by the client, keys bound to one tenant or to all

**Status:** accepted (Relay 1.0, November 2026)

## Context

Relay serves applications that have customers of their own. FieldOps, the first client, has one organization
per service business, and each organization has its own webhook endpoints and devices. The ingestion API
takes a tenant with each event (REL-01), and limits, subscriptions and logs are per tenant (REL-02, REL-04,
REL-05).

The client application should not have to store a Relay key per customer, and a customer should be able to get
a key that sees only its own data.

## Decision

- A **tenant** has a `slug` chosen by the client (for FieldOps, the organization's ID) and used in URLs
  (`/v1/tenants/{slug}/…`) and in events (`"tenant": "…"`). The client never needs to map its IDs to Relay's.
- An **API key** is either bound to one tenant or works for every tenant. Scopes: `ingest`, `read`, `manage`
  and `admin`. `admin` (tenants and keys) is only allowed on keys that are not bound to a tenant; a database
  check constraint enforces it.
- A key that can't see a tenant gets `404`, as if the tenant didn't exist, so keys can't probe for other
  tenants.
- Keys are 40 random base62 characters (about 238 bits) after `rk_`, stored as SHA-256 hashes. A slow hash adds
  nothing at that entropy and would cost latency on every request. Lookups are cached in each process for 30
  seconds; a revocation is broadcast over Redis so other processes drop the key at once.
- The first admin key comes from the command line (`relay keys create --admin`), so the API never has an
  unauthenticated bootstrap route.

## Consequences

- FieldOps uses one service key (`ingest`, `read`, `manage`) for every organization and creates tenants with
  an admin key when an organization turns on integrations.
- A tenant slug can't be renamed; a new tenant and a migration of its endpoints would be needed.
