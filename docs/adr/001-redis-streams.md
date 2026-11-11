# ADR 001: Redis Streams for work queues, PostgreSQL as the source of truth

**Status:** accepted (Relay 1.0, November 2026)

## Context

Relay needs two queues: events waiting to be routed into deliveries, and deliveries waiting for an attempt
(now, or later for a retry). The targets are at least 500 events a second on a 2-vCPU machine, a p99 ingest
latency under 50 ms, at-least-once delivery, horizontal scaling of workers, and per-tenant limits so that one
slow endpoint can't hold every worker.

Two designs were compared.

**PostgreSQL only (`SELECT … FOR UPDATE SKIP LOCKED`).** Workers poll the deliveries table for due rows and
lock them. One store, transactional by construction, nothing to keep in sync. The costs: every idle worker
polls (latency is the poll interval, or `LISTEN/NOTIFY` on top), every claim is an index scan on a hot table
that also takes inserts and updates, and per-tenant fairness needs either a scan that skips busy endpoints
(expensive at backlog) or one query per tenant. Under a large backlog for one slow endpoint, the scan keeps
meeting that endpoint's rows first.

**Redis Streams with consumer groups.** Ingestion writes the event to PostgreSQL and appends its ID to a
stream. Workers block on `XREADGROUP`, so new work is picked up in milliseconds without polling. A consumer
group gives each entry to one worker; entries a dead worker never acknowledged are taken over with
`XAUTOCLAIM`. Delayed retries sit in a sorted set scored by due time. Token buckets and concurrency slots are
small Lua scripts, atomic across all workers.

## Decision

Use Redis Streams for the queues and Redis for the limits, but keep **PostgreSQL as the only source of truth**.
Redis holds IDs of work that is due, never state that can't be rebuilt:

- An event row is committed before its ID goes to the `relay:events` stream. A delivery row (status, attempts,
  next attempt time) is committed before its ID goes to `relay:deliveries` or the `relay:delayed` set.
- An attempt in progress is held by the delivery's **slot in Redis** (a lease), not by a write to PostgreSQL.
  The worker reads the delivery only if it is still `pending` and due, and records the result with one
  `UPDATE … WHERE status = 'pending' AND attempts = <what it read>`. A duplicate or stale queue entry matches
  nothing, and a second attempt never counts twice, which makes redelivery of queue entries harmless.
- A **sweeper** on every worker requeues what Redis lost: events never routed, deliveries long overdue, and
  stream entries a dead worker never acknowledged (`XAUTOCLAIM` after the lease). It uses `SKIP LOCKED`, so
  several workers can sweep at once.
- Entries are acknowledged and deleted (`XACK` + `XDEL`), so a stream's length is its backlog.

**Limits.** Before claiming, a worker runs one Lua script that takes a slot at the endpoint (its
`max_in_flight`), a slot at the tenant, and a token from the tenant's delivery bucket, all or nothing. When a
slot isn't free the delivery is **parked** in that endpoint's (or tenant's) sorted set inside the same script,
and the worker moves on. Releasing a slot wakes the oldest parked delivery, again atomically, so no wake-up is
lost and parked work costs nothing while it waits. A slow endpoint therefore holds at most its own slots; the
other tenants' deliveries keep flowing (`TestSlowEndpointDoesNotStarveOthers`).

The first load test showed the cost of the other choice: claiming each delivery with an `UPDATE` before the
attempt doubled the writes per delivery, and with eight indexes on `deliveries` PostgreSQL saturated its CPU
on index maintenance and WAL locks at about 200 events a second. Holding the attempt in Redis instead, cutting
the indexes to what queries use, and committing the worker's writes asynchronously (a lost write only means a
repeated attempt) removed that ceiling.

## Consequences

- Ingestion is one indexed insert and one `XADD`: the k6 run measured a server-side p99 well under 50 ms.
- Losing Redis loses no events: `TestRecoveryAfterRedisDataLoss` flushes Redis and checks that the sweeper
  delivers everything. Redis still runs with an append-only file so a restart doesn't wait for the sweeper.
- Two stores to run instead of one. Redis memory stays small because it only holds IDs (about 110 bytes per
  queued delivery).
- Delivery is at least once. A crash after a webhook was sent but before the attempt was recorded sends it
  again; receivers deduplicate on `webhook-id` (the event ID).
- If Redis is unavailable, ingestion still accepts events (they are durable in PostgreSQL) and the sweeper
  queues them once Redis is back; rate limiting fails open meanwhile.
