# ADR 001: Redis Streams for work queues, PostgreSQL as the source of truth

**Status:** accepted (Relay 1.0, November 2026)

## Context

Relay needs a queue of deliveries waiting for an attempt (now, or later for a retry). The targets are at least
500 events a second on a 2-vCPU machine, a p99 ingest latency under 50 ms, at-least-once delivery, horizontal
scaling of workers, and per-tenant limits so that one slow endpoint can't hold every worker.

Two designs were compared.

**PostgreSQL only (`SELECT … FOR UPDATE SKIP LOCKED`).** Workers poll the deliveries table for due rows and
lock them. One store, transactional by construction, nothing to keep in sync. The costs: every idle worker
polls (latency is the poll interval, or `LISTEN/NOTIFY` on top), every claim is a write plus an index scan on the
hottest table, and per-tenant fairness needs either a scan that skips busy endpoints (expensive with a backlog)
or one query per tenant. Under a large backlog for one slow endpoint, the scan keeps meeting that endpoint's rows
first.

**Redis Streams with consumer groups.** Ingestion writes the event and its deliveries to PostgreSQL and appends
the delivery IDs to a stream. Workers block on `XREADGROUP`, so new work is picked up in milliseconds without
polling. A consumer group gives each entry to one worker; entries a dead worker never acknowledged are taken over
with `XAUTOCLAIM`. Delayed retries sit in a sorted set scored by due time. Token buckets and concurrency slots are
small Lua scripts, atomic across all workers.

## Decision

Use Redis Streams for the queue and Redis for the limits, but keep **PostgreSQL as the only source of truth**.
Redis holds IDs of work that is due, never state that can't be rebuilt:

- **Routing happens at ingestion.** The API matches the event against the tenant's subscriptions (cached) and
  commits the event and its deliveries in one transaction before answering `202`; only then do the delivery IDs
  go to `relay:deliveries`. There is no separate routing stage, no second stream and no "routed" flag to update.
- **Group commit.** Concurrent requests share transactions: a few flushers each take every request waiting and
  write them with one statement per table. One WAL flush covers many requests, and nothing waits on purpose.
- **An attempt in progress is held in Redis, not in PostgreSQL.** Before an attempt the worker takes the
  delivery's slot with a lease. It loads its batch of deliveries with one query (only those still `pending` and
  due) and records the results in batches with `UPDATE … WHERE status = 'pending' AND attempts = <what it read>`.
  A duplicate or stale queue entry matches nothing, and a second attempt never counts twice, so redelivery of
  queue entries is harmless. Each attempt costs one write.
- **The worker commits asynchronously** (`synchronous_commit = off` on its connections); the API doesn't. A lost
  worker write means an attempt is made again, which at-least-once delivery allows; a `202` always means the
  event is on disk.
- A **sweeper** on every worker requeues what Redis lost: deliveries long overdue and stream entries a dead worker
  never acknowledged (`XAUTOCLAIM` after the lease). It uses `SKIP LOCKED`, so several workers can sweep at once.
- Entries are acknowledged and deleted (`XACK` + `XDEL`), so the stream's length is the backlog.

**Limits.** Before an attempt, one Lua script takes a slot at the endpoint (its `max_in_flight`), a slot at the
tenant, and a token from the tenant's delivery bucket, all or nothing (pipelined for a whole batch). When a slot
isn't free the delivery is **parked** in that endpoint's (or tenant's) sorted set inside the same script, and the
worker moves on. Releasing a slot wakes the oldest parked delivery, again atomically, so no wake-up is lost and
parked work costs nothing while it waits. A slow endpoint therefore holds at most its own slots; the other
tenants' deliveries keep flowing (`TestSlowEndpointDoesNotStarveOthers`).

## How the load test shaped it

The first design routed events in the worker (a second stream), claimed each delivery with an `UPDATE` before the
attempt, committed every event on its own and kept eight indexes on `deliveries`. On two pinned CPUs it topped out
near 200 events a second with ingestion p99 over a second: PostgreSQL spent its time on index maintenance, WAL
locks and per-statement overhead, and backends were killed when 70 connections met a 256 MB memory limit. Each
step above removed one of those costs; [LOADTEST.md](../LOADTEST.md) has the final numbers.

## Consequences

- Losing Redis loses no events: `TestRecoveryAfterRedisDataLoss` flushes Redis and checks that the sweeper
  delivers everything. Redis still runs with an append-only file so a restart doesn't wait for the sweeper.
- Two stores to run instead of one. Redis memory stays small because it only holds IDs (about 110 bytes per
  queued delivery).
- Delivery is at least once. A crash after a webhook was sent but before the attempt was recorded sends it
  again; receivers deduplicate on `webhook-id` (the event ID).
- A subscription change reaches routing at once in the API process that made it and within milliseconds in the
  others (a Redis broadcast; entries also expire after 30 seconds).
- If Redis is unavailable, ingestion still stores events and their deliveries, and the sweeper queues them once
  Redis is back; rate limiting fails open meanwhile.
