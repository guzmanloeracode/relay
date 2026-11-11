# Load test

<!-- RESULTS -->

## Method

`loadtest/run.sh` builds the image and starts a throwaway stack from `compose.yaml` with the overlay in
`loadtest/compose.yaml`:

- **Relay on two CPUs.** PostgreSQL 18, Redis 8, one API process and one worker process are all pinned to the
  same two cores (`cpuset: "2,3"`), the shape of a 2-vCPU virtual machine. Memory limits are the production
  ones: API 128 MB, worker 192 MB, PostgreSQL 256 MB, Redis 128 MB.
- **Load on the other cores.** k6, the webhook sink, Prometheus and Grafana run on cores 0 and 1, so they don't
  take Relay's CPU time.
- **Durability as in production.** PostgreSQL commits synchronously (`synchronous_commit=on`, `fsync=on`) and
  Redis keeps its append-only file; nothing is tuned for the benchmark.
- **The load:** a constant arrival rate of `POST /v1/events` (k6 `constant-arrival-rate`), each request with its
  own `Idempotency-Key` and a 700-byte payload shaped like a FieldOps `job.completed` event. Each event matches
  one webhook subscription, so every event is also one signed webhook to the sink.
- **The sink** (`relay sink`) verifies every signature, counts unique `webhook-id`s and measures the lag from the
  event's `created_at` to its arrival.
- **Pass criteria** (k6 thresholds): p99 of ingestion under 50 ms, under 0.1% failed requests, fewer than 10
  dropped iterations; then every accepted event must reach the sink, signed, once.

Server-side latency comes from Relay's own histogram (`relay_ingest_duration_seconds`, scraped by Prometheus);
client-side latency from k6. CPU use is sampled every two seconds with `docker stats` (200% = both cores busy).

The host is a shared 4-vCPU server that runs other services at the same time, so the numbers are a floor rather
than a best case.

## Run it

```bash
loadtest/run.sh                          # 600 events/s for 2 minutes
RATE=800 DURATION=5m loadtest/run.sh     # other rates and durations
KEEP=1 loadtest/run.sh                   # keep the stack (Grafana on http://127.0.0.1:13300)
```

Results land in `loadtest/results/<time>/`: the k6 summary, the sink's counts, the Prometheus queries, the CPU
samples and a `report.md` table.
