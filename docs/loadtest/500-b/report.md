| Measure | Result |
|---|---|
| Target rate | 500 events/s for 2m |
| Events accepted (202) | 59,766 (497/s) |
| Failed requests | 0.000% |
| Dropped iterations | 234 |
| Ingest latency, client (k6) | p50 21.4 ms · p95 78.9 ms · p99 187.2 ms · max 615 ms |
| Ingest latency, server | p50 18.9 ms · p95 69.5 ms · p99 191.0 ms |
| Webhooks delivered | 59,766 unique, 0 duplicates, 0 bad signatures |
| Delivery throughput | 499 webhooks/s |
| Delivery lag (ingest → webhook) | p50 26 ms · p95 90 ms · p99 293 ms |
| Backlog drained | 0 s after the last request |
| Relay CPU (4 containers, 2 CPUs = 200%) | mean 164% · peak 180% |
