| Measure | Result |
|---|---|
| Target rate | 500 events/s for 2m |
| Events accepted (202) | 59,487 (495/s) |
| Failed requests | 0.000% |
| Dropped iterations | 514 |
| Ingest latency, client (k6) | p50 33.8 ms · p95 156.4 ms · p99 280.8 ms · max 1078 ms |
| Ingest latency, server | p50 32.0 ms · p95 174.4 ms · p99 250.0 ms |
| Webhooks delivered | 59,487 unique, 0 duplicates, 0 bad signatures |
| Delivery throughput | 496 webhooks/s |
| Delivery lag (ingest → webhook) | p50 38 ms · p95 226 ms · p99 599 ms |
| Backlog drained | 0 s after the last request |
| Relay CPU (4 containers, 2 CPUs = 200%) | mean 170% · peak 192% |
