| Measure | Result |
|---|---|
| Target rate | 300 events/s for 2m |
| Events accepted (202) | 35,798 (298/s) |
| Failed requests | 0.000% |
| Dropped iterations | 202 |
| Ingest latency, client (k6) | p50 18.7 ms · p95 69.7 ms · p99 164.6 ms · max 780 ms |
| Ingest latency, server | p50 18.0 ms · p95 67.3 ms · p99 203.6 ms |
| Webhooks delivered | 35,798 unique, 0 duplicates, 0 bad signatures |
| Delivery throughput | 299 webhooks/s |
| Delivery lag (ingest → webhook) | p50 23 ms · p95 66 ms · p99 173 ms |
| Backlog drained | 0 s after the last request |
| Relay CPU (4 containers, 2 CPUs = 200%) | mean 163% · peak 180% |
