#!/usr/bin/env bash
# Runs the k6 load test against a throwaway stack: Relay (PostgreSQL, Redis, API, worker) pinned to two CPUs,
# k6 and a signature-checking sink on the others. Results land in loadtest/results/<time>/.
#   loadtest/run.sh                       600 events/s for 2 minutes
#   RATE=800 DURATION=3m loadtest/run.sh
#   KEEP=1 loadtest/run.sh                leave the stack up (Grafana on 127.0.0.1:13300)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export RATE="${RATE:-600}" DURATION="${DURATION:-2m}"
export TAG="${TAG:-loadtest}"
OUT="$ROOT/loadtest/results/$(date -u +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
chmod 0777 "$OUT" # k6 runs as an unprivileged user and writes its summary here
export RESULTS_DIR="$OUT"
PROJECT=relay-load

envfile="$OUT/.env"
cat > "$envfile" <<EOF
TAG=$TAG
POSTGRES_PASSWORD=load-$(openssl rand -hex 8)
REDIS_PASSWORD=load-$(openssl rand -hex 8)
RELAY_SECRET_KEY=$(openssl rand -base64 32)
RELAY_PORT=18198
SINK_PORT=18190
PROMETHEUS_PORT=19090
GRAFANA_PORT=13300
ALLOW_PRIVATE_NETWORKS=true
LOG_LEVEL=warn
WORKER_CONCURRENCY=128
API_CPUS=2
WORKER_CPUS=2
PG_CPUS=2
REDIS_CPUS=2
EOF
compose() { docker compose -p "$PROJECT" --env-file "$envfile" -f compose.yaml -f loadtest/compose.yaml --profile observability "$@"; }
cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "stack left running: docker compose -p $PROJECT --env-file $envfile -f compose.yaml -f loadtest/compose.yaml --profile observability down -v"
    return
  fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "$envfile"
}
trap cleanup EXIT

echo "building relay:$TAG"
docker build -q -t "relay:$TAG" --build-arg VERSION="$(cat VERSION)" --build-arg COMMIT="$(git rev-parse --short=12 HEAD)" . >/dev/null
compose up -d --no-build --wait postgres redis api worker sink prometheus grafana >/dev/null
admin="$(compose run --rm --no-deps -T migrate keys create --admin --name k6 2>/dev/null | tail -1)"

# CPU and memory of every container, sampled while k6 runs.
( while sleep 2; do docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' | grep "^$PROJECT-" | sed "s/^/$(date +%s)\t/"; done ) > "$OUT/docker-stats.tsv" &
sampler=$!
start="$(date +%s)"
set +e
compose --profile k6 run --rm --no-deps -T -e ADMIN_KEY="$admin" k6 run --quiet --summary-export=/results/k6-summary.json /scripts/ingest.js | tee "$OUT/k6.txt"
k6_status=${PIPESTATUS[0]}
set -e
end="$(date +%s)"

accepted="$(python3 -c "import json; print(int(json.load(open('$OUT/k6-summary.json'))['metrics']['events_accepted']['count']))")"
echo "waiting for $accepted deliveries…"
for _ in $(seq 1 300); do
  unique="$(curl -fsS http://127.0.0.1:18190/stats | python3 -c 'import json,sys; print(json.load(sys.stdin)["unique"])')"
  [ "$unique" -ge "$accepted" ] && break
  sleep 1
done
drained="$(date +%s)"
kill "$sampler" 2>/dev/null || true
curl -fsS http://127.0.0.1:18190/stats > "$OUT/sink.json"
# Where PostgreSQL spent its time.
compose exec -T postgres psql -U relay -d relay -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
compose exec -T postgres psql -U relay -d relay -c "SELECT calls, round(total_exec_time) AS total_ms,
  round(mean_exec_time::numeric, 2) AS mean_ms, rows, left(regexp_replace(query, '\s+', ' ', 'g'), 90) AS query
  FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 15" > "$OUT/pg-statements.txt" || true

# Server-side numbers from Prometheus over the run.
q() { curl -fsS --get http://127.0.0.1:19090/api/v1/query --data-urlencode "query=$1" --data-urlencode "time=$end"; }
window="$((end - start))s"
{
  echo "{"
  for p in 0.5 0.95 0.99; do
    echo "\"ingest_p${p#0.}\": $(q "histogram_quantile($p, sum by (le) (increase(relay_ingest_duration_seconds_bucket[$window])))"),"
  done
  echo "\"attempt_p99\": $(q "histogram_quantile(0.99, sum by (le) (increase(relay_delivery_attempt_duration_seconds_bucket[$window])))"),"
  echo "\"attempts\": $(q "sum by (outcome) (increase(relay_delivery_attempts_total[$window]))")"
  echo "}"
} > "$OUT/prometheus.json"

python3 - "$OUT" "$RATE" "$DURATION" "$start" "$end" "$drained" <<'PY'
import json, sys, collections
out, rate, duration, start, end, drained = sys.argv[1], sys.argv[2], sys.argv[3], *map(int, sys.argv[4:])
k6 = json.load(open(f"{out}/k6-summary.json"))["metrics"]
sink = json.load(open(f"{out}/sink.json"))
prom = json.load(open(f"{out}/prometheus.json"))
lat = k6["http_req_duration{name:ingest}"]
reqs = k6["http_reqs"]
def pv(key):
    r = prom[key]["data"]["result"]
    return float(r[0]["value"][1]) * 1000 if r else float("nan")
cpu = collections.defaultdict(list)
for line in open(f"{out}/docker-stats.tsv"):
    parts = line.rstrip("\n").split("\t")
    if len(parts) >= 3 and start <= int(parts[0]) <= end:
        cpu[parts[1]].append(float(parts[2].rstrip("%")))
relay_cpu = [sum(v) for v in zip(*[cpu[n] for n in cpu if any(s in n for s in ("-api-", "-worker-", "-postgres-", "-redis-"))])]
rows = [
    ("Target rate", f"{rate} events/s for {duration}"),
    ("Events accepted (202)", f"{int(k6['events_accepted']['count']):,} ({k6['events_accepted']['rate']:.0f}/s)"),
    ("Failed requests", f"{k6['http_req_failed{name:ingest}']['value'] * 100:.3f}%"),
    ("Dropped iterations", f"{int(k6.get('dropped_iterations', {}).get('count', 0))}"),
    ("Ingest latency, client (k6)", f"p50 {lat['med']:.1f} ms · p95 {lat['p(95)']:.1f} ms · p99 {lat['p(99)']:.1f} ms · max {lat['max']:.0f} ms"),
    ("Ingest latency, server", f"p50 {pv('ingest_p5'):.1f} ms · p95 {pv('ingest_p95'):.1f} ms · p99 {pv('ingest_p99'):.1f} ms"),
    ("Webhooks delivered", f"{sink['unique']:,} unique, {sink['duplicates']} duplicates, {sink['bad_signatures']} bad signatures"),
    ("Delivery throughput", f"{sink['per_second']:.0f} webhooks/s"),
    ("Delivery lag (ingest → webhook)", f"p50 {sink['lag_p50_ms']:.0f} ms · p95 {sink['lag_p95_ms']:.0f} ms · p99 {sink['lag_p99_ms']:.0f} ms"),
    ("Backlog drained", f"{drained - end} s after the last request"),
    ("Relay CPU (4 containers, 2 CPUs = 200%)", f"mean {sum(relay_cpu)/max(len(relay_cpu),1):.0f}% · peak {max(relay_cpu or [0]):.0f}%"),
]
with open(f"{out}/report.md", "w") as f:
    f.write("| Measure | Result |\n|---|---|\n")
    for k, v in rows:
        f.write(f"| {k} | {v} |\n")
print(open(f"{out}/report.md").read())
PY
exit "$k6_status"
