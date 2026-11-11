#!/usr/bin/env bash
# Smoke test of a built image before it goes live: a throwaway Compose project (its own PostgreSQL, Redis and a
# signature-checking sink) runs the new image, then a tenant, an endpoint and events go through the public API
# and the sink must receive every event, signed, exactly once.
#   TAG=<image tag> deploy/e2e.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TAG="${TAG:?set TAG to the image tag to test}"
PROJECT="relay-e2e-$$"
PORT="${E2E_PORT:-18098}"
EVENTS=50

envfile="$(mktemp)"
override="$(mktemp --suffix=.yaml)"
cleanup() {
  docker compose -p "$PROJECT" --env-file "$envfile" -f "$ROOT/compose.yaml" -f "$override" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "$envfile" "$override"
}
trap cleanup EXIT

cat > "$envfile" <<EOF
TAG=$TAG
POSTGRES_PASSWORD=e2e-$(openssl rand -hex 8)
REDIS_PASSWORD=e2e-$(openssl rand -hex 8)
RELAY_SECRET_KEY=$(openssl rand -base64 32)
RELAY_PORT=$PORT
ALLOW_PRIVATE_NETWORKS=true
LOG_LEVEL=warn
EOF
cat > "$override" <<EOF
services:
  sink:
    image: relay:$TAG
    command: ["sink", ":8090"]
    read_only: true
    ports: ["127.0.0.1:$((PORT + 1)):8090"]
EOF
compose() { docker compose -p "$PROJECT" --env-file "$envfile" -f "$ROOT/compose.yaml" -f "$override" "$@"; }

compose up -d --no-build --wait api worker sink >/dev/null 2>&1 || { compose logs --tail 50; echo "e2e: stack did not start"; exit 1; }
api="http://127.0.0.1:$PORT"
sink="http://127.0.0.1:$((PORT + 1))"
admin="$(compose run --rm --no-deps -T migrate keys create --admin --name e2e 2>/dev/null | tail -1)"
[ -n "$admin" ] || { echo "e2e: could not create an admin key"; exit 1; }

j() { curl -fsS -H "Authorization: Bearer $1" -H 'Content-Type: application/json' "${@:2}"; }
field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }

j "$admin" "$api/v1/tenants" -d '{"slug":"e2e","name":"E2E"}' >/dev/null
key="$(j "$admin" "$api/v1/tenants/e2e/keys" -d '{"name":"e2e"}' | field '["key"]')"
secret="$(j "$key" "$api/v1/tenants/e2e/endpoints" \
  -d '{"channel":"webhook","url":"http://sink:8090/hook","event_types":["e2e.*"]}' | field '["secret"]')"
curl -fsS -X PUT "$sink/config" -d "{\"secret\":\"$secret\"}"

for i in $(seq 1 "$EVENTS"); do
  j "$key" "$api/v1/events" -H "Idempotency-Key: e2e-$i" -d "{\"type\":\"e2e.ping\",\"payload\":{\"n\":$i}}" >/dev/null
done
# The same key again must not create another event.
j "$key" "$api/v1/events" -H "Idempotency-Key: e2e-1" -d '{"type":"e2e.ping","payload":{"n":1}}' >/dev/null

for _ in $(seq 1 60); do
  unique="$(curl -fsS "$sink/stats" | field '["unique"]')"
  [ "$unique" = "$EVENTS" ] && break
  sleep 0.5
done
stats="$(curl -fsS "$sink/stats")"
echo "e2e sink: $stats"
python3 - "$stats" "$EVENTS" <<'PY'
import json, sys
s, n = json.loads(sys.argv[1]), int(sys.argv[2])
assert s["unique"] == n, f"{s['unique']} of {n} events arrived"
assert s["duplicates"] == 0, "duplicate deliveries"
assert s["bad_signatures"] == 0, "bad signatures"
PY
delivered="$(j "$key" "$api/v1/tenants/e2e/deliveries?status=delivered&limit=100" | field '["data"].__len__()')"
[ "$delivered" = "$EVENTS" ] || { echo "e2e: the delivery log shows $delivered delivered"; exit 1; }
spec="$(curl -fsS "$api/openapi.yaml")"
[[ "$spec" == "openapi: 3.1"* ]] || { echo "e2e: no OpenAPI document"; exit 1; }
ready="$(curl -fsS "$api/readyz")"
[[ "$ready" == *'"status":"ok"'* ]] || { echo "e2e: not ready: $ready"; exit 1; }
echo "e2e passed: $EVENTS events delivered and verified"
