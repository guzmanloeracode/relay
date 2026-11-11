#!/usr/bin/env bash
# Builds and deploys Relay on this server. There is no hosted CI: this script is the pipeline.
#   deploy/release.sh              verify, build HEAD, smoke-test the new image, deploy, health check, auto-rollback
#   SKIP_VERIFY=1 deploy/release.sh   skip the verify gate (docs-only changes)
#   SKIP_E2E=1 / SKIP_BUILD=1         skip the smoke test / reuse an image already built for HEAD
#   SKIP_PUBLIC=1                     don't check the public URL (before DNS exists)
# Full log: /var/log/relay-release.log
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/relay/relay.env}"
COMPOSE=(docker compose -p relay --env-file "$ENV_FILE" -f "$ROOT/compose.yaml")
LOG=/var/log/relay-release.log
PUBLIC_URL="${PUBLIC_URL:-https://relay.tinggalpake.com}"
LOCAL_URL="http://127.0.0.1:$(grep -E '^RELAY_PORT=' "$ENV_FILE" | cut -d= -f2 || echo 8098)"

cd "$ROOT"
COMMIT="$(git rev-parse --short=12 HEAD)"
VERSION="$(cat VERSION)"
export TAG="$COMMIT"
exec 3>&1 >>"$LOG" 2>&1
say() { echo "$*" >&3; echo "[$(date -Is)] $*"; }

say "relay $VERSION ($COMMIT)"
[ -z "$(git status --porcelain -- cmd internal pkg api go.mod go.sum Dockerfile compose.yaml)" ] ||
  say "warning: uncommitted changes are not deployed (building HEAD)"

# The containerd image store drops an image as soon as its tag moves, so keep the running one first.
current="$(docker inspect --format '{{.Config.Image}}' relay-api-1 2>/dev/null || true)"
if [ -n "$current" ] && [[ "$current" == relay:* ]] && [ "$current" != "relay:$TAG" ]; then
  docker tag "$current" relay:rollback
fi

if [ "${SKIP_VERIFY:-0}" != "1" ]; then
  "$ROOT/scripts/verify.sh" || { say "verify failed: nothing deployed (see $LOG)"; exit 1; }
  say "verify passed"
fi

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  ctx="$(mktemp -d)"
  trap 'rm -rf "$ctx"' EXIT
  git archive HEAD | tar -x -C "$ctx"
  docker build -q -t "relay:$TAG" --build-arg VERSION="$VERSION" --build-arg COMMIT="$COMMIT" "$ctx" >/dev/null
  say "built relay:$TAG"
fi

if [ "${SKIP_E2E:-0}" != "1" ]; then
  TAG="$TAG" "$ROOT/deploy/e2e.sh" || { say "e2e failed: nothing deployed (see $LOG)"; exit 1; }
  say "e2e passed"
fi

rollback() {
  say "health check failed: rolling back"
  if docker image inspect relay:rollback >/dev/null 2>&1; then
    TAG=rollback "${COMPOSE[@]}" up -d --no-build api worker
    say "rolled back to the previous image"
  fi
  exit 1
}

"${COMPOSE[@]}" up -d --no-build --remove-orphans postgres redis migrate api worker
for i in $(seq 1 60); do
  body="$(curl -fsS "$LOCAL_URL/readyz" 2>/dev/null || true)"
  if [[ "$body" == *'"status":"ok"'* && "$body" == *"$COMMIT"* ]]; then break; fi
  [ "$i" = 60 ] && rollback
  sleep 2
done
worker_state="$(docker inspect --format '{{.State.Health.Status}}' relay-worker-1 2>/dev/null || echo missing)"
for _ in $(seq 1 30); do
  [ "$worker_state" = healthy ] && break
  sleep 2
  worker_state="$(docker inspect --format '{{.State.Health.Status}}' relay-worker-1 2>/dev/null || echo missing)"
done
[ "$worker_state" = healthy ] || rollback

if [ "${SKIP_PUBLIC:-0}" != "1" ]; then
  code="$(curl -s -o /dev/null -w '%{http_code}' "$PUBLIC_URL/healthz")"
  docs="$(curl -s -o /dev/null -w '%{http_code}' "$PUBLIC_URL/openapi.yaml")"
  [ "$code" = 200 ] && [ "$docs" = 200 ] || rollback
  say "live: $PUBLIC_URL (healthz: $code, openapi: $docs)"
else
  say "live on $LOCAL_URL (public URL not checked)"
fi
docker image prune -f --filter "label!=keep" >/dev/null 2>&1 || true
