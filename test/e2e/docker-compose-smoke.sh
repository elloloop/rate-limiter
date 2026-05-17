#!/usr/bin/env bash
set -euo pipefail

PROJECT="${QUOTA_E2E_PROJECT:-rate-limiter-e2e}"
GRPC_PORT="${QUOTA_E2E_GRPC_PORT:-38080}"
METRICS_PORT="${QUOTA_E2E_METRICS_PORT:-39090}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.e2e.yml"

for bin in docker grpcurl curl; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "missing required command: $bin" >&2
    exit 1
  fi
done

cleanup() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

export QUOTA_E2E_GRPC_PORT="$GRPC_PORT"
export QUOTA_E2E_METRICS_PORT="$METRICS_PORT"

cleanup
docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d --build

status_json=""
for _ in $(seq 1 60); do
  if status_json="$(grpcurl -plaintext -d '{}' "localhost:$GRPC_PORT" quota.v1.QuotaService/GetRedisStatus 2>/dev/null)" &&
    grep -q '"reachable": true' <<<"$status_json" &&
    grep -q '"message": "ok"' <<<"$status_json"; then
    break
  fi
  sleep 1
done

if ! grep -q '"reachable": true' <<<"$status_json"; then
  echo "quota service did not become ready" >&2
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" logs >&2 || true
  exit 1
fi

consume_payload='{
  "requestId": "req-e2e-consume",
  "context": {"product": "workspace", "environment": "test"},
  "action": "workspace.email.recipients",
  "cost": "25",
  "limits": [{
    "limitId": "user_email_recipients_daily",
    "scopeKey": "user:user_123",
    "action": "workspace.email.recipients",
    "unit": "recipients",
    "algorithm": "ALGORITHM_FIXED_WINDOW_CALENDAR",
    "window": {
      "type": "WINDOW_TYPE_CALENDAR",
      "calendarUnit": "CALENDAR_UNIT_DAY",
      "timezone": "UTC"
    },
    "limit": "30"
  }]
}'

consume_json="$(grpcurl -plaintext -d "$consume_payload" "localhost:$GRPC_PORT" quota.v1.QuotaService/Consume)"
grep -q '"allowed": true' <<<"$consume_json"
grep -q '"remaining": "5"' <<<"$consume_json"

replay_json="$(grpcurl -plaintext -d "$consume_payload" "localhost:$GRPC_PORT" quota.v1.QuotaService/Consume)"
grep -q '"idempotency_hit": "true"' <<<"$replay_json"

deny_payload="${consume_payload/req-e2e-consume/req-e2e-deny}"
deny_payload="${deny_payload/\"cost\": \"25\"/\"cost\": \"10\"}"
deny_json="$(grpcurl -plaintext -d "$deny_payload" "localhost:$GRPC_PORT" quota.v1.QuotaService/Consume)"
if grep -q '"allowed": true' <<<"$deny_json"; then
  echo "expected denial response, got: $deny_json" >&2
  exit 1
fi
grep -q '"reason": "DECISION_REASON_LIMIT_EXCEEDED"' <<<"$deny_json"

metrics="$(curl -fsS "http://localhost:$METRICS_PORT/metrics")"
grep -q 'quota_requests_total' <<<"$metrics"
grep -q 'quota_denials_total' <<<"$metrics"

echo "docker compose e2e smoke passed"
