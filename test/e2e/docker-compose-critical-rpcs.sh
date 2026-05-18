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

grpc() {
  local method="$1"
  local payload="$2"
  grpcurl -plaintext -d "$payload" "localhost:$GRPC_PORT" "quota.v1.QuotaService/$method"
}

require_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if ! grep -Fq "$needle" <<<"$haystack"; then
    echo "expected $label to contain: $needle" >&2
    echo "$haystack" >&2
    exit 1
  fi
}

require_not_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if grep -Fq "$needle" <<<"$haystack"; then
    echo "expected $label not to contain: $needle" >&2
    echo "$haystack" >&2
    exit 1
  fi
}

json_string_field() {
  local field="$1"
  local json="$2"
  grep -m1 "\"$field\"" <<<"$json" | sed -E "s/.*\"$field\": \"([^\"]+)\".*/\1/"
}

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

health_json="$(grpcurl -plaintext -d '{"service":"quota.v1.QuotaService"}' "localhost:$GRPC_PORT" grpc.health.v1.Health/Check)"
require_contains "$health_json" '"status": "SERVING"' "gRPC health response"

email_limit='{
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
}'

token_limit='{
  "limitId": "user_daily_tokens",
  "scopeKey": "user:user_123",
  "action": "assistant.llm.tokens",
  "unit": "tokens",
  "algorithm": "ALGORITHM_FIXED_WINDOW_CALENDAR",
  "window": {
    "type": "WINDOW_TYPE_CALENDAR",
    "calendarUnit": "CALENDAR_UNIT_DAY",
    "timezone": "UTC"
  },
  "limit": "100",
  "refundable": true,
  "reservationExpiryPolicy": "RESERVATION_EXPIRY_POLICY_CHARGE_FULL"
}'

concurrency_limit='{
  "limitId": "user_llm_concurrency",
  "scopeKey": "user:user_123",
  "action": "assistant.llm.concurrent",
  "unit": "requests",
  "algorithm": "ALGORITHM_CONCURRENCY",
  "limit": "1"
}'

validate_json="$(grpc ValidateLimits "{
  \"limits\": [$email_limit, $token_limit, $concurrency_limit]
}")"
require_contains "$validate_json" '"valid": true' "ValidateLimits response"

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

consume_json="$(grpc Consume "$consume_payload")"
require_contains "$consume_json" '"allowed": true' "Consume allow response"
require_contains "$consume_json" '"remaining": "5"' "Consume allow response"

replay_json="$(grpc Consume "$consume_payload")"
require_contains "$replay_json" '"idempotency_hit": "true"' "Consume replay response"

usage_json="$(grpc GetCurrentUsage "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.email.recipients\",
  \"limits\": [$email_limit]
}")"
require_contains "$usage_json" '"used": "25"' "GetCurrentUsage response"
require_contains "$usage_json" '"remaining": "5"' "GetCurrentUsage response"

explain_json="$(grpc Explain "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.email.recipients\",
  \"cost\": \"6\",
  \"limits\": [$email_limit]
}")"
require_not_contains "$explain_json" '"wouldAllow": true' "Explain response"
require_contains "$explain_json" '"reason": "DECISION_REASON_LIMIT_EXCEEDED"' "Explain response"

deny_payload="${consume_payload/req-e2e-consume/req-e2e-deny}"
deny_payload="${deny_payload/\"cost\": \"25\"/\"cost\": \"10\"}"
deny_json="$(grpc Consume "$deny_payload")"
require_not_contains "$deny_json" '"allowed": true' "Consume denial response"
require_contains "$deny_json" '"reason": "DECISION_REASON_LIMIT_EXCEEDED"' "Consume denial response"

reserve_json="$(grpc Reserve "{
  \"requestId\": \"req-e2e-reserve\",
  \"context\": {\"product\": \"assistant\", \"environment\": \"test\", \"metadata\": {\"model\": \"dummy-llm\"}},
  \"action\": \"assistant.llm.tokens\",
  \"reserveCost\": \"40\",
  \"reservationTtlMs\": \"60000\",
  \"limits\": [$token_limit]
}")"
require_contains "$reserve_json" '"allowed": true' "Reserve response"
reservation_id="$(json_string_field reservationId "$reserve_json")"
if [[ -z "$reservation_id" ]]; then
  echo "Reserve response did not include reservationId" >&2
  echo "$reserve_json" >&2
  exit 1
fi

get_reservation_json="$(grpc GetReservation "{\"reservationId\": \"$reservation_id\"}")"
require_contains "$get_reservation_json" "\"reservationId\": \"$reservation_id\"" "GetReservation active response"
require_contains "$get_reservation_json" '"status": "RESERVATION_STATUS_ACTIVE"' "GetReservation active response"
require_contains "$get_reservation_json" '"redisKey": "' "GetReservation active response"

finalize_json="$(grpc FinalizeReservation "{
  \"requestId\": \"req-e2e-finalize\",
  \"reservationId\": \"$reservation_id\",
  \"actualCost\": \"25\",
  \"status\": \"FINALIZE_STATUS_SUCCEEDED\",
  \"metadata\": {\"result\": \"dummy-success\"}
}")"
require_contains "$finalize_json" '"reservedCost": "40"' "FinalizeReservation response"
require_contains "$finalize_json" '"actualCost": "25"' "FinalizeReservation response"
require_contains "$finalize_json" '"refundedCost": "15"' "FinalizeReservation response"
require_contains "$finalize_json" '"finalized": true' "FinalizeReservation response"

finalized_reservation_json="$(grpc GetReservation "{\"reservationId\": \"$reservation_id\"}")"
require_contains "$finalized_reservation_json" '"status": "RESERVATION_STATUS_FINALIZED"' "GetReservation finalized response"
require_contains "$finalized_reservation_json" '"actualCost": "25"' "GetReservation finalized response"

release_reserve_json="$(grpc Reserve "{
  \"requestId\": \"req-e2e-release-reserve\",
  \"context\": {\"product\": \"assistant\", \"environment\": \"test\"},
  \"action\": \"assistant.llm.tokens\",
  \"reserveCost\": \"10\",
  \"reservationTtlMs\": \"60000\",
  \"limits\": [$token_limit]
}")"
require_contains "$release_reserve_json" '"allowed": true' "Reserve-for-release response"
release_reservation_id="$(json_string_field reservationId "$release_reserve_json")"
if [[ -z "$release_reservation_id" ]]; then
  echo "Reserve-for-release response did not include reservationId" >&2
  echo "$release_reserve_json" >&2
  exit 1
fi

release_reservation_json="$(grpc ReleaseReservation "{
  \"requestId\": \"req-e2e-release-reservation\",
  \"reservationId\": \"$release_reservation_id\",
  \"reason\": \"dummy operation cancelled\"
}")"
require_contains "$release_reservation_json" '"releasedCost": "10"' "ReleaseReservation response"
require_contains "$release_reservation_json" '"released": true' "ReleaseReservation response"

released_reservation_json="$(grpc GetReservation "{\"reservationId\": \"$release_reservation_id\"}")"
require_contains "$released_reservation_json" '"status": "RESERVATION_STATUS_RELEASED"' "GetReservation released response"

lease_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-acquire-lease\",
  \"context\": {\"product\": \"assistant\", \"environment\": \"test\"},
  \"action\": \"assistant.llm.concurrent\",
  \"limits\": [$concurrency_limit],
  \"leaseTtlMs\": \"60000\"
}")"
require_contains "$lease_json" '"allowed": true' "AcquireLease response"
lease_id="$(json_string_field leaseId "$lease_json")"
if [[ -z "$lease_id" ]]; then
  echo "AcquireLease response did not include leaseId" >&2
  echo "$lease_json" >&2
  exit 1
fi

get_lease_json="$(grpc GetLease "{\"leaseId\": \"$lease_id\"}")"
require_contains "$get_lease_json" "\"leaseId\": \"$lease_id\"" "GetLease active response"
require_contains "$get_lease_json" '"status": "LEASE_STATUS_ACTIVE"' "GetLease active response"

lease_denied_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-acquire-lease-denied\",
  \"context\": {\"product\": \"assistant\", \"environment\": \"test\"},
  \"action\": \"assistant.llm.concurrent\",
  \"limits\": [$concurrency_limit],
  \"leaseTtlMs\": \"60000\"
}")"
require_not_contains "$lease_denied_json" '"allowed": true' "AcquireLease denial response"
require_contains "$lease_denied_json" '"reason": "DECISION_REASON_CONCURRENCY_EXCEEDED"' "AcquireLease denial response"

renew_json="$(grpc RenewLease "{
  \"requestId\": \"req-e2e-renew-lease\",
  \"leaseId\": \"$lease_id\",
  \"extendTtlMs\": \"120000\"
}")"
require_contains "$renew_json" '"renewed": true' "RenewLease response"
require_contains "$renew_json" "\"leaseId\": \"$lease_id\"" "RenewLease response"

release_lease_json="$(grpc ReleaseLease "{
  \"requestId\": \"req-e2e-release-lease\",
  \"leaseId\": \"$lease_id\"
}")"
require_contains "$release_lease_json" "\"leaseId\": \"$lease_id\"" "ReleaseLease response"
require_contains "$release_lease_json" '"released": true' "ReleaseLease response"

released_lease_json="$(grpc GetLease "{\"leaseId\": \"$lease_id\"}")"
require_contains "$released_lease_json" '"status": "LEASE_STATUS_RELEASED"' "GetLease released response"

metrics="$(curl -fsS "http://localhost:$METRICS_PORT/metrics")"
require_contains "$metrics" 'quota_requests_total' "metrics output"
require_contains "$metrics" 'quota_denials_total' "metrics output"
require_contains "$metrics" 'quota_reservations_active' "metrics output"
require_contains "$metrics" 'quota_leases_active' "metrics output"

echo "docker compose critical RPC e2e passed"
