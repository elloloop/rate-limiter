#!/usr/bin/env bash
set -euo pipefail

PROJECT="${QUOTA_E2E_PROJECT:-rate-limiter-e2e}"
GRPC_PORT="${QUOTA_E2E_GRPC_PORT:-38080}"
METRICS_PORT="${QUOTA_E2E_METRICS_PORT:-39090}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.e2e.yml"

for bin in docker grpcurl curl python3; do
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

grpc_health() {
  grpcurl -plaintext -d '{"service":"quota.v1.QuotaService"}' "localhost:$GRPC_PORT" grpc.health.v1.Health/Check
}

grpc_expect_error() {
  local method="$1"
  local payload="$2"
  local out
  set +e
  out="$(grpcurl -plaintext -d "$payload" "localhost:$GRPC_PORT" "quota.v1.QuotaService/$method" 2>&1)"
  local code=$?
  set -e
  if [[ "$code" -eq 0 ]]; then
    echo "expected $method to fail" >&2
    echo "$out" >&2
    exit 1
  fi
  echo "$out"
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

fail_with_service_logs() {
  local message="$1"
  local response="$2"
  echo "$message" >&2
  if [[ -n "$response" ]]; then
    echo "$response" >&2
  fi
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" logs quota-service redis >&2 || true
  exit 1
}

wait_for_redis_ok() {
  local attempts="$1"
  local message="$2"
  local response=""
  for _ in $(seq 1 "$attempts"); do
    if response="$(grpc GetRedisStatus '{}' 2>/dev/null)" &&
      grep -Fq '"reachable": true' <<<"$response" &&
      grep -Fq '"message": "ok"' <<<"$response"; then
      return
    fi
    sleep 1
  done
  fail_with_service_logs "$message" "$response"
}

wait_for_redis_unreachable() {
  local attempts="$1"
  local message="$2"
  local response=""
  for _ in $(seq 1 "$attempts"); do
    if response="$(grpc GetRedisStatus '{}' 2>/dev/null)" &&
      ! grep -Fq '"reachable": true' <<<"$response" &&
      ! grep -Fq '"message": "ok"' <<<"$response"; then
      return
    fi
    sleep 1
  done
  fail_with_service_logs "$message" "$response"
}

wait_for_health_status() {
  local want="$1"
  local attempts="$2"
  local message="$3"
  local response=""
  for _ in $(seq 1 "$attempts"); do
    if response="$(grpc_health 2>/dev/null)" &&
      grep -Fq "\"status\": \"$want\"" <<<"$response"; then
      return
    fi
    sleep 1
  done
  fail_with_service_logs "$message" "$response"
}

json_string_field() {
  local field="$1"
  local json="$2"
  grep -m1 "\"$field\"" <<<"$json" | sed -E "s/.*\"$field\": \"([^\"]+)\".*/\1/"
}

require_equal() {
  local got="$1"
  local want="$2"
  local label="$3"
  if [[ "$got" != "$want" ]]; then
    echo "expected $label to be $want, got $got" >&2
    exit 1
  fi
}

postgres_query() {
  local sql="$1"
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" exec -T postgres \
    psql -U quota -d quota -tAc "$sql"
}

wait_for_postgres_event_count() {
  local want="$1"
  local got=""
  for _ in $(seq 1 60); do
    if got="$(postgres_query "SELECT count(*) FROM quota_usage_events;" 2>/dev/null)" &&
      [[ "$got" =~ ^[0-9]+$ ]] &&
      (( got >= want )); then
      return
    fi
    sleep 1
  done
  echo "expected at least $want persisted quota events, got ${got:-none}" >&2
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" logs quota-service postgres >&2 || true
  exit 1
}

require_postgres_scalar() {
  local sql="$1"
  local want="$2"
  local label="$3"
  local got
  got="$(postgres_query "$sql")"
  if [[ "$got" != "$want" ]]; then
    echo "expected $label to be $want, got $got" >&2
    exit 1
  fi
}

require_postgres_event_count() {
  local where="$1"
  local want="$2"
  local label="$3"
  require_postgres_scalar "SELECT count(*) FROM quota_usage_events WHERE $where;" "$want" "$label"
}

require_limit_status_field() {
  local json="$1"
  local path="$2"
  local limit_id="$3"
  local field="$4"
  local want="$5"
  local label="$6"
  local got
  if ! got="$(JSON_INPUT="$json" python3 - "$path" "$limit_id" "$field" <<'PY'
import json
import os
import sys

path, limit_id, field = sys.argv[1:]
data = json.loads(os.environ["JSON_INPUT"])
for part in path.split("."):
    data = data[part]
for status in data:
    if status.get("limitId") == limit_id:
        value = status.get(field)
        if value is None and field == "allowed":
            value = False
        if value is None and field in {"limit", "used", "remaining", "cost", "resetAtUnixMs", "retryAfterMs"}:
            value = "0"
        print(str(value).lower() if isinstance(value, bool) else value)
        sys.exit(0)
print(f"limit status {limit_id!r} not found at {path}", file=sys.stderr)
sys.exit(1)
PY
  )"; then
    echo "expected $label to include limit status $limit_id" >&2
    echo "$json" >&2
    exit 1
  fi
  if [[ "$got" != "$want" ]]; then
    echo "expected $label $limit_id.$field to be $want, got $got" >&2
    echo "$json" >&2
    exit 1
  fi
}

require_limit_status_field_gt_zero() {
  local json="$1"
  local path="$2"
  local limit_id="$3"
  local field="$4"
  local label="$5"
  if ! JSON_INPUT="$json" python3 - "$path" "$limit_id" "$field" <<'PY'
import json
import os
import sys

path, limit_id, field = sys.argv[1:]
data = json.loads(os.environ["JSON_INPUT"])
for part in path.split("."):
    data = data[part]
for status in data:
    if status.get("limitId") == limit_id:
        value = int(status.get(field, "0"))
        if value > 0:
            sys.exit(0)
        print(f"{limit_id}.{field} was {value}, want > 0", file=sys.stderr)
        sys.exit(1)
print(f"limit status {limit_id!r} not found at {path}", file=sys.stderr)
sys.exit(1)
PY
  then
    echo "expected $label $limit_id.$field to be greater than zero" >&2
    echo "$json" >&2
    exit 1
  fi
}

export QUOTA_E2E_GRPC_PORT="$GRPC_PORT"
export QUOTA_E2E_METRICS_PORT="$METRICS_PORT"

cleanup
docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d --build

wait_for_redis_ok 60 "quota service did not become ready"
wait_for_health_status SERVING 30 "gRPC health did not report SERVING"

docker compose -p "$PROJECT" -f "$COMPOSE_FILE" stop redis >/dev/null
wait_for_redis_unreachable 30 "quota service did not report Redis as unreachable after Redis stopped"
wait_for_health_status NOT_SERVING 30 "gRPC health did not report NOT_SERVING while Redis was stopped"

docker compose -p "$PROJECT" -f "$COMPOSE_FILE" start redis >/dev/null
wait_for_redis_ok 60 "quota service did not recover Redis connectivity after Redis restarted"
wait_for_health_status SERVING 30 "gRPC health did not report SERVING after Redis restarted"

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

multi_low_limit='{
  "limitId": "multi_low",
  "scopeKey": "user:user_123",
  "action": "workspace.multi.consume",
  "unit": "requests",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "60000"},
  "limit": "2"
}'

multi_high_limit='{
  "limitId": "multi_high",
  "scopeKey": "user:user_123",
  "action": "workspace.multi.consume",
  "unit": "requests",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "60000"},
  "limit": "5"
}'

short_window_limit='{
  "limitId": "short_window_refresh",
  "scopeKey": "user:user_123",
  "action": "workspace.short_window.consume",
  "unit": "requests",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "3000"},
  "limit": "2"
}'

e2e_token_bucket_limit='{
  "limitId": "e2e_token_bucket",
  "scopeKey": "user:user_123",
  "action": "workspace.token.consume",
  "unit": "tokens",
  "algorithm": "ALGORITHM_TOKEN_BUCKET",
  "window": {"type": "WINDOW_TYPE_CONTINUOUS", "durationMs": "60000"},
  "limit": "10",
  "burst": "10",
  "refillRatePerSec": 0.001
}'

e2e_leaky_bucket_limit='{
  "limitId": "e2e_leaky_bucket",
  "scopeKey": "user:user_123",
  "action": "workspace.leaky.consume",
  "unit": "tokens",
  "algorithm": "ALGORITHM_LEAKY_BUCKET",
  "window": {"type": "WINDOW_TYPE_CONTINUOUS", "durationMs": "60000"},
  "limit": "10",
  "burst": "10",
  "refillRatePerSec": 0.001
}'

e2e_gcra_limit='{
  "limitId": "e2e_gcra",
  "scopeKey": "user:user_123",
  "action": "workspace.gcra.consume",
  "unit": "requests",
  "algorithm": "ALGORITHM_GCRA",
  "window": {"type": "WINDOW_TYPE_CONTINUOUS", "durationMs": "60000"},
  "limit": "1",
  "burst": "1",
  "refillRatePerSec": 2
}'

dry_run_limit='{
  "limitId": "dry_run_limit",
  "scopeKey": "user:user_123",
  "action": "workspace.dryrun.consume",
  "unit": "requests",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "60000"},
  "limit": "5"
}'

reserve_dry_run_limit='{
  "limitId": "reserve_dry_run_limit",
  "scopeKey": "user:user_123",
  "action": "workspace.dryrun.reserve",
  "unit": "requests",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "60000"},
  "limit": "5",
  "refundable": true,
  "reservationExpiryPolicy": "RESERVATION_EXPIRY_POLICY_CHARGE_FULL"
}'

lease_dry_run_limit='{
  "limitId": "lease_dry_run_limit",
  "scopeKey": "user:user_123",
  "action": "workspace.dryrun.lease",
  "unit": "requests",
  "algorithm": "ALGORITHM_CONCURRENCY",
  "limit": "1"
}'

lease_expiry_limit='{
  "limitId": "lease_expiry_limit",
  "scopeKey": "user:user_123",
  "action": "workspace.expiring.lease",
  "unit": "requests",
  "algorithm": "ALGORITHM_CONCURRENCY",
  "limit": "1"
}'

event_consume_limit='{
  "limitId": "event_consume_limit",
  "scopeKey": "event:e2e",
  "action": "workspace.events.consume",
  "unit": "requests",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "60000"},
  "limit": "1"
}'

event_reserve_limit='{
  "limitId": "event_reserve_limit",
  "scopeKey": "event:e2e",
  "action": "workspace.events.reserve",
  "unit": "tokens",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "60000"},
  "limit": "10",
  "refundable": true,
  "reservationExpiryPolicy": "RESERVATION_EXPIRY_POLICY_CHARGE_FULL"
}'

event_lease_limit='{
  "limitId": "event_lease_limit",
  "scopeKey": "event:e2e",
  "action": "workspace.events.lease",
  "unit": "requests",
  "algorithm": "ALGORITHM_CONCURRENCY",
  "limit": "1"
}'

reservation_expiry_limit='{
  "limitId": "reservation_expiry_limit",
  "scopeKey": "user:user_123",
  "action": "workspace.expiring.reserve",
  "unit": "requests",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "60000"},
  "limit": "10",
  "reservationExpiryPolicy": "RESERVATION_EXPIRY_POLICY_REFUND_FULL"
}'

overage_limit='{
  "limitId": "overage_limit",
  "scopeKey": "user:user_123",
  "action": "workspace.overage.reserve",
  "unit": "requests",
  "algorithm": "ALGORITHM_FIXED_WINDOW_DURATION",
  "window": {"type": "WINDOW_TYPE_DURATION", "durationMs": "60000"},
  "limit": "20",
  "refundable": true,
  "reservationExpiryPolicy": "RESERVATION_EXPIRY_POLICY_CHARGE_FULL"
}'

sliding_warning_limit='{
  "limitId": "sliding_warning",
  "scopeKey": "user:user_123",
  "action": "workspace.validation.sliding",
  "unit": "requests",
  "algorithm": "ALGORITHM_SLIDING_WINDOW",
  "window": {"type": "WINDOW_TYPE_SLIDING", "durationMs": "60000"},
  "limit": "10"
}'

invalid_sliding_limit='{
  "limitId": "invalid_sliding",
  "scopeKey": "user:user_123",
  "action": "workspace.validation.invalid",
  "unit": "requests",
  "algorithm": "ALGORITHM_SLIDING_WINDOW",
  "window": {"type": "WINDOW_TYPE_SLIDING", "durationMs": "60000", "bucketCount": 1},
  "limit": "10"
}'

validate_json="$(grpc ValidateLimits "{
  \"limits\": [$email_limit, $token_limit, $concurrency_limit]
}")"
require_contains "$validate_json" '"valid": true' "ValidateLimits response"

warning_validate_json="$(grpc ValidateLimits "{
  \"limits\": [$sliding_warning_limit]
}")"
require_contains "$warning_validate_json" '"valid": true' "ValidateLimits sliding warning response"
require_contains "$warning_validate_json" '"field": "window.bucket_count"' "ValidateLimits sliding warning response"

invalid_validate_json="$(grpc ValidateLimits "{
  \"limits\": [$invalid_sliding_limit]
}")"
require_contains "$invalid_validate_json" '"field": "window.bucket_count"' "ValidateLimits invalid sliding response"
require_not_contains "$invalid_validate_json" '"valid": true' "ValidateLimits invalid sliding response"

invalid_consume_error="$(grpc_expect_error Consume "{
  \"action\": \"workspace.email.recipients\",
  \"cost\": \"1\",
  \"limits\": [$email_limit]
}")"
require_contains "$invalid_consume_error" "request_id is required" "Consume missing request_id error"

invalid_explain_json="$(grpc Explain "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.email.recipients\",
  \"cost\": \"0\",
  \"limits\": [$email_limit]
}")"
require_contains "$invalid_explain_json" '"reason": "DECISION_REASON_INVALID_REQUEST"' "Explain invalid cost response"
require_contains "$invalid_explain_json" 'cost must be greater than zero' "Explain invalid cost response"

missing_reservation_error="$(grpc_expect_error GetReservation "{\"reservationId\": \"missing-reservation\"}")"
require_contains "$missing_reservation_error" "reservation not found" "GetReservation missing error"

missing_lease_error="$(grpc_expect_error GetLease "{\"leaseId\": \"missing-lease\"}")"
require_contains "$missing_lease_error" "lease not found" "GetLease missing error"

negative_finalize_error="$(grpc_expect_error FinalizeReservation "{
  \"requestId\": \"req-e2e-negative-finalize\",
  \"reservationId\": \"missing-reservation\",
  \"actualCost\": \"-1\"
}")"
require_contains "$negative_finalize_error" "actual_cost cannot be negative" "FinalizeReservation negative actual cost error"

zero_increment_json="$(grpc IncrementReservation "{
  \"requestId\": \"req-e2e-zero-increment\",
  \"reservationId\": \"missing-reservation\",
  \"deltaCost\": \"0\"
}")"
require_contains "$zero_increment_json" '"reason": "DECISION_REASON_INVALID_REQUEST"' "IncrementReservation zero delta response"
require_contains "$zero_increment_json" 'delta_cost must be non-zero' "IncrementReservation zero delta response"

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

consume_concurrency_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-consume-concurrency-limit\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"assistant.llm.concurrent\",
  \"cost\": \"1\",
  \"limits\": [$concurrency_limit]
}")"
require_contains "$consume_concurrency_json" '"reason": "DECISION_REASON_INVALID_REQUEST"' "Consume with concurrency limit response"
require_contains "$consume_concurrency_json" 'Consume/Reserve do not accept ALGORITHM_CONCURRENCY limits' "Consume with concurrency limit response"

lease_fixed_window_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-lease-fixed-window-limit\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.email.recipients\",
  \"limits\": [$email_limit],
  \"leaseTtlMs\": \"1000\"
}")"
require_contains "$lease_fixed_window_json" '"reason": "DECISION_REASON_INVALID_REQUEST"' "AcquireLease with fixed-window limit response"
require_contains "$lease_fixed_window_json" 'AcquireLease only accepts ALGORITHM_CONCURRENCY limits' "AcquireLease with fixed-window limit response"

dry_run_consume_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-dry-run-consume\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.consume\",
  \"cost\": \"4\",
  \"limits\": [$dry_run_limit],
  \"options\": {\"dryRun\": true}
}")"
require_contains "$dry_run_consume_json" '"reason": "DECISION_REASON_DRY_RUN"' "Consume dry-run response"
require_limit_status_field "$dry_run_consume_json" "decision.limitStatuses" "dry_run_limit" "used" "0" "Consume dry-run response"

dry_run_usage_json="$(grpc GetCurrentUsage "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.consume\",
  \"limits\": [$dry_run_limit]
}")"
require_limit_status_field "$dry_run_usage_json" "limitStatuses" "dry_run_limit" "used" "0" "dry-run usage response"

real_after_dry_run_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-real-after-dry-run\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.consume\",
  \"cost\": \"5\",
  \"limits\": [$dry_run_limit]
}")"
require_contains "$real_after_dry_run_json" '"allowed": true' "real consume after dry-run response"
require_limit_status_field "$real_after_dry_run_json" "decision.limitStatuses" "dry_run_limit" "used" "5" "real consume after dry-run response"

multi_first_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-multi-first\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.multi.consume\",
  \"cost\": \"2\",
  \"limits\": [$multi_low_limit, $multi_high_limit]
}")"
require_contains "$multi_first_json" '"allowed": true' "multi-limit first consume response"
require_limit_status_field "$multi_first_json" "decision.limitStatuses" "multi_low" "used" "2" "multi-limit first consume response"
require_limit_status_field "$multi_first_json" "decision.limitStatuses" "multi_high" "remaining" "3" "multi-limit first consume response"

multi_denied_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-multi-denied\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.multi.consume\",
  \"cost\": \"1\",
  \"limits\": [$multi_low_limit, $multi_high_limit]
}")"
require_contains "$multi_denied_json" '"reason": "DECISION_REASON_LIMIT_EXCEEDED"' "multi-limit denial response"
require_limit_status_field "$multi_denied_json" "decision.limitStatuses" "multi_low" "allowed" "false" "multi-limit denial response"

multi_usage_json="$(grpc GetCurrentUsage "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.multi.consume\",
  \"limits\": [$multi_low_limit, $multi_high_limit]
}")"
require_limit_status_field "$multi_usage_json" "limitStatuses" "multi_low" "used" "2" "multi-limit usage response"
require_limit_status_field "$multi_usage_json" "limitStatuses" "multi_low" "remaining" "0" "multi-limit usage response"
require_limit_status_field "$multi_usage_json" "limitStatuses" "multi_high" "used" "2" "multi-limit usage response"
require_limit_status_field "$multi_usage_json" "limitStatuses" "multi_high" "remaining" "3" "multi-limit usage response"

short_window_first_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-short-window-first\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.short_window.consume\",
  \"cost\": \"2\",
  \"limits\": [$short_window_limit]
}")"
require_contains "$short_window_first_json" '"allowed": true' "short fixed-window first consume response"
require_limit_status_field "$short_window_first_json" "decision.limitStatuses" "short_window_refresh" "remaining" "0" "short fixed-window first consume response"

sleep 4

short_window_refreshed_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-short-window-refreshed\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.short_window.consume\",
  \"cost\": \"1\",
  \"limits\": [$short_window_limit]
}")"
require_contains "$short_window_refreshed_json" '"allowed": true' "short fixed-window refreshed consume response"
require_limit_status_field "$short_window_refreshed_json" "decision.limitStatuses" "short_window_refresh" "used" "1" "short fixed-window refreshed consume response"

token_bucket_first_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-token-bucket-first\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.token.consume\",
  \"cost\": \"6\",
  \"limits\": [$e2e_token_bucket_limit]
}")"
require_contains "$token_bucket_first_json" '"allowed": true' "token bucket first consume response"
require_limit_status_field "$token_bucket_first_json" "decision.limitStatuses" "e2e_token_bucket" "remaining" "4" "token bucket first consume response"

token_bucket_denied_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-token-bucket-denied\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.token.consume\",
  \"cost\": \"5\",
  \"limits\": [$e2e_token_bucket_limit]
}")"
require_not_contains "$token_bucket_denied_json" '"allowed": true' "token bucket denial response"
require_limit_status_field "$token_bucket_denied_json" "decision.limitStatuses" "e2e_token_bucket" "remaining" "4" "token bucket denial response"

token_bucket_remaining_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-token-bucket-remaining\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.token.consume\",
  \"cost\": \"4\",
  \"limits\": [$e2e_token_bucket_limit]
}")"
require_contains "$token_bucket_remaining_json" '"allowed": true' "token bucket remaining consume response"
require_limit_status_field "$token_bucket_remaining_json" "decision.limitStatuses" "e2e_token_bucket" "remaining" "0" "token bucket remaining consume response"

leaky_bucket_first_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-leaky-bucket-first\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.leaky.consume\",
  \"cost\": \"6\",
  \"limits\": [$e2e_leaky_bucket_limit]
}")"
require_contains "$leaky_bucket_first_json" '"allowed": true' "leaky bucket first consume response"
require_limit_status_field "$leaky_bucket_first_json" "decision.limitStatuses" "e2e_leaky_bucket" "remaining" "4" "leaky bucket first consume response"

leaky_bucket_denied_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-leaky-bucket-denied\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.leaky.consume\",
  \"cost\": \"5\",
  \"limits\": [$e2e_leaky_bucket_limit]
}")"
require_not_contains "$leaky_bucket_denied_json" '"allowed": true' "leaky bucket denial response"
require_contains "$leaky_bucket_denied_json" '"reason": "DECISION_REASON_LIMIT_EXCEEDED"' "leaky bucket denial response"
require_limit_status_field "$leaky_bucket_denied_json" "decision.limitStatuses" "e2e_leaky_bucket" "remaining" "4" "leaky bucket denial response"

leaky_bucket_remaining_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-leaky-bucket-remaining\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.leaky.consume\",
  \"cost\": \"4\",
  \"limits\": [$e2e_leaky_bucket_limit]
}")"
require_contains "$leaky_bucket_remaining_json" '"allowed": true' "leaky bucket remaining consume response"
require_limit_status_field "$leaky_bucket_remaining_json" "decision.limitStatuses" "e2e_leaky_bucket" "remaining" "0" "leaky bucket remaining consume response"

gcra_first_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-gcra-first\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.gcra.consume\",
  \"cost\": \"2\",
  \"limits\": [$e2e_gcra_limit]
}")"
require_contains "$gcra_first_json" '"allowed": true' "GCRA first consume response"
require_limit_status_field "$gcra_first_json" "decision.limitStatuses" "e2e_gcra" "remaining" "1" "GCRA first consume response"

gcra_denied_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-gcra-denied\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.gcra.consume\",
  \"cost\": \"2\",
  \"limits\": [$e2e_gcra_limit]
}")"
require_not_contains "$gcra_denied_json" '"allowed": true' "GCRA denial response"
require_contains "$gcra_denied_json" '"reason": "DECISION_REASON_LIMIT_EXCEEDED"' "GCRA denial response"
require_limit_status_field "$gcra_denied_json" "decision.limitStatuses" "e2e_gcra" "allowed" "false" "GCRA denial response"
require_limit_status_field_gt_zero "$gcra_denied_json" "decision.limitStatuses" "e2e_gcra" "retryAfterMs" "GCRA denial response"

sleep 1

gcra_refilled_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-gcra-refilled\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.gcra.consume\",
  \"cost\": \"1\",
  \"limits\": [$e2e_gcra_limit]
}")"
require_contains "$gcra_refilled_json" '"allowed": true' "GCRA refilled consume response"

reserve_dry_run_json="$(grpc Reserve "{
  \"requestId\": \"req-e2e-reserve-dry-run\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.reserve\",
  \"reserveCost\": \"4\",
  \"reservationTtlMs\": \"60000\",
  \"limits\": [$reserve_dry_run_limit],
  \"options\": {\"dryRun\": true}
}")"
require_contains "$reserve_dry_run_json" '"reason": "DECISION_REASON_DRY_RUN"' "Reserve dry-run response"
require_not_contains "$reserve_dry_run_json" '"reservationId"' "Reserve dry-run response"

reserve_dry_run_usage_json="$(grpc GetCurrentUsage "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.reserve\",
  \"limits\": [$reserve_dry_run_limit]
}")"
require_limit_status_field "$reserve_dry_run_usage_json" "limitStatuses" "reserve_dry_run_limit" "used" "0" "Reserve dry-run usage response"

reserve_after_dry_run_json="$(grpc Reserve "{
  \"requestId\": \"req-e2e-reserve-after-dry-run\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.reserve\",
  \"reserveCost\": \"4\",
  \"reservationTtlMs\": \"60000\",
  \"limits\": [$reserve_dry_run_limit]
}")"
require_contains "$reserve_after_dry_run_json" '"allowed": true' "Reserve after dry-run response"
require_contains "$reserve_after_dry_run_json" '"reservationId"' "Reserve after dry-run response"

expiring_reserve_json="$(grpc Reserve "{
  \"requestId\": \"req-e2e-expiring-reservation\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.expiring.reserve\",
  \"reserveCost\": \"4\",
  \"reservationTtlMs\": \"100\",
  \"limits\": [$reservation_expiry_limit]
}")"
require_contains "$expiring_reserve_json" '"allowed": true' "expiring reservation response"
expiring_reservation_id="$(json_string_field reservationId "$expiring_reserve_json")"
if [[ -z "$expiring_reservation_id" ]]; then
  echo "Expiring Reserve response did not include reservationId" >&2
  echo "$expiring_reserve_json" >&2
  exit 1
fi

expiring_usage_before_json="$(grpc GetCurrentUsage "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.expiring.reserve\",
  \"limits\": [$reservation_expiry_limit]
}")"
require_limit_status_field "$expiring_usage_before_json" "limitStatuses" "reservation_expiry_limit" "used" "4" "expiring reservation usage before sweep"

sleep 2

expired_reservation_json="$(grpc GetReservation "{\"reservationId\": \"$expiring_reservation_id\"}")"
require_contains "$expired_reservation_json" '"status": "RESERVATION_STATUS_EXPIRED"' "expired reservation response"
require_contains "$expired_reservation_json" '"refundedCost": "4"' "expired reservation response"

expiring_usage_after_json="$(grpc GetCurrentUsage "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.expiring.reserve\",
  \"limits\": [$reservation_expiry_limit]
}")"
require_limit_status_field "$expiring_usage_after_json" "limitStatuses" "reservation_expiry_limit" "used" "0" "expiring reservation usage after sweep"

overage_reserve_json="$(grpc Reserve "{
  \"requestId\": \"req-e2e-overage-reserve\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.overage.reserve\",
  \"reserveCost\": \"8\",
  \"reservationTtlMs\": \"60000\",
  \"limits\": [$overage_limit]
}")"
require_contains "$overage_reserve_json" '"allowed": true' "overage reserve response"
overage_reservation_id="$(json_string_field reservationId "$overage_reserve_json")"
if [[ -z "$overage_reservation_id" ]]; then
  echo "Overage Reserve response did not include reservationId" >&2
  echo "$overage_reserve_json" >&2
  exit 1
fi

overage_finalize_json="$(grpc FinalizeReservation "{
  \"requestId\": \"req-e2e-overage-finalize\",
  \"reservationId\": \"$overage_reservation_id\",
  \"actualCost\": \"13\",
  \"status\": \"FINALIZE_STATUS_SUCCEEDED\"
}")"
require_contains "$overage_finalize_json" '"reservedCost": "8"' "overage finalize response"
require_contains "$overage_finalize_json" '"actualCost": "13"' "overage finalize response"
require_contains "$overage_finalize_json" '"overageCost": "5"' "overage finalize response"

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

reserve_replay_json="$(grpc Reserve "{
  \"requestId\": \"req-e2e-reserve\",
  \"context\": {\"product\": \"assistant\", \"environment\": \"test\", \"metadata\": {\"model\": \"ignored-on-replay\"}},
  \"action\": \"assistant.llm.tokens\",
  \"reserveCost\": \"99\",
  \"reservationTtlMs\": \"60000\",
  \"limits\": [$token_limit]
}")"
require_contains "$reserve_replay_json" '"idempotency_hit": "true"' "Reserve replay response"
reserve_replay_id="$(json_string_field reservationId "$reserve_replay_json")"
require_equal "$reserve_replay_id" "$reservation_id" "Reserve replay reservationId"

get_reservation_json="$(grpc GetReservation "{\"reservationId\": \"$reservation_id\"}")"
require_contains "$get_reservation_json" "\"reservationId\": \"$reservation_id\"" "GetReservation active response"
require_contains "$get_reservation_json" '"status": "RESERVATION_STATUS_ACTIVE"' "GetReservation active response"
require_contains "$get_reservation_json" '"redisKey": "' "GetReservation active response"

increment_grow_json="$(grpc IncrementReservation "{
  \"requestId\": \"req-e2e-increment-grow\",
  \"reservationId\": \"$reservation_id\",
  \"deltaCost\": \"30\"
}")"
require_contains "$increment_grow_json" '"allowed": true' "IncrementReservation grow response"
require_contains "$increment_grow_json" '"reservedCost": "70"' "IncrementReservation grow response"

increment_replay_json="$(grpc IncrementReservation "{
  \"requestId\": \"req-e2e-increment-grow\",
  \"reservationId\": \"$reservation_id\",
  \"deltaCost\": \"999\"
}")"
require_contains "$increment_replay_json" '"idempotency_hit": "true"' "IncrementReservation replay response"
require_contains "$increment_replay_json" '"reservedCost": "70"' "IncrementReservation replay response"

increment_deny_json="$(grpc IncrementReservation "{
  \"requestId\": \"req-e2e-increment-denied\",
  \"reservationId\": \"$reservation_id\",
  \"deltaCost\": \"40\"
}")"
require_not_contains "$increment_deny_json" '"allowed": true' "IncrementReservation denial response"
require_contains "$increment_deny_json" '"reason": "DECISION_REASON_LIMIT_EXCEEDED"' "IncrementReservation denial response"
require_contains "$increment_deny_json" '"reservedCost": "70"' "IncrementReservation denial response"

increment_shrink_json="$(grpc IncrementReservation "{
  \"requestId\": \"req-e2e-increment-shrink\",
  \"reservationId\": \"$reservation_id\",
  \"deltaCost\": \"-30\"
}")"
require_contains "$increment_shrink_json" '"allowed": true' "IncrementReservation shrink response"
require_contains "$increment_shrink_json" '"reservedCost": "40"' "IncrementReservation shrink response"

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

finalize_replay_json="$(grpc FinalizeReservation "{
  \"requestId\": \"req-e2e-finalize\",
  \"reservationId\": \"$reservation_id\",
  \"actualCost\": \"99\",
  \"status\": \"FINALIZE_STATUS_SUCCEEDED\"
}")"
require_contains "$finalize_replay_json" '"reservedCost": "40"' "FinalizeReservation replay response"
require_contains "$finalize_replay_json" '"actualCost": "25"' "FinalizeReservation replay response"
require_contains "$finalize_replay_json" '"refundedCost": "15"' "FinalizeReservation replay response"

finalized_reservation_json="$(grpc GetReservation "{\"reservationId\": \"$reservation_id\"}")"
require_contains "$finalized_reservation_json" '"status": "RESERVATION_STATUS_FINALIZED"' "GetReservation finalized response"
require_contains "$finalized_reservation_json" '"actualCost": "25"' "GetReservation finalized response"

increment_finalized_error="$(grpc_expect_error IncrementReservation "{
  \"requestId\": \"req-e2e-increment-finalized\",
  \"reservationId\": \"$reservation_id\",
  \"deltaCost\": \"1\"
}")"
require_contains "$increment_finalized_error" "reservation is not active" "IncrementReservation finalized reservation error"

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

release_reservation_replay_json="$(grpc ReleaseReservation "{
  \"requestId\": \"req-e2e-release-reservation\",
  \"reservationId\": \"$release_reservation_id\",
  \"reason\": \"ignored replay reason\"
}")"
require_contains "$release_reservation_replay_json" '"releasedCost": "10"' "ReleaseReservation replay response"
require_contains "$release_reservation_replay_json" '"released": true' "ReleaseReservation replay response"

released_reservation_json="$(grpc GetReservation "{\"reservationId\": \"$release_reservation_id\"}")"
require_contains "$released_reservation_json" '"status": "RESERVATION_STATUS_RELEASED"' "GetReservation released response"

lease_dry_run_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-lease-dry-run\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.lease\",
  \"limits\": [$lease_dry_run_limit],
  \"leaseTtlMs\": \"60000\",
  \"options\": {\"dryRun\": true}
}")"
require_contains "$lease_dry_run_json" '"reason": "DECISION_REASON_DRY_RUN"' "AcquireLease dry-run response"
require_not_contains "$lease_dry_run_json" '"leaseId"' "AcquireLease dry-run response"

lease_dry_run_usage_json="$(grpc GetCurrentUsage "{
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.lease\",
  \"limits\": [$lease_dry_run_limit]
}")"
require_limit_status_field "$lease_dry_run_usage_json" "limitStatuses" "lease_dry_run_limit" "used" "0" "AcquireLease dry-run usage response"

lease_after_dry_run_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-lease-after-dry-run\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.dryrun.lease\",
  \"limits\": [$lease_dry_run_limit],
  \"leaseTtlMs\": \"60000\"
}")"
require_contains "$lease_after_dry_run_json" '"allowed": true' "AcquireLease after dry-run response"

expiring_lease_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-expiring-lease\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.expiring.lease\",
  \"limits\": [$lease_expiry_limit],
  \"leaseTtlMs\": \"100\"
}")"
require_contains "$expiring_lease_json" '"allowed": true' "expiring lease response"

expiring_lease_denied_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-expiring-lease-denied\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.expiring.lease\",
  \"limits\": [$lease_expiry_limit],
  \"leaseTtlMs\": \"60000\"
}")"
require_contains "$expiring_lease_denied_json" '"reason": "DECISION_REASON_CONCURRENCY_EXCEEDED"' "expiring lease denial response"

sleep 1

expiring_lease_replacement_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-expiring-lease-replacement\",
  \"context\": {\"product\": \"workspace\", \"environment\": \"test\"},
  \"action\": \"workspace.expiring.lease\",
  \"limits\": [$lease_expiry_limit],
  \"leaseTtlMs\": \"60000\"
}")"
require_contains "$expiring_lease_replacement_json" '"allowed": true' "expiring lease replacement response"

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

lease_replay_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-acquire-lease\",
  \"context\": {\"product\": \"assistant\", \"environment\": \"test\"},
  \"action\": \"assistant.llm.concurrent\",
  \"limits\": [$concurrency_limit],
  \"leaseTtlMs\": \"60000\"
}")"
require_contains "$lease_replay_json" '"idempotency_hit": "true"' "AcquireLease replay response"
lease_replay_id="$(json_string_field leaseId "$lease_replay_json")"
require_equal "$lease_replay_id" "$lease_id" "AcquireLease replay leaseId"

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

renew_replay_json="$(grpc RenewLease "{
  \"requestId\": \"req-e2e-renew-lease\",
  \"leaseId\": \"$lease_id\",
  \"extendTtlMs\": \"1\"
}")"
require_contains "$renew_replay_json" '"renewed": true' "RenewLease replay response"
require_contains "$renew_replay_json" "\"leaseId\": \"$lease_id\"" "RenewLease replay response"

release_lease_json="$(grpc ReleaseLease "{
  \"requestId\": \"req-e2e-release-lease\",
  \"leaseId\": \"$lease_id\"
}")"
require_contains "$release_lease_json" "\"leaseId\": \"$lease_id\"" "ReleaseLease response"
require_contains "$release_lease_json" '"released": true' "ReleaseLease response"

release_lease_replay_json="$(grpc ReleaseLease "{
  \"requestId\": \"req-e2e-release-lease\",
  \"leaseId\": \"$lease_id\"
}")"
require_contains "$release_lease_replay_json" "\"leaseId\": \"$lease_id\"" "ReleaseLease replay response"
require_contains "$release_lease_replay_json" '"released": true' "ReleaseLease replay response"

released_lease_json="$(grpc GetLease "{\"leaseId\": \"$lease_id\"}")"
require_contains "$released_lease_json" '"status": "LEASE_STATUS_RELEASED"' "GetLease released response"

renew_released_error="$(grpc_expect_error RenewLease "{
  \"requestId\": \"req-e2e-renew-released-lease\",
  \"leaseId\": \"$lease_id\",
  \"extendTtlMs\": \"1000\"
}")"
require_contains "$renew_released_error" "lease is not active" "RenewLease released lease error"

event_consume_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-event-consume-allowed\",
  \"context\": {
    \"product\": \"workspace\",
    \"environment\": \"test\",
    \"metadata\": {\"scenario\": \"postgres-e2e\"}
  },
  \"action\": \"workspace.events.consume\",
  \"cost\": \"1\",
  \"limits\": [$event_consume_limit],
  \"options\": {\"emitEvent\": true}
}")"
require_contains "$event_consume_json" '"allowed": true' "event consume allow response"

event_denied_json="$(grpc Consume "{
  \"requestId\": \"req-e2e-event-consume-denied\",
  \"context\": {
    \"product\": \"workspace\",
    \"environment\": \"test\",
    \"metadata\": {\"scenario\": \"postgres-e2e\"}
  },
  \"action\": \"workspace.events.consume\",
  \"cost\": \"1\",
  \"limits\": [$event_consume_limit],
  \"options\": {\"emitEvent\": true}
}")"
require_contains "$event_denied_json" '"reason": "DECISION_REASON_LIMIT_EXCEEDED"' "event consume denial response"

event_reserve_json="$(grpc Reserve "{
  \"requestId\": \"req-e2e-event-reserve\",
  \"context\": {
    \"product\": \"workspace\",
    \"environment\": \"test\",
    \"metadata\": {\"scenario\": \"postgres-e2e\"}
  },
  \"action\": \"workspace.events.reserve\",
  \"reserveCost\": \"2\",
  \"reservationTtlMs\": \"60000\",
  \"limits\": [$event_reserve_limit],
  \"options\": {\"emitEvent\": true}
}")"
require_contains "$event_reserve_json" '"allowed": true' "event reserve response"

event_lease_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-event-lease\",
  \"context\": {
    \"product\": \"workspace\",
    \"environment\": \"test\",
    \"metadata\": {\"scenario\": \"postgres-e2e\"}
  },
  \"action\": \"workspace.events.lease\",
  \"limits\": [$event_lease_limit],
  \"leaseTtlMs\": \"60000\",
  \"options\": {\"emitEvent\": true}
}")"
require_contains "$event_lease_json" '"allowed": true' "event lease response"

event_lease_denied_json="$(grpc AcquireLease "{
  \"requestId\": \"req-e2e-event-lease-denied\",
  \"context\": {
    \"product\": \"workspace\",
    \"environment\": \"test\",
    \"metadata\": {\"scenario\": \"postgres-e2e\"}
  },
  \"action\": \"workspace.events.lease\",
  \"limits\": [$event_lease_limit],
  \"leaseTtlMs\": \"60000\",
  \"options\": {\"emitEvent\": true}
}")"
require_contains "$event_lease_denied_json" '"reason": "DECISION_REASON_CONCURRENCY_EXCEEDED"' "event lease denial response"

wait_for_postgres_event_count 5
require_postgres_scalar "SELECT count(*) FROM quota_usage_events;" "5" "persisted quota event count"
require_postgres_event_count "event_type = 'quota.consumed'" "1" "quota.consumed event count"
require_postgres_event_count "event_type = 'quota.reserved'" "1" "quota.reserved event count"
require_postgres_event_count "event_type = 'quota.lease_acquired'" "1" "quota.lease_acquired event count"
require_postgres_event_count "event_type = 'quota.denied'" "2" "quota.denied event count"
require_postgres_event_count "
  request_id = 'req-e2e-event-consume-allowed'
  AND product = 'workspace'
  AND environment = 'test'
  AND action = 'workspace.events.consume'
  AND payload->>'allowed' = 'true'
  AND payload->>'cost' = '1'
  AND payload->'metadata'->>'scenario' = 'postgres-e2e'
" "1" "persisted consume payload"
require_postgres_event_count "
  request_id = 'req-e2e-event-consume-denied'
  AND event_type = 'quota.denied'
  AND payload->>'allowed' = 'false'
" "1" "persisted consume denial payload"
require_postgres_event_count "
  request_id = 'req-e2e-event-reserve'
  AND event_type = 'quota.reserved'
  AND payload ? 'reservation'
" "1" "persisted reserve payload"
require_postgres_event_count "
  request_id = 'req-e2e-event-lease'
  AND event_type = 'quota.lease_acquired'
  AND payload ? 'lease'
" "1" "persisted lease payload"
require_postgres_event_count "
  request_id = 'req-e2e-event-lease-denied'
  AND event_type = 'quota.denied'
  AND payload->>'allowed' = 'false'
" "1" "persisted lease denial payload"

metrics="$(curl -fsS "http://localhost:$METRICS_PORT/metrics")"
require_contains "$metrics" 'quota_requests_total' "metrics output"
require_contains "$metrics" 'quota_denials_total' "metrics output"
require_contains "$metrics" 'quota_reservations_active' "metrics output"
require_contains "$metrics" 'quota_leases_active' "metrics output"

echo "docker compose critical RPC e2e passed"
