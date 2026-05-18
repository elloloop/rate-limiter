# Operations

## Health

The service implements standard gRPC health checking:

```text
grpc.health.v1.Health
```

Readiness verifies:

- Redis is reachable.
- Required Lua scripts are loaded.
- The configured event sink initializes.

The gRPC health status is refreshed continuously while the service is running.
If Redis becomes unreachable or required scripts cannot be loaded, the health
service reports `NOT_SERVING`. If Redis drops cached Lua scripts during a
restart, failover, or `SCRIPT FLUSH`, the service reloads them automatically.

## Reflection

The service enables gRPC reflection:

```text
grpc.reflection.v1alpha.ServerReflection
```

## Metrics

Prometheus metrics are exposed on `QUOTA_METRICS_BIND_ADDR`.

Required metrics:

```text
quota_requests_total{rpc,action,product,allowed,reason}
quota_request_duration_ms{rpc,action,product}
quota_denials_total{action,product,limit_id}
quota_redis_errors_total
quota_idempotency_hits_total
quota_reservations_active
quota_reservations_expired_total
quota_leases_active
quota_overages_total
quota_event_emit_errors_total
```

## Events

Event emission is append-only and factual. It is not a reporting API.

V1 sinks:

- `none`
- `stdout`
- `postgres`

Event emission must not block the hot path in default mode.
Postgres event writes run with their own short timeout instead of inheriting the
caller RPC context, so a completed request does not cancel best-effort event
delivery.

## Reservation Expiry

The service keeps an internal Redis sorted-set index of reservation expiration
times. A background sweeper runs in-process and expires due reservations in
batches. `RESERVATION_EXPIRY_POLICY_REFUND_FULL` refunds the stored reserved
cost even if the impact is not otherwise refundable; `CHARGE_FULL` leaves the
counter debit in place. The sweeper uses stored `ReservationImpact.redis_key`
values and does not recompute current windows.
