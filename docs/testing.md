# Testing

This repository has a production-grade harness for the gRPC rate-limiter
service, Redis backend, Postgres event sink, background reservation sweeper,
observability, Docker image, and generated protobuf contract.

Every future agent change must add or update all applicable tests. Do not ship
behavior without the matching unit, integration, e2e, regression, benchmark, or
fuzz coverage.

## Commands

```bash
make test-fast        # go test ./...
make test-race        # go test -race ./...
make test             # alias for test-race
make test-integration # Redis/Postgres required; runs with -tags=integration
make test-cover       # 100% aggregate and per-package coverage gates
make test-e2e         # Docker Compose critical public-RPC e2e
make test-bench       # go test -bench=. -benchmem ./...
make test-fuzz        # fuzz smoke for every Fuzz* target
make vulncheck        # govulncheck
make verify           # standard pre-merge local checks
make verify-ci        # proto + docs + CI gates + e2e + coverage
```

`make ci-full` remains the direct local mirror for the CI jobs that include
Docker Compose e2e. `make verify-ci` adds protobuf, docs, and coverage gates so
local confidence matches the release bar.

## Local Services

Redis-backed tests and Postgres event-sink tests must use real local services:

```bash
make redis-up
export QUOTA_TEST_REDIS_URL=redis://localhost:6379/0

make postgres-up
export QUOTA_TEST_POSTGRES_URL=postgres://quota:quota@localhost:15432/quota?sslmode=disable
```

The images are pinned in the Makefile. Stop them with:

```bash
make redis-down
make postgres-down
```

Normal tests must not call real third-party services. Use package-local fakes,
in-memory sinks, local Redis/Postgres containers, or `httptest.Server` if an
external HTTP client is added.

## Test Layers

### Unit and Table-Driven Tests

```bash
make test-fast
```

Use for pure logic, validation, config loading, Redis key derivation, request
mapping, metrics helpers, and command helpers. Keep cases table-driven when the
behavior has multiple inputs or edge cases.

### Race and Concurrency Tests

```bash
make test-race
```

Use for service methods, background sweeper behavior, event sinks, Redis Lua
atomicity, idempotency, cancellation, and any code that shares state across
goroutines.

### Redis and Postgres Integration Tests

```bash
make test-integration
```

The target requires both service URLs so important integration coverage cannot
silently skip. Redis tests cover fixed windows, duration windows, sliding
windows, token buckets, leaky buckets, GCRA, reservations, increments,
finalization, release, expiry-policy refunds, lease acquire/renew/release,
current usage, explain behavior, script reload after `SCRIPT FLUSH`, and Redis
health helpers.

Postgres tests cover event schema setup, event persistence, payload shape,
error paths, and log behavior. If migrations or repository-style queries are
added, add tests for create/read/update/delete behavior, filtering, sorting,
pagination, unique constraints, tenant scoping, null handling, transaction
rollback, and migration correctness.

### Docker Compose E2E

```bash
make test-e2e
```

The e2e script builds the service image, starts Redis, Postgres, and the
service, then exercises the public gRPC surface through `grpcurl`: health,
`GetRedisStatus`, `ValidateLimits`, `Consume`, `GetCurrentUsage`, `Explain`,
`Reserve`, `GetReservation`, `IncrementReservation`, `FinalizeReservation`,
`ReleaseReservation`, `AcquireLease`, `GetLease`, `RenewLease`, and
`ReleaseLease`. It also verifies denial/idempotency paths, Prometheus metrics,
Postgres event rows, Redis outage detection, and Redis recovery.

### Benchmarks

```bash
make test-bench
```

Benchmark hot paths or performance-sensitive changes. The current examples
cover key hashing, prefix generation, and sliding-bucket key derivation.

### Fuzz and Property Tests

```bash
make test-fuzz
```

Fuzz targets exist for Redis key hashing and namespace prefixes. Add fuzz or
property-style tests for parsers, encoders, schema mappers, key builders, and
other input-heavy code.

## Policy for Future Changes

At minimum:

1. Pure logic changes require unit, table-driven, and edge-case tests.
2. Public gRPC API changes require service, validation, response, and
   error-path tests.
3. Redis backend changes require real Redis integration tests and race tests.
4. Postgres event changes require schema/persistence/error-path integration
   tests.
5. Migration or query changes require migration, rollback, constraint, and
   fixture-isolation tests.
6. Tenant/privacy-sensitive changes require cross-tenant negative tests and
   data-leakage assertions.
7. External client changes require fake-client or `httptest.Server` tests for
   success, error, timeout, retry, malformed response, and no-network behavior.
8. Background job changes require synchronous processing, retry, idempotency,
   duplicate, poison-message, cancellation, and graceful-shutdown tests.
9. Event publishing changes require capture helpers and payload/version
   assertions.
10. Bug fixes require regression tests that fail before the fix and pass after.
11. Concurrent code requires race, cancellation, and goroutine-leak-oriented
   tests where feasible.
12. Public protobuf contract changes require schema/contract freshness checks
   and backward-compatible response tests where applicable.
13. Performance-sensitive changes require benchmarks or load-test hooks.
14. Security-sensitive changes require negative and abuse-case tests, including
   redaction checks for logs, errors, metrics, and responses.

HTTP routers, middleware, browser auth, file uploads, webhooks, queues, and
role-based authorization do not exist in this service today. If any of those
surfaces are added, add the matching helpers and positive/negative tests in the
same change.

## Reusable Patterns

Keep helpers close to the package unless more than one package needs them.
Current examples:

- `ratelimiterserver/server_unit_test.go` builds service instances with fake
  backends and event sinks.
- `ratelimiterserver/server_integration_test.go` builds Redis-backed services
  and asserts current usage, metrics, cancellation, and refund behavior.
- `ratelimiterserver/backend/redis/redis_test.go` creates real Redis backends
  and resets Redis state per test.
- `internal/events/events_test.go` uses fake SQL drivers, Postgres integration,
  log capture, and event payload assertions.
- `cmd/quota-service/main_test.go` tests config loading, TLS, health,
  background sweeper ticks, logging, and gRPC serving behavior.
- `test/e2e/docker-compose-critical-rpcs.sh` provides reusable shell helpers
  for `grpcurl` calls, JSON assertions, service logs, health checks, and Redis
  outage/recovery waits.

## Before Merge

For small pure-logic changes:

```bash
make test-fast
```

For normal changes:

```bash
make verify
```

For Redis, Postgres, release, e2e, or confidence-critical changes:

```bash
make redis-up
make postgres-up
export QUOTA_TEST_REDIS_URL=redis://localhost:6379/0
export QUOTA_TEST_POSTGRES_URL=postgres://quota:quota@localhost:15432/quota?sslmode=disable
make verify-ci
```
