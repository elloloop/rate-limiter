# Testing

The repository has several test layers.

## Unit Tests

```bash
go test ./...
```

These cover configuration validation, YAML limit parsing, limit validation,
Redis key derivation, embedded Lua script presence, command helpers, and
Prometheus metric exposition.

## Redis Integration Tests

```bash
docker run --rm -p 16379:6379 redis:7.4-alpine
QUOTA_TEST_REDIS_URL=redis://localhost:16379/0 go test -race -count=1 ./...
```

These exercise the service against a real Redis instance. Coverage includes
fixed calendar windows, fixed duration windows, sliding windows, token buckets,
leaky buckets, GCRA, reservations, finalization refunds, release refunds,
reservation overages, concurrency leases, renewals, releases, lease expiry,
idempotency, invalid algorithm/RPC combinations, `Explain`, current usage, a
concurrent contention stress case for Redis Lua atomicity, and a real gRPC
client/server round trip.

## Docker Compose E2E

```bash
test/e2e/docker-compose-smoke.sh
```

This builds the service image, starts Redis and the service, calls gRPC through
reflection with `grpcurl`, verifies idempotency and denial behavior, and checks
that Prometheus metrics are exposed.

## CI and Release Gates

CI runs protobuf lint/generation checks, Go tests with Redis and race detection,
the docs build, Docker build smoke, Docker Compose e2e, and Trivy high/critical
filesystem scanning.

Release tags run the same Redis integration and e2e gates before the multi-arch
GHCR image is published. After publish, the release workflow inspects the
published manifest, pulls the versioned image from GHCR, and runs
`quota-service version` from that pulled image before creating the GitHub
Release.
