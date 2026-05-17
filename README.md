# Rate Limiter

Generic quota and rate limiting service for ElloLoop products.

`rate-limiter` is a pure gRPC service that evaluates caller-supplied limits
against Redis-backed counters and returns allow/deny decisions. It is not a
policy engine: it does not know product plans, pricing, users, contracts,
profiles, or entitlements.

The architectural boundary is:

```text
Business rules outside.
Quota math inside.
```

## What It Does

- Enforces known-cost consumption with idempotent `Consume` calls.
- Reserves estimated usage and finalizes or releases reservations without
  recomputing window keys.
- Manages concurrency through Redis-backed leases.
- Supports fixed calendar windows, fixed duration windows, sliding windows,
  token buckets, leaky buckets, GCRA, and semaphores.
- Emits factual usage events asynchronously to `none`, `stdout`, or Postgres.
- Exposes gRPC health, gRPC reflection, and Prometheus metrics.
- Ships as one Docker image: `ghcr.io/elloloop/rate-limiter`.

## What It Does Not Do

- No application-layer auth.
- No JWT/API key/RBAC validation.
- No admin rule CRUD APIs in v1.
- No business rule store.
- No billing or entitlement engine.
- No customer-facing analytics APIs.
- No Redis Cluster support in v1.
- No SDKs in this repo yet.

## Quick Start

```bash
docker compose up -d --build
```

The local stack starts Redis and the quota service:

- gRPC: `localhost:28080`
- Metrics: `http://localhost:29090/metrics`
- Redis: `localhost:6379`

Validate an example limit file:

```bash
quota-service validate-limits examples/limits/workspace-email.yaml
```

Print resolved configuration:

```bash
quota-service print-config
```

## Configuration

```text
QUOTA_PRODUCT=workspace
QUOTA_ENVIRONMENT=local
QUOTA_GRPC_BIND_ADDR=0.0.0.0:8080
QUOTA_REDIS_URL=redis://redis:6379/0
QUOTA_REDIS_MODE=single_primary
QUOTA_EVENT_SINK=none
QUOTA_EVENT_DATABASE_URL=postgres://...
QUOTA_METRICS_BIND_ADDR=0.0.0.0:9090
QUOTA_TLS_ENABLED=false
QUOTA_TLS_CERT_FILE=/etc/quota/tls/server.crt
QUOTA_TLS_KEY_FILE=/etc/quota/tls/server.key
QUOTA_MTLS_ENABLED=false
QUOTA_MTLS_CLIENT_CA_FILE=/etc/quota/tls/client-ca.crt
QUOTA_LOG_LEVEL=info
```

## Documentation

The docs site is published through GitHub Pages:

https://elloloop.github.io/rate-limiter/

Source documentation lives in:

- `docs/`
- `docs-site/`
- `quota/v1/quota.proto`

## Tests

Run the fast unit suite:

```bash
go test ./...
```

Run Redis-backed integration tests:

```bash
docker run --rm -p 16379:6379 redis:7.4-alpine
QUOTA_TEST_REDIS_URL=redis://localhost:16379/0 go test -race -count=1 ./...
```

Run the Docker Compose e2e smoke test:

```bash
test/e2e/docker-compose-smoke.sh
```

CI runs protobuf checks, unit tests, race-enabled Redis integration tests, docs
builds, Docker builds, a Docker Compose e2e smoke test, and Trivy high/critical
vulnerability scanning.

## Releases

Push a `v*` tag to publish:

- `ghcr.io/elloloop/rate-limiter:<version>`
- `ghcr.io/elloloop/rate-limiter:latest`
- a GitHub Release with protobuf archives and checksums
- refreshed GitHub Pages documentation
