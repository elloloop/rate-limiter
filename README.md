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
- Reserves estimated usage, increments or shrinks active reservations for
  streaming workloads, and finalizes or releases reservations without
  recomputing window keys.
- Manages concurrency through Redis-backed leases.
- Supports fixed calendar windows, fixed duration windows, sliding windows,
  token buckets, leaky buckets, GCRA, and semaphores.
- Emits factual usage events asynchronously to `none`, `stdout`, or Postgres.
- Exposes gRPC health, gRPC reflection, and Prometheus metrics.
- Ships two ways: a signed, multi-arch Docker image
  (`ghcr.io/elloloop/rate-limiter`) and an importable Go module plus a
  language-agnostic proto bundle (see [Consuming the API](#consuming-the-api)).

## What It Does Not Do

- No application-layer auth.
- No JWT/API key/RBAC validation.
- No admin rule CRUD APIs in v1.
- No business rule store.
- No billing or entitlement engine.
- No customer-facing analytics APIs.
- No Redis Cluster support in v1.
- No hand-written client SDKs (the generated gRPC stubs are importable; see
  [Consuming the API](#consuming-the-api)).

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

## Consuming the API

The service has no application-layer auth and no hand-written SDK, but the
`quota.v1` gRPC contract is published for consumers in two forms.

**Go** — import the generated client straight from the module:

```bash
go get github.com/elloloop/rate-limiter@v0.4.8
```

```go
import quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"

conn, _ := grpc.NewClient("quota:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := quotav1.NewQuotaServiceClient(conn)
resp, _ := client.Consume(ctx, &quotav1.ConsumeRequest{ /* ... */ })
```

**Other languages** — each release attaches a `rate-limiter-protos-<version>`
bundle (`.tar.gz` / `.zip` + `.sha256`) containing `quota/`, `buf.yaml`, and
`buf.gen.yaml`. Codegen against that pinned contract instead of vendoring a git
ref.

## Library mode (embedding)

Go programs that prefer one process to two can mount the rate-limiter into
their own `*grpc.Server` instead of running the dedicated container. The
`ratelimiterserver` package exposes a constructor that returns a
`quotav1.QuotaServiceServer`:

```go
import (
    "context"
    "log"
    "net"

    "google.golang.org/grpc"

    quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
    "github.com/elloloop/rate-limiter/ratelimiterserver"
    rlredis "github.com/elloloop/rate-limiter/ratelimiterserver/backend/redis"
)

func main() {
    ctx := context.Background()

    backend, err := rlredis.New(ctx, "redis://localhost:6379/0")
    if err != nil { log.Fatal(err) }
    defer backend.Close()

    rl, err := ratelimiterserver.New(ctx, ratelimiterserver.Options{
        Product:     "myapp",
        Environment: "prod",
        Backend:     backend,
    })
    if err != nil { log.Fatal(err) }

    g := grpc.NewServer()
    quotav1.RegisterQuotaServiceServer(g, rl)
    lis, _ := net.Listen("tcp", ":8080")
    g.Serve(lis)
}
```

A runnable end-to-end example lives in `examples/embedded`; see its package
doc for the env vars it accepts. `cmd/quota-service` is itself a thin shim
over `ratelimiterserver.New`, so embedded and container modes share one
service-layer wiring.

**Backends** — v0.4.0 ships exactly one backend (Redis); the algorithms
depend on Redis Lua atomicity. The `ratelimiterserver/backend.Backend`
interface keeps the door open for future drivers without breaking the
`Server` / `Options` shape. The smallest supported "minimal" deployment
is the embedded application plus a real Redis instance — there is no
in-memory backend.

## Documentation

The docs site is published through GitHub Pages:

https://elloloop.github.io/rate-limiter/

Source documentation lives in:

- `docs/`
- `docs-site/`
- `quota/v1/quota.proto`

## Contributing and Security

- Contribution guide: [CONTRIBUTING.md](CONTRIBUTING.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- Security policy: [.github/SECURITY.md](.github/SECURITY.md)
- Contributor license agreement: [CLA.md](CLA.md)

## Tests

Run the fast unit suite:

```bash
go test ./...
```

Run Redis-backed integration tests:

```bash
docker run --rm -p 16379:6379 redis:7.4.2-alpine
QUOTA_TEST_REDIS_URL=redis://localhost:16379/0 go test -race -count=1 ./...
```

Reproduce the full CI gate set locally with the Makefile:

```bash
make help          # list targets
make ci            # lint, tidy-check, vuln, build, test, smoke, fuzz
make redis-up      # throwaway Redis for the Redis-backed paths
make test-cover    # coverage profile + aggregate and per-package gates
make ci-full       # ci + docker-compose critical-RPC e2e
```

CI (`.github/workflows/ci.yml`) runs golangci-lint, protobuf checks,
`govulncheck`, race-enabled unit/Redis tests with per-package coverage gates
(`.coverage-gates.yml`), a boot smoke, a fuzz smoke, docs and Docker builds, a
Docker Compose critical-RPC e2e, and a Trivy filesystem scan. CodeQL
(`codeql.yml`) and a nightly race/fuzz loop (`nightly.yml`) run on a schedule.

## Releases

Push a `v*` tag to publish. The release re-runs every CI gate, then:

- builds and pushes the multi-arch image
  `ghcr.io/elloloop/rate-limiter:<version>` with SBOM and provenance
  attestations. A mutable `:latest` tag is also published for discovery;
  production deployments should pin a version tag or digest.
- signs each tag with cosign keyless OIDC;
- scans the published image with Trivy (HIGH/CRITICAL gate, SARIF to GitHub
  Security);
- pulls the image back and verifies its `version` output;
- creates a GitHub Release with the proto bundle and checksums.

Verify the published image's signature:

```bash
cosign verify ghcr.io/elloloop/rate-limiter:<version> \
  --certificate-identity-regexp '^https://github.com/elloloop/rate-limiter/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

GitHub Pages documentation deploys from `main` through the docs workflow.
