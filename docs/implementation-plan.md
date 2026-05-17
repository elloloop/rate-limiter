# Implementation Plan

The build order for v1 is:

1. Protobuf definition and generated Go stubs.
2. Documentation and docs site.
3. Tests for validation, keying, Redis scripts, idempotency, and gRPC behavior.
4. CI, docs deploy, release, and Docker publishing workflows.
5. gRPC server skeleton.
6. Redis client and script loader.
7. Limit validation.
8. Fixed calendar window consume.
9. Fixed duration window consume.
10. Token bucket consume.
11. Sliding window consume.
12. Concurrency leases.
13. Reservations, finalization, and release.
14. Leaky bucket and GCRA.
15. Explain and GetCurrentUsage.
16. Async event emission.
17. Docker image hardening.

SDKs are intentionally excluded from the initial repository cut and can be
added later as generated or thin client packages.

Current release gates include unit tests, Redis-backed race-enabled integration
tests, a Docker Compose e2e smoke test, docs builds, Docker builds, protobuf
generation checks, and high/critical vulnerability scans.
