# Security policy

## Reporting a vulnerability

**Do not open a public issue for security vulnerabilities.**

Report privately via [GitHub Security Advisories](https://github.com/elloloop/rate-limiter/security/advisories/new).

Include:

- A description of the vulnerability and its impact.
- A repro: steps, payload, affected version.
- Whether you have already disclosed this to anyone else.

We aim to acknowledge reports within 2 business days and to publish a fix within 30 days for high/critical issues.

## Scope note

Rate Limiter does not implement application-layer auth. Deploy it behind
trusted networking, service-mesh policy, internal load balancers, or
equivalent runtime controls. TLS and mTLS are transport-security features
only. Vulnerabilities that assume an attacker already has untrusted direct
access to the gRPC port without any such control are out of scope.

## Supported versions

The most recent minor release receives security fixes. Older minor versions are best-effort.

## What we treat as a vulnerability

- Quota-evasion vectors: a caller exceeding a configured limit through a
  flaw in the windowing, lease, or reservation math.
- Counter corruption or cross-scope leakage (one scope's usage affecting
  another's decision).
- Information disclosure (limit configuration, scope identifiers, internal
  errors) in responses or logs beyond what the API intends.
- Denial-of-service vectors that an unauthenticated caller with port access
  can trigger at disproportionately low cost.
- Supply-chain compromise (dependency, container image, GitHub Actions
  workflow).

Bugs that affect correctness but not security should be filed as regular issues.
