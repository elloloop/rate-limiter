# Architecture

`rate-limiter` is a deployable quota math service. Each product runs its own
instance with its own Redis and optional event sink.

```text
caller/product
  -> builds concrete Limit messages from business policy
  -> calls QuotaService over gRPC

quota-service
  -> validates the supplied limits
  -> evaluates all limits atomically in Redis
  -> returns a Decision and per-limit state
  -> optionally emits append-only usage events
```

The service is deliberately business-agnostic. `scope_key`, `action`, and
`unit` are opaque strings. The service never fetches profiles, plans,
contracts, or pricing.

## Isolation Model

Every product deploys its own stack:

```text
Product A -> quota-service -> Product A Redis -> Product A event sink
Product B -> quota-service -> Product B Redis -> Product B event sink
Product C -> quota-service -> Product C Redis -> Product C event sink
```

The Docker image, protobuf contract, Redis scripts, and documentation are
shared. Redis data and runtime infrastructure are not shared.

## Hot Path

Mutating RPCs use Redis Lua scripts so a decision either fully applies or does
not mutate anything:

1. Check idempotency by `request_id`.
2. Validate supplied limits.
3. Evaluate all limits.
4. If any limit denies, return denial without counter mutations.
5. If all limits allow, mutate all impacted Redis keys.
6. Store the idempotent response.
7. Emit a usage event asynchronously when configured.

## Security Boundary

The service only supports transport security:

- plaintext gRPC by default
- optional TLS
- optional mTLS

It does not implement JWT validation, API keys, tenant auth, user auth, RBAC, or
business authorization. Put the service behind VPC networking, security groups,
Kubernetes NetworkPolicy, service mesh policy, or internal load balancers.

