// Package redis is the Redis-backed implementation of the
// backend.Backend interface. It is the only backend the
// rate-limiter ships with in v0.4.0; the algorithms (sliding window,
// token bucket, GCRA, reservation, lease) rely on Redis Lua scripts
// for atomicity, so the abstraction is intentionally Redis-shaped.
//
// Requirements:
//
//   - Redis 7.0 or newer (the embedded Lua scripts use cjson and
//     redis.replicate_commands, plus structured-reply features that
//     landed in 7.x).
//   - A primary-mode deployment (single Redis instance or a
//     primary/replica pair with reads pinned to the primary). Redis
//     Cluster is not supported in v1 because EVALSHA / Lua key
//     scoping does not extend across slots without code changes.
//
// New pings Redis synchronously during construction and returns an
// error if the dsn is malformed, the server is unreachable, or any
// of the embedded Lua scripts fails to load. A failed New leaves
// nothing behind: the underlying connection is closed before the
// error is returned.
//
// Usage:
//
//	backend, err := redis.New(ctx, "redis://localhost:6379/0")
//	if err != nil { return err }
//	defer backend.Close()
//
//	srv, err := ratelimiterserver.New(ctx, ratelimiterserver.Options{
//	    Product:     "myapp",
//	    Environment: "prod",
//	    Backend:     backend,
//	})
package redis
