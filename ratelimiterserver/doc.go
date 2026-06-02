// Package ratelimiterserver is the rate-limiter's embeddable public
// API. It lets a Go program import rate-limiter and mount its
// QuotaService onto an existing *grpc.Server instead of running the
// dedicated container image.
//
// Usage:
//
//	backend, err := rlredis.New(ctx, "redis://localhost:6379/0")
//	if err != nil { return err }
//	defer backend.Close()
//
//	rl, err := ratelimiterserver.New(ratelimiterserver.Options{
//	    Product:     "myapp",
//	    Environment: "prod",
//	    Backend:     backend,
//	})
//	if err != nil { return err }
//
//	g := grpc.NewServer()
//	quotav1.RegisterQuotaServiceServer(g, rl)
//	g.Serve(lis)
//
// cmd/quota-service loads configuration from the environment, constructs
// a Redis backend, builds a Server, and serves it on a *grpc.Server with
// reflection, health, a metrics endpoint, and the reservation expiry
// sweeper. The container and the embedded API run the same service-layer
// wiring.
//
// Backend posture:
//
//   - The current release ships exactly one backend, the Redis driver under
//     ratelimiterserver/backend/redis. The rate-limit algorithms
//     (sliding window, token / leaky / GCRA buckets, reservations,
//     concurrency leases) depend on Redis Lua atomicity, so the
//     abstraction is intentionally Redis-shaped.
//   - The server/backend boundary is intentionally shaped around the
//     Redis-backed quota, reservation, and lease operations. Adding an
//     in-memory or alternative-store backend is out of scope; the
//     smallest supported deployment is the
//     embedded application plus a real Redis instance.
package ratelimiterserver
