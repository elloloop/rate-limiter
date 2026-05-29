// Command embedded shows how a host program mounts the rate-limiter
// into its own *grpc.Server instead of running the dedicated
// quota-service container. It builds one ratelimiterserver.Server
// against a real Redis backend and registers it on a host-owned
// grpc.Server alongside (in a real host) the application's own
// services.
//
// Run it with:
//
//	docker run --rm -p 16399:6379 redis:7.4-alpine
//	go run ./examples/embedded
//
// Then exercise it:
//
//	grpcurl -plaintext localhost:8090 list quota.v1.QuotaService
//
// The example deliberately fails fast if Redis is not reachable;
// see ratelimiterserver/backend/redis for the requirements.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	"github.com/elloloop/rate-limiter/ratelimiterserver"
	rlredis "github.com/elloloop/rate-limiter/ratelimiterserver/backend/redis"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisDSN := envOr("EMBEDDED_REDIS_URL", "redis://localhost:16399/0")
	grpcAddr := envOr("EMBEDDED_GRPC_ADDR", "127.0.0.1:8090")

	backend, err := rlredis.New(ctx, redisDSN)
	if err != nil {
		return fmt.Errorf("redis backend: %w", err)
	}
	defer func() { _ = backend.Close() }()

	rl, err := ratelimiterserver.New(ctx, ratelimiterserver.Options{
		Product:     "embedded-demo",
		Environment: "local",
		Backend:     backend,
	})
	if err != nil {
		return fmt.Errorf("ratelimiterserver.New: %w", err)
	}

	g := grpc.NewServer()
	quotav1.RegisterQuotaServiceServer(g, rl)
	// A real host would register its own services here too:
	//   myservicepb.RegisterMyServiceServer(g, myImpl)
	reflection.Register(g)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", grpcAddr, err)
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("embedded quota service listening on %s (redis=%s)", grpcAddr, redisDSN)
		serveErr <- g.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		log.Print("shutting down")
	case err := <-serveErr:
		return fmt.Errorf("grpc serve: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		g.Stop()
	}
	if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
