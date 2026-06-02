package ratelimiterserver_test

import (
	"strings"
	"testing"

	"github.com/elloloop/rate-limiter/ratelimiterserver"
	"github.com/elloloop/rate-limiter/ratelimiterserver/backend"
)

// noopBackend implements backend.Backend just enough for Options validation
// tests to reach (or skip) the RedisMode check; no method is exercised here.
type noopBackend struct{ backend.Backend }

func TestNewRequiresProduct(t *testing.T) {
	_, err := ratelimiterserver.New(ratelimiterserver.Options{
		Environment: "prod",
		Backend:     noopBackend{},
	})
	if err == nil || !strings.Contains(err.Error(), "Product") {
		t.Fatalf("err = %v, want one mentioning Product", err)
	}
}

func TestNewRequiresEnvironment(t *testing.T) {
	_, err := ratelimiterserver.New(ratelimiterserver.Options{
		Product: "app",
		Backend: noopBackend{},
	})
	if err == nil || !strings.Contains(err.Error(), "Environment") {
		t.Fatalf("err = %v, want one mentioning Environment", err)
	}
}

func TestNewRequiresBackend(t *testing.T) {
	_, err := ratelimiterserver.New(ratelimiterserver.Options{
		Product:     "app",
		Environment: "prod",
	})
	if err == nil || !strings.Contains(err.Error(), "Backend") {
		t.Fatalf("err = %v, want one mentioning Backend", err)
	}
}

func TestNewRejectsUnsupportedRedisMode(t *testing.T) {
	_, err := ratelimiterserver.New(ratelimiterserver.Options{
		Product:     "app",
		Environment: "prod",
		Backend:     noopBackend{},
		RedisMode:   "cluster",
	})
	if err == nil || !strings.Contains(err.Error(), "RedisMode") {
		t.Fatalf("err = %v, want one mentioning RedisMode", err)
	}
	if !strings.Contains(err.Error(), "cluster") {
		t.Fatalf("err should echo the offending value: %v", err)
	}
}

func TestNewAcceptsEmptyRedisMode(t *testing.T) {
	srv, err := ratelimiterserver.New(ratelimiterserver.Options{
		Product:     "app",
		Environment: "prod",
		Backend:     noopBackend{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if srv == nil {
		t.Fatal("Server is nil")
	}
}

func TestNewAcceptsSinglePrimaryRedisMode(t *testing.T) {
	srv, err := ratelimiterserver.New(ratelimiterserver.Options{
		Product:     "app",
		Environment: "prod",
		Backend:     noopBackend{},
		RedisMode:   "single_primary",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if srv == nil {
		t.Fatal("Server is nil")
	}
}
