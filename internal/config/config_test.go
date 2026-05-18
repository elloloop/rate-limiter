package config

import (
	"strings"
	"testing"
)

func TestLoadUsesDocumentedDefaults(t *testing.T) {
	t.Setenv("QUOTA_PRODUCT", "")
	t.Setenv("QUOTA_ENVIRONMENT", "")
	t.Setenv("QUOTA_GRPC_BIND_ADDR", "")
	t.Setenv("QUOTA_REDIS_URL", "")
	t.Setenv("QUOTA_REDIS_MODE", "")
	t.Setenv("QUOTA_EVENT_SINK", "")
	t.Setenv("QUOTA_METRICS_BIND_ADDR", "")
	t.Setenv("QUOTA_TLS_ENABLED", "")
	t.Setenv("QUOTA_MTLS_ENABLED", "")
	t.Setenv("QUOTA_LOG_LEVEL", "")

	cfg := Load()
	if cfg.Product != "default" || cfg.Environment != "local" {
		t.Fatalf("unexpected product/environment defaults: %#v", cfg)
	}
	if cfg.GRPCBindAddr != "0.0.0.0:8080" || cfg.MetricsBindAddr != "0.0.0.0:9090" {
		t.Fatalf("unexpected bind addr defaults: %#v", cfg)
	}
	if cfg.RedisURL != "redis://localhost:6379/0" || cfg.RedisMode != "single_primary" {
		t.Fatalf("unexpected Redis defaults: %#v", cfg)
	}
	if cfg.EventSink != "none" || cfg.TLSEnabled || cfg.MTLSEnabled || cfg.LogLevel != "info" {
		t.Fatalf("unexpected optional defaults: %#v", cfg)
	}
}

func TestValidateRejectsUnsupportedRedisMode(t *testing.T) {
	cfg := Load()
	cfg.RedisMode = "cluster"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Redis Cluster mode to be rejected in v1")
	}
}

func TestValidateRequiresPostgresURLForPostgresEvents(t *testing.T) {
	cfg := Load()
	cfg.EventSink = "postgres"
	cfg.EventDatabaseURL = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected postgres event sink without URL to fail")
	}
}

func TestValidateRequiresTLSForMTLS(t *testing.T) {
	cfg := Load()
	cfg.TLSEnabled = false
	cfg.MTLSEnabled = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected mTLS without TLS to fail")
	}
}

func TestRedactedHidesURLCredentials(t *testing.T) {
	cfg := Config{
		RedisURL:         "redis://:redis-secret@redis.internal:6379/0?token=abc",
		EventDatabaseURL: "postgres://quota:pg-secret@db.internal:5432/quota?sslmode=require",
	}

	redacted := cfg.Redacted()
	for _, got := range []string{redacted.RedisURL, redacted.EventDatabaseURL} {
		if got == "" {
			t.Fatal("redacted URL should preserve non-empty URL")
		}
		if containsAny(got, []string{"redis-secret", "pg-secret", "token=abc"}) {
			t.Fatalf("redacted URL leaked a credential: %s", got)
		}
	}
}

func containsAny(value string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
