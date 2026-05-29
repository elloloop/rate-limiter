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

func TestValidateRejectsUnsupportedEventSink(t *testing.T) {
	cfg := Load()
	cfg.EventSink = "kafka"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unsupported event sink to fail")
	}
}

func TestValidateRequiresTLSFilesWhenTLSEnabled(t *testing.T) {
	cfg := Load()
	cfg.TLSEnabled = true
	cfg.TLSCertFile = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing TLS cert to fail")
	}

	cfg.TLSCertFile = "/tmp/server.crt"
	cfg.TLSKeyFile = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing TLS key to fail")
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

func TestValidateRequiresClientCAForMTLS(t *testing.T) {
	cfg := Load()
	cfg.TLSEnabled = true
	cfg.TLSCertFile = "/tmp/server.crt"
	cfg.TLSKeyFile = "/tmp/server.key"
	cfg.MTLSEnabled = true
	cfg.MTLSClientCAFile = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing mTLS client CA to fail")
	}
}

func TestRedactedHidesURLCredentials(t *testing.T) {
	cfg := Config{
		RedisURL:         "redis://:redis-secret@redis.internal:6379/0?TOKEN=abc&safe=visible",
		EventDatabaseURL: "postgres://quota:pg-secret@db.internal:5432/quota?ApiKey=secret&sslmode=require",
	}

	redacted := cfg.Redacted()
	for _, got := range []string{redacted.RedisURL, redacted.EventDatabaseURL} {
		if got == "" {
			t.Fatal("redacted URL should preserve non-empty URL")
		}
		if containsAny(got, []string{"redis-secret", "pg-secret", "TOKEN=abc", "ApiKey=secret"}) {
			t.Fatalf("redacted URL leaked a credential: %s", got)
		}
	}
	if !strings.Contains(redacted.RedisURL, "safe=visible") {
		t.Fatalf("redaction removed non-secret query values: %s", redacted.RedisURL)
	}
}

func TestRedactedHandlesEmptyAndInvalidURLs(t *testing.T) {
	cfg := Config{
		RedisURL:         "",
		EventDatabaseURL: "://not-a-url",
	}
	redacted := cfg.Redacted()
	if redacted.RedisURL != "" {
		t.Fatalf("empty URL redacted to %q, want empty", redacted.RedisURL)
	}
	if redacted.EventDatabaseURL != "[redacted]" {
		t.Fatalf("invalid URL redacted to %q", redacted.EventDatabaseURL)
	}
}

func TestEnvBoolFallsBackOnInvalidValues(t *testing.T) {
	t.Setenv("QUOTA_TEST_BOOL", "not-bool")
	if !envBool("QUOTA_TEST_BOOL", true) {
		t.Fatal("invalid boolean should preserve true fallback")
	}
	if envBool("QUOTA_TEST_BOOL", false) {
		t.Fatal("invalid boolean should preserve false fallback")
	}
}

func TestValidateAcceptsSupportedConfiguration(t *testing.T) {
	cfg := Load()
	cfg.EventSink = "postgres"
	cfg.EventDatabaseURL = "postgres://quota:secret@db.internal:5432/quota"
	cfg.TLSEnabled = true
	cfg.TLSCertFile = "/tmp/server.crt"
	cfg.TLSKeyFile = "/tmp/server.key"
	cfg.MTLSEnabled = true
	cfg.MTLSClientCAFile = "/tmp/client-ca.crt"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestLoadReadsOverridesAndNormalizesLogLevel(t *testing.T) {
	t.Setenv("QUOTA_PRODUCT", "assistant")
	t.Setenv("QUOTA_ENVIRONMENT", "prod")
	t.Setenv("QUOTA_GRPC_BIND_ADDR", "127.0.0.1:18080")
	t.Setenv("QUOTA_REDIS_URL", "redis://redis.internal:6379/3")
	t.Setenv("QUOTA_REDIS_MODE", "single_primary")
	t.Setenv("QUOTA_EVENT_SINK", "stdout")
	t.Setenv("QUOTA_EVENT_DATABASE_URL", "postgres://quota@db/quota")
	t.Setenv("QUOTA_METRICS_BIND_ADDR", "127.0.0.1:19090")
	t.Setenv("QUOTA_TLS_ENABLED", "true")
	t.Setenv("QUOTA_TLS_CERT_FILE", "/certs/server.crt")
	t.Setenv("QUOTA_TLS_KEY_FILE", "/certs/server.key")
	t.Setenv("QUOTA_MTLS_ENABLED", "true")
	t.Setenv("QUOTA_MTLS_CLIENT_CA_FILE", "/certs/client-ca.crt")
	t.Setenv("QUOTA_LOG_LEVEL", "WARN")

	cfg := Load()
	if cfg.Product != "assistant" || cfg.Environment != "prod" {
		t.Fatalf("unexpected identity overrides: %#v", cfg)
	}
	if cfg.GRPCBindAddr != "127.0.0.1:18080" || cfg.MetricsBindAddr != "127.0.0.1:19090" {
		t.Fatalf("unexpected bind overrides: %#v", cfg)
	}
	if cfg.RedisURL != "redis://redis.internal:6379/3" || cfg.RedisMode != "single_primary" {
		t.Fatalf("unexpected Redis overrides: %#v", cfg)
	}
	if cfg.EventSink != "stdout" || cfg.EventDatabaseURL != "postgres://quota@db/quota" {
		t.Fatalf("unexpected event overrides: %#v", cfg)
	}
	if !cfg.TLSEnabled || cfg.TLSCertFile != "/certs/server.crt" || cfg.TLSKeyFile != "/certs/server.key" {
		t.Fatalf("unexpected TLS overrides: %#v", cfg)
	}
	if !cfg.MTLSEnabled || cfg.MTLSClientCAFile != "/certs/client-ca.crt" {
		t.Fatalf("unexpected mTLS overrides: %#v", cfg)
	}
	if cfg.LogLevel != "warn" {
		t.Fatalf("log level = %q, want warn", cfg.LogLevel)
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
