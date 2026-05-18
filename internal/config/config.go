package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Product          string
	Environment      string
	GRPCBindAddr     string
	RedisURL         string
	RedisMode        string
	EventSink        string
	EventDatabaseURL string
	MetricsBindAddr  string
	TLSEnabled       bool
	TLSCertFile      string
	TLSKeyFile       string
	MTLSEnabled      bool
	MTLSClientCAFile string
	LogLevel         string
}

func Load() Config {
	return Config{
		Product:          env("QUOTA_PRODUCT", "default"),
		Environment:      env("QUOTA_ENVIRONMENT", "local"),
		GRPCBindAddr:     env("QUOTA_GRPC_BIND_ADDR", "0.0.0.0:8080"),
		RedisURL:         env("QUOTA_REDIS_URL", "redis://localhost:6379/0"),
		RedisMode:        env("QUOTA_REDIS_MODE", "single_primary"),
		EventSink:        env("QUOTA_EVENT_SINK", "none"),
		EventDatabaseURL: env("QUOTA_EVENT_DATABASE_URL", ""),
		MetricsBindAddr:  env("QUOTA_METRICS_BIND_ADDR", "0.0.0.0:9090"),
		TLSEnabled:       envBool("QUOTA_TLS_ENABLED", false),
		TLSCertFile:      env("QUOTA_TLS_CERT_FILE", "/etc/quota/tls/server.crt"),
		TLSKeyFile:       env("QUOTA_TLS_KEY_FILE", "/etc/quota/tls/server.key"),
		MTLSEnabled:      envBool("QUOTA_MTLS_ENABLED", false),
		MTLSClientCAFile: env("QUOTA_MTLS_CLIENT_CA_FILE", "/etc/quota/tls/client-ca.crt"),
		LogLevel:         strings.ToLower(env("QUOTA_LOG_LEVEL", "info")),
	}
}

func (c Config) Validate() error {
	if c.RedisMode != "single_primary" {
		return fmt.Errorf("QUOTA_REDIS_MODE=%q is unsupported in v1; use single_primary", c.RedisMode)
	}
	switch c.EventSink {
	case "none", "stdout", "postgres":
	default:
		return fmt.Errorf("QUOTA_EVENT_SINK must be one of none, stdout, postgres")
	}
	if c.EventSink == "postgres" && c.EventDatabaseURL == "" {
		return fmt.Errorf("QUOTA_EVENT_DATABASE_URL is required when QUOTA_EVENT_SINK=postgres")
	}
	if c.TLSEnabled && (c.TLSCertFile == "" || c.TLSKeyFile == "") {
		return fmt.Errorf("TLS cert and key files are required when QUOTA_TLS_ENABLED=true")
	}
	if c.MTLSEnabled && !c.TLSEnabled {
		return fmt.Errorf("QUOTA_TLS_ENABLED=true is required when QUOTA_MTLS_ENABLED=true")
	}
	if c.MTLSEnabled && c.MTLSClientCAFile == "" {
		return fmt.Errorf("QUOTA_MTLS_CLIENT_CA_FILE is required when QUOTA_MTLS_ENABLED=true")
	}
	return nil
}

func (c Config) Redacted() Config {
	c.RedisURL = redactURL(c.RedisURL)
	c.EventDatabaseURL = redactURL(c.EventDatabaseURL)
	return c
}

func redactURL(raw string) string {
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "[redacted]"
	}
	if parsed.User != nil {
		username := parsed.User.Username()
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword(username, "xxxxx")
		}
	}
	query := parsed.Query()
	for _, key := range []string{"password", "pass", "token", "apikey", "api_key"} {
		if query.Has(key) {
			query.Set(key, "xxxxx")
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}
