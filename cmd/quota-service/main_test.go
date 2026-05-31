package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	"github.com/elloloop/rate-limiter/internal/config"
	"github.com/elloloop/rate-limiter/ratelimiterserver"
	"github.com/elloloop/rate-limiter/ratelimiterserver/backend"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func TestValidateLimitsCommandAcceptsExamples(t *testing.T) {
	examples := []string{
		"workspace-email.yaml",
		"assistant-llm.yaml",
		"assistant-concurrency.yaml",
	}
	for _, example := range examples {
		t.Run(example, func(t *testing.T) {
			path := filepath.Join("..", "..", "examples", "limits", example)
			if err := validateLimits(path); err != nil {
				t.Fatalf("validateLimits(%s): %v", example, err)
			}
		})
	}
}

func TestParsersRejectUnknownValues(t *testing.T) {
	if got := parseAlgorithm("ALGORITHM_DOES_NOT_EXIST"); got != quotav1.Algorithm_ALGORITHM_UNSPECIFIED {
		t.Fatalf("unexpected algorithm parse result: %s", got)
	}
	if got := parseWindowType("WINDOW_TYPE_DOES_NOT_EXIST"); got != quotav1.WindowType_WINDOW_TYPE_UNSPECIFIED {
		t.Fatalf("unexpected window type parse result: %s", got)
	}
	if got := parseCalendarUnit("CALENDAR_UNIT_DOES_NOT_EXIST"); got != quotav1.CalendarUnit_CALENDAR_UNIT_UNSPECIFIED {
		t.Fatalf("unexpected calendar unit parse result: %s", got)
	}
	if got := parseReservationExpiryPolicy("RESERVATION_EXPIRY_POLICY_DOES_NOT_EXIST"); got != quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_UNSPECIFIED {
		t.Fatalf("unexpected expiry policy parse result: %s", got)
	}
}

func TestRunHandlesNonServingCommands(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "version", args: []string{"version"}},
		{name: "help", args: []string{"help"}},
		{name: "validate-limits-usage", args: []string{"validate-limits"}, wantErr: "usage:"},
		{name: "unknown", args: []string{"does-not-exist"}, wantErr: "unknown command"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(tt.args)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("run(%v): %v", tt.args, err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("run(%v) succeeded, want error containing %q", tt.args, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("run(%v) error = %v, want %q", tt.args, err, tt.wantErr)
				}
			}
		})
	}
}

func TestMainReturnsForSuccessfulCommand(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"quota-service", "version"}
	t.Cleanup(func() { os.Args = oldArgs })

	out := captureStdout(t, main)
	if !strings.Contains(out, "quota-service") {
		t.Fatalf("main version output = %q, want version banner", out)
	}
}

func TestRunPrintConfigCommand(t *testing.T) {
	t.Setenv("QUOTA_REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("QUOTA_EVENT_SINK", "none")
	t.Setenv("QUOTA_TLS_ENABLED", "false")
	t.Setenv("QUOTA_MTLS_ENABLED", "false")

	out := captureStdout(t, func() {
		if err := run([]string{"print-config"}); err != nil {
			t.Fatalf("run print-config: %v", err)
		}
	})
	if !strings.Contains(out, `"EventSink": "none"`) {
		t.Fatalf("print-config output missing event sink: %s", out)
	}
}

func TestRunValidateLimitsCommand(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "limits", "workspace-email.yaml")
	if err := run([]string{"validate-limits", path}); err != nil {
		t.Fatalf("run validate-limits: %v", err)
	}
}

func TestRunDefaultsToServeAndValidatesConfiguration(t *testing.T) {
	t.Setenv("QUOTA_REDIS_MODE", "cluster")

	err := run(nil)
	if err == nil {
		t.Fatal("expected default serve command to reject invalid config")
	}
	if !strings.Contains(err.Error(), "QUOTA_REDIS_MODE") {
		t.Fatalf("run(nil) error = %v, want Redis mode validation", err)
	}
}

func TestRunServeReturnsRedisInitializationError(t *testing.T) {
	t.Setenv("QUOTA_REDIS_URL", "://not-a-redis-url")
	t.Setenv("QUOTA_REDIS_MODE", "single_primary")
	t.Setenv("QUOTA_EVENT_SINK", "none")
	t.Setenv("QUOTA_TLS_ENABLED", "false")
	t.Setenv("QUOTA_MTLS_ENABLED", "false")

	err := run([]string{"serve"})
	if err == nil {
		t.Fatal("expected serve to fail Redis initialization")
	}
	if !strings.Contains(err.Error(), "redis init") {
		t.Fatalf("run serve error = %v, want Redis initialization failure", err)
	}
}

func TestServeReturnsEventSinkInitializationErrorWithRedis(t *testing.T) {
	redisURL := redisURLForStartupTest(t)
	t.Setenv("QUOTA_REDIS_URL", redisURL)
	t.Setenv("QUOTA_EVENT_SINK", "postgres")
	t.Setenv("QUOTA_EVENT_DATABASE_URL", "%")
	t.Setenv("QUOTA_TLS_ENABLED", "false")
	t.Setenv("QUOTA_MTLS_ENABLED", "false")

	err := run([]string{"serve"})
	if err == nil {
		t.Fatal("expected serve to fail event sink initialization")
	}
	if !strings.Contains(err.Error(), "event sink init") {
		t.Fatalf("run serve error = %v, want event sink initialization failure", err)
	}
}

func TestServeReturnsTLSConfigurationErrorWithRedis(t *testing.T) {
	redisURL := redisURLForStartupTest(t)
	dir := t.TempDir()
	t.Setenv("QUOTA_REDIS_URL", redisURL)
	t.Setenv("QUOTA_EVENT_SINK", "none")
	t.Setenv("QUOTA_GRPC_BIND_ADDR", "127.0.0.1:0")
	t.Setenv("QUOTA_METRICS_BIND_ADDR", "127.0.0.1:0")
	t.Setenv("QUOTA_TLS_ENABLED", "true")
	t.Setenv("QUOTA_TLS_CERT_FILE", filepath.Join(dir, "missing.crt"))
	t.Setenv("QUOTA_TLS_KEY_FILE", filepath.Join(dir, "missing.key"))
	t.Setenv("QUOTA_MTLS_ENABLED", "false")

	err := run([]string{"serve"})
	if err == nil {
		t.Fatal("expected serve to fail TLS configuration")
	}
	if !strings.Contains(err.Error(), "missing.crt") {
		t.Fatalf("run serve error = %v, want missing certificate failure", err)
	}
}

func TestServeReturnsGRPCListenErrorWithRedis(t *testing.T) {
	redisURL := redisURLForStartupTest(t)
	t.Setenv("QUOTA_REDIS_URL", redisURL)
	t.Setenv("QUOTA_EVENT_SINK", "none")
	t.Setenv("QUOTA_GRPC_BIND_ADDR", "127.0.0.1:-1")
	t.Setenv("QUOTA_METRICS_BIND_ADDR", "127.0.0.1:0")
	t.Setenv("QUOTA_TLS_ENABLED", "false")
	t.Setenv("QUOTA_MTLS_ENABLED", "false")

	err := run([]string{"serve"})
	if err == nil {
		t.Fatal("expected serve to fail gRPC listen")
	}
	if !strings.Contains(err.Error(), "too many colons") && !strings.Contains(err.Error(), "missing port") && !strings.Contains(err.Error(), "invalid port") {
		t.Fatalf("run serve error = %v, want bind failure", err)
	}
}

func TestServeStartsAndStopsOnInterruptWithRedis(t *testing.T) {
	redisURL := redisURLForStartupTest(t)
	grpcAddr := freeTCPAddr(t)
	metricsAddr := freeTCPAddr(t)
	t.Setenv("QUOTA_REDIS_URL", redisURL)
	t.Setenv("QUOTA_EVENT_SINK", "none")
	t.Setenv("QUOTA_GRPC_BIND_ADDR", grpcAddr)
	t.Setenv("QUOTA_METRICS_BIND_ADDR", metricsAddr)
	t.Setenv("QUOTA_TLS_ENABLED", "false")
	t.Setenv("QUOTA_MTLS_ENABLED", "false")

	done := make(chan error, 1)
	go func() {
		done <- serve()
	}()

	waitForTCP(t, grpcAddr)

	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("find current process: %v", err)
	}
	if err := proc.Signal(os.Interrupt); err != nil {
		t.Fatalf("interrupt serve: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not stop after interrupt")
	}
}

func TestServeStopsWhenMetricsServerFailsWithRedis(t *testing.T) {
	redisURL := redisURLForStartupTest(t)
	t.Setenv("QUOTA_REDIS_URL", redisURL)
	t.Setenv("QUOTA_EVENT_SINK", "none")
	t.Setenv("QUOTA_GRPC_BIND_ADDR", freeTCPAddr(t))
	t.Setenv("QUOTA_METRICS_BIND_ADDR", "127.0.0.1:-1")
	t.Setenv("QUOTA_TLS_ENABLED", "false")
	t.Setenv("QUOTA_MTLS_ENABLED", "false")

	done := make(chan error, 1)
	go func() {
		done <- serve()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not stop after metrics failure")
	}
}

func TestPrintConfigRedactsSecretsAndValidates(t *testing.T) {
	t.Setenv("QUOTA_REDIS_URL", "redis://:secret@redis.internal:6379/0?TOKEN=secret-token")
	t.Setenv("QUOTA_EVENT_SINK", "none")
	t.Setenv("QUOTA_TLS_ENABLED", "false")
	t.Setenv("QUOTA_MTLS_ENABLED", "false")

	out := captureStdout(t, func() {
		if err := printConfig(); err != nil {
			t.Fatalf("printConfig: %v", err)
		}
	})
	if !strings.Contains(out, `"RedisURL"`) {
		t.Fatalf("printConfig output missing RedisURL: %s", out)
	}
	if strings.Contains(out, "secret") || strings.Contains(out, "secret-token") {
		t.Fatalf("printConfig leaked secret: %s", out)
	}
}

func TestValidateLimitsRejectsBadInputs(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	if err := validateLimits(missing); err == nil {
		t.Fatal("expected missing file error")
	}

	badYAML := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(badYAML, []byte("limits: ["), 0o600); err != nil {
		t.Fatalf("write bad yaml: %v", err)
	}
	if err := validateLimits(badYAML); err == nil {
		t.Fatal("expected malformed YAML error")
	}

	invalidLimits := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(invalidLimits, []byte("limits:\n  - limit_id: missing_fields\n"), 0o600); err != nil {
		t.Fatalf("write invalid limits: %v", err)
	}
	out := captureStdout(t, func() {
		if err := validateLimits(invalidLimits); err == nil {
			t.Fatal("expected invalid limits error")
		}
	})
	if !strings.Contains(out, `"valid": false`) {
		t.Fatalf("expected invalid JSON output, got: %s", out)
	}
}

func TestGRPCServerOptions(t *testing.T) {
	if opts, err := grpcServerOptions(configForTLS(false, false, "", "", "")); err != nil || opts != nil {
		t.Fatalf("TLS disabled options = %v, %v; want nil, nil", opts, err)
	}

	if _, err := grpcServerOptions(configForTLS(true, false, "missing.crt", "missing.key", "")); err == nil {
		t.Fatal("expected missing cert files to fail")
	}

	dir := t.TempDir()
	certFile, keyFile := writeTestCert(t, dir, "server")
	if opts, err := grpcServerOptions(configForTLS(true, false, certFile, keyFile, "")); err != nil || len(opts) != 1 {
		t.Fatalf("TLS options = %d, %v; want one option", len(opts), err)
	}

	if _, err := grpcServerOptions(configForTLS(true, true, certFile, keyFile, filepath.Join(dir, "missing-ca.crt"))); err == nil {
		t.Fatal("expected missing client CA to fail")
	}

	badCA := filepath.Join(dir, "bad-ca.crt")
	if err := os.WriteFile(badCA, []byte("not pem"), 0o600); err != nil {
		t.Fatalf("write bad CA: %v", err)
	}
	if _, err := grpcServerOptions(configForTLS(true, true, certFile, keyFile, badCA)); err == nil {
		t.Fatal("expected invalid client CA to fail")
	}

	caFile, _ := writeTestCert(t, dir, "client-ca")
	if opts, err := grpcServerOptions(configForTLS(true, true, certFile, keyFile, caFile)); err != nil || len(opts) != 1 {
		t.Fatalf("mTLS options = %d, %v; want one option", len(opts), err)
	}
}

func TestHealthProbeUpdatesServingStatus(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	serving := health.NewServer()
	updateHealthStatus(context.Background(), serving, newHealthService(t, &cmdHealthBackend{loaded: true}), logger)
	assertHealthStatus(t, serving, "quota.v1.QuotaService", healthgrpc.HealthCheckResponse_SERVING)

	notServing := health.NewServer()
	updateHealthStatus(context.Background(), notServing, newHealthService(t, &cmdHealthBackend{pingErr: errors.New("down")}), logger)
	assertHealthStatus(t, notServing, "quota.v1.QuotaService", healthgrpc.HealthCheckResponse_NOT_SERVING)
}

func TestRunHealthProbeMarksNotServingOnCancel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	healthServer := health.NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runHealthProbe(ctx, healthServer, newHealthService(t, &cmdHealthBackend{loaded: true}), logger, time.Hour)
	assertHealthStatus(t, healthServer, "quota.v1.QuotaService", healthgrpc.HealthCheckResponse_NOT_SERVING)
}

func TestRunHealthProbeUpdatesOnTicker(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	healthServer := health.NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	quota := newHealthService(t, &cmdHealthBackend{loaded: true})

	go func() {
		defer close(done)
		runHealthProbe(ctx, healthServer, quota, logger, time.Millisecond)
	}()

	deadline := time.After(time.Second)
	for {
		resp, err := healthServer.Check(context.Background(), &healthgrpc.HealthCheckRequest{Service: "quota.v1.QuotaService"})
		if err == nil && resp.GetStatus() == healthgrpc.HealthCheckResponse_SERVING {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("health probe did not mark service serving")
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("health probe did not stop after cancellation")
	}
}

func TestRunReservationExpirySweeperStopsOnCancel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runReservationExpirySweeper(ctx, newHealthService(t, &cmdHealthBackend{}), logger, time.Hour, 100)
}

func TestRunReservationExpirySweeperLogsFailures(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	called := make(chan struct{})
	store := &cmdHealthBackend{
		expireErr:    errors.New("redis down"),
		expireCalled: called,
	}
	done := make(chan struct{})
	quota := newHealthService(t, store)

	go func() {
		defer close(done)
		runReservationExpirySweeper(ctx, quota, logger, time.Hour, 100)
	}()

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("sweeper did not call ExpireReservations")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sweeper did not stop after cancellation")
	}
}

func TestRunReservationExpirySweeperLogsExpirations(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	called := make(chan struct{})
	store := &cmdHealthBackend{
		expireResult: backend.ExpireReservationsResult{Expired: 2, Scanned: 2},
		expireCalled: called,
	}
	done := make(chan struct{})
	quota := newHealthService(t, store)

	go func() {
		defer close(done)
		runReservationExpirySweeper(ctx, quota, logger, time.Hour, 100)
	}()

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("sweeper did not call ExpireReservations")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sweeper did not stop after cancellation")
	}
}

func TestLimitYAMLToProto(t *testing.T) {
	limit := limitYAML{
		LimitID:                 "user_tokens",
		ScopeKey:                "user:user_123",
		Action:                  "assistant.llm.tokens",
		Unit:                    "tokens",
		Algorithm:               "ALGORITHM_TOKEN_BUCKET",
		Limit:                   100,
		Burst:                   150,
		RefillRatePerSec:        25,
		Refundable:              true,
		ReservationExpiryPolicy: "RESERVATION_EXPIRY_POLICY_REFUND_FULL",
		Metadata:                map[string]string{"model": "dummy"},
		Window: windowYAML{
			Type:       "WINDOW_TYPE_CONTINUOUS",
			DurationMS: 60000,
		},
	}

	got := limit.toProto()
	if got.GetLimitId() != "user_tokens" || got.GetScopeKey() != "user:user_123" || got.GetAction() != "assistant.llm.tokens" {
		t.Fatalf("unexpected identity fields: %v", got)
	}
	if got.GetAlgorithm() != quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET {
		t.Fatalf("algorithm = %s, want token bucket", got.GetAlgorithm())
	}
	if got.GetWindow().GetType() != quotav1.WindowType_WINDOW_TYPE_CONTINUOUS || got.GetWindow().GetDurationMs() != 60000 {
		t.Fatalf("unexpected window: %v", got.GetWindow())
	}
	if got.GetLimit() != 100 || got.GetBurst() != 150 || got.GetRefillRatePerSec() != 25 {
		t.Fatalf("unexpected capacity fields: %v", got)
	}
	if !got.GetRefundable() || got.GetReservationExpiryPolicy() != quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_REFUND_FULL {
		t.Fatalf("unexpected reservation fields: %v", got)
	}
	if got.GetMetadata()["model"] != "dummy" {
		t.Fatalf("metadata missing: %v", got.GetMetadata())
	}
}

func TestWindowYAMLReturnsNilWhenEmpty(t *testing.T) {
	if got := (windowYAML{}).toProto(); got != nil {
		t.Fatalf("empty window converted to %v, want nil", got)
	}
}

func TestLogLevelParser(t *testing.T) {
	tests := map[string]string{
		"debug":   "DEBUG",
		"warn":    "WARN",
		"warning": "WARN",
		"error":   "ERROR",
		"unknown": "INFO",
		"":        "INFO",
	}
	for value, want := range tests {
		if got := logLevel(value).String(); got != want {
			t.Fatalf("logLevel(%q) = %s, want %s", value, got, want)
		}
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	original := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = original })
	fn()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	return string(out)
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free tcp addr: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close free tcp listener: %v", err)
	}
	return addr
}

func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			if err := conn.Close(); err != nil {
				t.Fatalf("close tcp probe: %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for tcp listener %s", addr)
}

func redisURLForStartupTest(t *testing.T) string {
	t.Helper()
	redisURL := os.Getenv("QUOTA_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set QUOTA_TEST_REDIS_URL to run Redis-backed startup tests")
	}
	parsed, err := url.Parse(redisURL)
	if err != nil {
		t.Fatalf("parse QUOTA_TEST_REDIS_URL: %v", err)
	}
	parsed.Path = "/3"
	return parsed.String()
}

func configForTLS(enabled, mtls bool, certFile, keyFile, caFile string) config.Config {
	return config.Config{
		TLSEnabled:       enabled,
		TLSCertFile:      certFile,
		TLSKeyFile:       keyFile,
		MTLSEnabled:      mtls,
		MTLSClientCAFile: caFile,
	}
}

func writeTestCert(t *testing.T, dir, name string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certFile := filepath.Join(dir, name+".crt")
	keyFile := filepath.Join(dir, name+".key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

func newHealthService(t *testing.T, store backend.Backend) *ratelimiterserver.Server {
	t.Helper()
	svc, err := ratelimiterserver.New(context.Background(), ratelimiterserver.Options{
		Product:     "test",
		Environment: "test",
		Backend:     store,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc
}

func assertHealthStatus(t *testing.T, healthServer *health.Server, service string, want healthgrpc.HealthCheckResponse_ServingStatus) {
	t.Helper()
	resp, err := healthServer.Check(context.Background(), &healthgrpc.HealthCheckRequest{Service: service})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if resp.GetStatus() != want {
		t.Fatalf("health status = %s, want %s", resp.GetStatus(), want)
	}
}

type cmdHealthBackend struct {
	backend.Backend
	pingErr      error
	loaded       bool
	expireResult backend.ExpireReservationsResult
	expireErr    error
	expireCalled chan struct{}
}

func (b *cmdHealthBackend) Ping(context.Context) error {
	return b.pingErr
}

func (b *cmdHealthBackend) ScriptsLoaded(context.Context) (bool, error) {
	return b.loaded, nil
}

func (b *cmdHealthBackend) LoadScripts(context.Context) error {
	b.loaded = true
	return nil
}

func (b *cmdHealthBackend) ExpireReservations(context.Context, string, time.Time, int64) (backend.ExpireReservationsResult, error) {
	if b.expireCalled != nil {
		select {
		case b.expireCalled <- struct{}{}:
		default:
		}
	}
	return b.expireResult, b.expireErr
}
