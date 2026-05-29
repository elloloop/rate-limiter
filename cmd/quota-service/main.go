package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gopkg.in/yaml.v3"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	"github.com/elloloop/rate-limiter/internal/config"
	"github.com/elloloop/rate-limiter/internal/events"
	"github.com/elloloop/rate-limiter/internal/limits"
	"github.com/elloloop/rate-limiter/ratelimiterserver"
	rlredis "github.com/elloloop/rate-limiter/ratelimiterserver/backend/redis"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		args = []string{"serve"}
	}
	switch args[0] {
	case "serve":
		return serve()
	case "print-config":
		return printConfig()
	case "validate-limits":
		if len(args) != 2 {
			return errors.New("usage: quota-service validate-limits /path/examples.yaml")
		}
		return validateLimits(args[1])
	case "version":
		fmt.Printf("quota-service %s (%s)\n", version, commit)
		return nil
	case "help", "-h", "--help":
		printHelp()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve() error {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel(cfg.LogLevel)}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	backend, err := rlredis.New(ctx, cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("redis init: %w", err)
	}
	defer func() { _ = backend.Close() }()

	eventSink, err := events.New(cfg.EventSink, cfg.EventDatabaseURL, logger)
	if err != nil {
		return fmt.Errorf("event sink init: %w", err)
	}
	defer func() { _ = eventSink.Close() }()

	quota, err := ratelimiterserver.New(ctx, ratelimiterserver.Options{
		Product:     cfg.Product,
		Environment: cfg.Environment,
		Backend:     backend,
		RedisMode:   cfg.RedisMode,
		EventSink:   eventSink,
		Logger:      logger,
	})
	if err != nil {
		return fmt.Errorf("ratelimiterserver init: %w", err)
	}

	go func() {
		if err := quota.Metrics().Serve(ctx, cfg.MetricsBindAddr); err != nil {
			logger.Error("metrics server failed", "error", err)
			stop()
		}
	}()

	grpcOpts, err := grpcServerOptions(cfg)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpcOpts...)
	quotav1.RegisterQuotaServiceServer(server, quota)
	go runReservationExpirySweeper(ctx, quota, logger, time.Second, 100)

	healthServer := health.NewServer()
	healthgrpc.RegisterHealthServer(server, healthServer)
	updateHealthStatus(ctx, healthServer, quota, logger)
	go runHealthProbe(ctx, healthServer, quota, logger, 5*time.Second)
	reflection.Register(server)

	lis, err := net.Listen("tcp", cfg.GRPCBindAddr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		logger.Info("shutting down grpc server")
		done := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			server.Stop()
		}
	}()

	logger.Info("quota service listening", "grpc", cfg.GRPCBindAddr, "metrics", cfg.MetricsBindAddr, "version", version, "commit", commit)
	if err := server.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

func runReservationExpirySweeper(ctx context.Context, quota *ratelimiterserver.Server, logger *slog.Logger, interval time.Duration, batchSize int64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		expired, err := quota.ExpireReservations(ctx, batchSize)
		if err != nil && ctx.Err() == nil {
			logger.Warn("reservation expiry sweep failed", "error", err)
		}
		if expired > 0 {
			logger.Info("expired reservations swept", "expired", expired)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runHealthProbe(ctx context.Context, healthServer *health.Server, quota *ratelimiterserver.Server, logger *slog.Logger, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			healthServer.SetServingStatus("", healthgrpc.HealthCheckResponse_NOT_SERVING)
			healthServer.SetServingStatus("quota.v1.QuotaService", healthgrpc.HealthCheckResponse_NOT_SERVING)
			return
		case <-ticker.C:
			updateHealthStatus(ctx, healthServer, quota, logger)
		}
	}
}

func updateHealthStatus(ctx context.Context, healthServer *health.Server, quota *ratelimiterserver.Server, logger *slog.Logger) {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	status := healthgrpc.HealthCheckResponse_SERVING
	redisStatus, err := quota.GetRedisStatus(probeCtx, &quotav1.GetRedisStatusRequest{})
	if err != nil || !redisStatus.GetReachable() || redisStatus.GetMessage() != "ok" {
		status = healthgrpc.HealthCheckResponse_NOT_SERVING
		if err != nil {
			logger.Warn("health probe failed", "error", err)
		} else {
			logger.Warn("health probe not serving", "message", redisStatus.GetMessage())
		}
	}

	healthServer.SetServingStatus("", status)
	healthServer.SetServingStatus("quota.v1.QuotaService", status)
}

func printConfig() error {
	cfg := config.Load()
	encoded, err := json.MarshalIndent(cfg.Redacted(), "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return cfg.Validate()
}

func validateLimits(path string) error {
	data, err := os.ReadFile(path) //nolint:gosec // path is an operator-supplied CLI argument to the validate-limits command
	if err != nil {
		return err
	}
	doc := limitFile{}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	converted := make([]*quotav1.Limit, 0, len(doc.Limits))
	for _, limit := range doc.Limits {
		converted = append(converted, limit.toProto())
	}
	errs, warnings := limits.Validate("", converted)
	out := map[string]any{
		"valid":    len(errs) == 0,
		"errors":   errs,
		"warnings": warnings,
	}
	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	if len(errs) > 0 {
		return errors.New("invalid limits")
	}
	return nil
}

func printHelp() {
	fmt.Println(`quota-service commands:
  serve                              Start the gRPC service
  validate-limits /path/examples.yaml Validate a YAML limit file
  print-config                       Print resolved QUOTA_* configuration
  version                            Print build version`)
}

func grpcServerOptions(cfg config.Config) ([]grpc.ServerOption, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if cfg.MTLSEnabled {
		caBytes, err := os.ReadFile(cfg.MTLSClientCAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("failed to parse client CA file")
		}
		tlsConfig.ClientCAs = pool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsConfig))}, nil
}

func logLevel(value string) slog.Level {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type limitFile struct {
	Limits []limitYAML `yaml:"limits"`
}

type limitYAML struct {
	LimitID                 string            `yaml:"limit_id"`
	ScopeKey                string            `yaml:"scope_key"`
	Action                  string            `yaml:"action"`
	Unit                    string            `yaml:"unit"`
	Algorithm               string            `yaml:"algorithm"`
	Window                  windowYAML        `yaml:"window"`
	Limit                   int64             `yaml:"limit"`
	Burst                   int64             `yaml:"burst"`
	RefillRatePerSec        float64           `yaml:"refill_rate_per_sec"`
	Refundable              bool              `yaml:"refundable"`
	ReservationExpiryPolicy string            `yaml:"reservation_expiry_policy"`
	Metadata                map[string]string `yaml:"metadata"`
}

type windowYAML struct {
	Type         string `yaml:"type"`
	DurationMS   int64  `yaml:"duration_ms"`
	BucketCount  int32  `yaml:"bucket_count"`
	CalendarUnit string `yaml:"calendar_unit"`
	Timezone     string `yaml:"timezone"`
}

func (l limitYAML) toProto() *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:                 l.LimitID,
		ScopeKey:                l.ScopeKey,
		Action:                  l.Action,
		Unit:                    l.Unit,
		Algorithm:               parseAlgorithm(l.Algorithm),
		Window:                  l.Window.toProto(),
		Limit:                   l.Limit,
		Burst:                   l.Burst,
		RefillRatePerSec:        l.RefillRatePerSec,
		Refundable:              l.Refundable,
		ReservationExpiryPolicy: parseReservationExpiryPolicy(l.ReservationExpiryPolicy),
		Metadata:                l.Metadata,
	}
}

func (w windowYAML) toProto() *quotav1.Window {
	if w.Type == "" && w.DurationMS == 0 && w.BucketCount == 0 && w.CalendarUnit == "" && w.Timezone == "" {
		return nil
	}
	return &quotav1.Window{
		Type:         parseWindowType(w.Type),
		DurationMs:   w.DurationMS,
		BucketCount:  w.BucketCount,
		CalendarUnit: parseCalendarUnit(w.CalendarUnit),
		Timezone:     w.Timezone,
	}
}

func parseAlgorithm(value string) quotav1.Algorithm {
	if v, ok := quotav1.Algorithm_value[value]; ok {
		return quotav1.Algorithm(v)
	}
	return quotav1.Algorithm_ALGORITHM_UNSPECIFIED
}

func parseWindowType(value string) quotav1.WindowType {
	if v, ok := quotav1.WindowType_value[value]; ok {
		return quotav1.WindowType(v)
	}
	return quotav1.WindowType_WINDOW_TYPE_UNSPECIFIED
}

func parseCalendarUnit(value string) quotav1.CalendarUnit {
	if v, ok := quotav1.CalendarUnit_value[value]; ok {
		return quotav1.CalendarUnit(v)
	}
	return quotav1.CalendarUnit_CALENDAR_UNIT_UNSPECIFIED
}

func parseReservationExpiryPolicy(value string) quotav1.ReservationExpiryPolicy {
	if v, ok := quotav1.ReservationExpiryPolicy_value[value]; ok {
		return quotav1.ReservationExpiryPolicy(v)
	}
	return quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_UNSPECIFIED
}
