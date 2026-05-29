package metrics

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestHandlerExposesDocumentedMetrics(t *testing.T) {
	m := New(nil)
	m.Observe("Consume", "workspace.email.recipients", "workspace", true, "DECISION_REASON_ALLOWED", time.Now().Add(-25*time.Millisecond))
	m.Denial("workspace.email.recipients", "workspace", "user_daily")
	m.RedisError()
	m.IdempotencyHit()
	m.ReservationInc()
	m.ReservationsExpired(2)
	m.LeaseInc()
	m.Overage()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	for _, want := range []string{
		`quota_requests_total{action="workspace.email.recipients",allowed="true",product="workspace",reason="DECISION_REASON_ALLOWED",rpc="Consume"} 1`,
		`quota_request_duration_ms_bucket{action="workspace.email.recipients",product="workspace",rpc="Consume"`,
		`quota_denials_total{action="workspace.email.recipients",limit_id="user_daily",product="workspace"} 1`,
		`quota_redis_errors_total 1`,
		`quota_idempotency_hits_total 1`,
		`quota_reservations_active 1`,
		`quota_reservations_expired_total 2`,
		`quota_leases_active 1`,
		`quota_overages_total 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q\n%s", want, body)
		}
	}
}

func TestHandlerUnavailableWithHostRegisterer(t *testing.T) {
	m := New(prometheus.NewRegistry())

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if !strings.Contains(rec.Body.String(), "host-supplied prometheus.Registerer") {
		t.Fatalf("unexpected body: %q", rec.Body.String())
	}
}

func TestServeExitsWhenContextCanceled(t *testing.T) {
	m := New(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	errs := make(chan error, 1)
	go func() {
		errs <- m.Serve(ctx, addr)
	}()

	waitForMetrics(t, "http://"+addr+"/metrics")
	cancel()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after context cancellation")
	}
}

func waitForMetrics(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:gosec,noctx // local test readiness probe
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("metrics server did not start at %s", url)
}
