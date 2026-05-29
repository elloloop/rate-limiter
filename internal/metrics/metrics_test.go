package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
