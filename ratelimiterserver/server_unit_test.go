package ratelimiterserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	"github.com/elloloop/rate-limiter/internal/keys"
	"github.com/elloloop/rate-limiter/ratelimiterserver/backend"
)

func TestConsumeEmitsDecisionEvent(t *testing.T) {
	limit := unitDurationLimit("event_limit", 10)
	store := &consumeBackend{result: backend.DecisionResult{
		DecisionID:   "decision-1",
		Allowed:      true,
		RetryAfterMS: 0,
		Statuses: []backend.ScriptStatus{{
			LimitID:   limit.GetLimitId(),
			Used:      3,
			Remaining: 7,
			Allowed:   true,
			Message:   "allowed",
		}},
	}}
	events := &captureSink{}
	svc := newUnitService(t, store, events)

	resp, err := svc.Consume(context.Background(), &quotav1.ConsumeRequest{
		RequestId: "req-event",
		Context: &quotav1.RequestContext{
			Product:     "request-product",
			Environment: "request-env",
			Metadata:    map[string]string{"account": "acct_1"},
		},
		Action:  limit.GetAction(),
		Cost:    3,
		Limits:  []*quotav1.Limit{limit},
		Options: &quotav1.RequestOptions{EmitEvent: true},
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !resp.GetDecision().GetAllowed() || resp.GetDecision().GetDecisionId() != "decision-1" {
		t.Fatalf("unexpected decision: %v", resp.GetDecision())
	}
	if store.calls != 1 {
		t.Fatalf("backend consume calls = %d, want 1", store.calls)
	}
	if store.idemKey != keys.Request(keys.Prefix("unit-env", "unit-product"), "req-event") {
		t.Fatalf("unexpected idempotency key: %q", store.idemKey)
	}
	if len(store.ops) != 1 || store.ops[0].Kind != "counter" || store.ops[0].Cost != 3 {
		t.Fatalf("unexpected limit ops: %#v", store.ops)
	}
	if len(events.events) != 1 {
		t.Fatalf("events len = %d, want 1", len(events.events))
	}
	event := events.events[0]
	if event.EventType != "quota.consumed" || event.RequestID != "req-event" || event.DecisionID != "decision-1" {
		t.Fatalf("unexpected event identity: %#v", event)
	}
	if event.Product != "request-product" || event.Environment != "request-env" {
		t.Fatalf("event did not use request context identity: %#v", event)
	}
	if event.Action != limit.GetAction() || event.Unit != "requests" || event.Cost != 3 || !event.Allowed {
		t.Fatalf("unexpected event details: %#v", event)
	}
	if event.Metadata["account"] != "acct_1" {
		t.Fatalf("metadata missing from event: %#v", event.Metadata)
	}
}

func TestConsumeDenialEmitsDeniedEvent(t *testing.T) {
	limit := unitDurationLimit("event_denied_limit", 10)
	store := &consumeBackend{result: backend.DecisionResult{
		DecisionID:   "decision-denied",
		Allowed:      false,
		RetryAfterMS: 500,
		Statuses: []backend.ScriptStatus{{
			LimitID:      limit.GetLimitId(),
			Used:         10,
			Remaining:    0,
			RetryAfterMS: 500,
			Allowed:      false,
			Message:      "limit exceeded",
		}},
	}}
	events := &captureSink{}
	svc := newUnitService(t, store, events)

	resp, err := svc.Consume(context.Background(), &quotav1.ConsumeRequest{
		RequestId: "req-denied-event",
		Action:    limit.GetAction(),
		Cost:      1,
		Limits:    []*quotav1.Limit{limit},
		Options:   &quotav1.RequestOptions{EmitEvent: true},
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if resp.GetDecision().GetAllowed() || resp.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_LIMIT_EXCEEDED {
		t.Fatalf("expected limit exceeded decision, got %v", resp.GetDecision())
	}
	if len(events.events) != 1 {
		t.Fatalf("events len = %d, want 1", len(events.events))
	}
	event := events.events[0]
	if event.EventType != "quota.denied" || event.RequestID != "req-denied-event" || event.Allowed {
		t.Fatalf("unexpected denial event: %#v", event)
	}
}

func TestNilEventSinkDropsRequestedEvent(t *testing.T) {
	limit := unitDurationLimit("nil_event_sink_limit", 10)
	store := &consumeBackend{result: backend.DecisionResult{
		DecisionID: "decision-nil-event",
		Allowed:    true,
		Statuses: []backend.ScriptStatus{{
			LimitID:   limit.GetLimitId(),
			Used:      1,
			Remaining: 9,
			Allowed:   true,
			Message:   "allowed",
		}},
	}}
	svc := newUnitService(t, store, nil)

	resp, err := svc.Consume(context.Background(), &quotav1.ConsumeRequest{
		RequestId: "req-nil-event",
		Action:    limit.GetAction(),
		Cost:      1,
		Limits:    []*quotav1.Limit{limit},
		Options:   &quotav1.RequestOptions{EmitEvent: true},
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !resp.GetDecision().GetAllowed() {
		t.Fatalf("unexpected decision: %v", resp.GetDecision())
	}
	if store.calls != 1 {
		t.Fatalf("backend consume calls = %d, want 1", store.calls)
	}
}

func TestConsumeInvalidCostSkipsBackendAndEvents(t *testing.T) {
	store := &consumeBackend{}
	events := &captureSink{}
	svc := newUnitService(t, store, events)

	resp, err := svc.Consume(context.Background(), &quotav1.ConsumeRequest{
		RequestId: "req-invalid-cost",
		Action:    "test.invalid",
		Cost:      0,
		Limits:    []*quotav1.Limit{unitDurationLimit("invalid_cost", 10)},
		Options:   &quotav1.RequestOptions{EmitEvent: true},
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if resp.GetDecision().GetAllowed() || resp.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST {
		t.Fatalf("expected invalid decision, got %v", resp.GetDecision())
	}
	if store.calls != 0 {
		t.Fatalf("backend consume calls = %d, want 0", store.calls)
	}
	if len(events.events) != 0 {
		t.Fatalf("events emitted for invalid request: %#v", events.events)
	}
}

func TestExplainRejectsNonPositiveCostBeforeUsageLookup(t *testing.T) {
	for _, cost := range []int64{0, -1} {
		t.Run(strings.ReplaceAll("cost_"+strconv.FormatInt(cost, 10), "-", "negative_"), func(t *testing.T) {
			store := &usageBackend{}
			svc := newUnitService(t, store, nil)
			resp, err := svc.Explain(context.Background(), &quotav1.ExplainRequest{
				Action: unitDurationLimit("explain_cost", 10).GetAction(),
				Cost:   cost,
				Limits: []*quotav1.Limit{unitDurationLimit("explain_cost", 10)},
			})
			if err != nil {
				t.Fatalf("Explain: %v", err)
			}
			if resp.GetWouldAllow() || resp.GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST {
				t.Fatalf("expected invalid explain response, got %v", resp)
			}
			if !strings.Contains(resp.GetMessage(), "cost") {
				t.Fatalf("invalid explain response should mention cost: %v", resp)
			}
			if store.counterCalls != 0 {
				t.Fatalf("usage lookups = %d, want 0", store.counterCalls)
			}
		})
	}
}

func TestExplainProjectsCurrentUsage(t *testing.T) {
	limit := unitDurationLimit("explain_usage", 10)
	store := &usageBackend{counterValue: 7}
	svc := newUnitService(t, store, nil)

	resp, err := svc.Explain(context.Background(), &quotav1.ExplainRequest{
		Action: limit.GetAction(),
		Cost:   4,
		Limits: []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if resp.GetWouldAllow() || resp.GetReason() != quotav1.DecisionReason_DECISION_REASON_LIMIT_EXCEEDED {
		t.Fatalf("expected limit exceeded response, got %v", resp)
	}
	if len(resp.GetEvaluations()) != 1 {
		t.Fatalf("evaluations len = %d, want 1", len(resp.GetEvaluations()))
	}
	status := resp.GetEvaluations()[0].GetCurrentStatus()
	if status.GetUsed() != 7 || status.GetRemaining() != 3 || status.GetCost() != 4 || status.GetAllowed() {
		t.Fatalf("unexpected projected status: %v", status)
	}
	if store.counterCalls != 1 {
		t.Fatalf("usage lookups = %d, want 1", store.counterCalls)
	}
}

func TestGetRedisStatusScriptStates(t *testing.T) {
	tests := []struct {
		name          string
		store         *healthBackend
		wantReachable bool
		wantMessage   string
		wantLoadCalls int
	}{
		{
			name:          "loaded",
			store:         &healthBackend{loaded: []bool{true}},
			wantReachable: true,
			wantMessage:   "ok",
		},
		{
			name:          "reloads_missing_scripts",
			store:         &healthBackend{loaded: []bool{false, true}},
			wantReachable: true,
			wantMessage:   "ok",
			wantLoadCalls: 1,
		},
		{
			name:          "reports_reload_error",
			store:         &healthBackend{loaded: []bool{false}, loadErr: errors.New("load failed")},
			wantReachable: true,
			wantMessage:   "reload Redis scripts: load failed",
			wantLoadCalls: 1,
		},
		{
			name:          "reports_ping_error",
			store:         &healthBackend{pingErr: errors.New("dial failed")},
			wantReachable: false,
			wantMessage:   "dial failed",
		},
		{
			name:          "reports_scripts_still_missing",
			store:         &healthBackend{loaded: []bool{false, false}},
			wantReachable: true,
			wantMessage:   "one or more Redis scripts are not loaded",
			wantLoadCalls: 1,
		},
		{
			name:          "reports_script_check_error",
			store:         &healthBackend{loadedErrs: []error{errors.New("script exists failed")}},
			wantReachable: false,
			wantMessage:   "script exists failed",
		},
		{
			name:          "reports_post_reload_script_check_error",
			store:         &healthBackend{loaded: []bool{false}, loadedErrs: []error{nil, errors.New("post reload script check failed")}},
			wantReachable: false,
			wantMessage:   "post reload script check failed",
			wantLoadCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newUnitService(t, tt.store, nil)
			resp, err := svc.GetRedisStatus(context.Background(), &quotav1.GetRedisStatusRequest{})
			if err != nil {
				t.Fatalf("GetRedisStatus: %v", err)
			}
			if resp.GetReachable() != tt.wantReachable || resp.GetMessage() != tt.wantMessage {
				t.Fatalf("status = %v, want reachable=%v message=%q", resp, tt.wantReachable, tt.wantMessage)
			}
			if resp.GetMode() != "single_primary" {
				t.Fatalf("mode = %q, want single_primary", resp.GetMode())
			}
			if tt.store.loadCalls != tt.wantLoadCalls {
				t.Fatalf("load calls = %d, want %d", tt.store.loadCalls, tt.wantLoadCalls)
			}
		})
	}
}

func TestReadRPCsMapBackendNotFound(t *testing.T) {
	store := &readBackend{}
	svc := newUnitService(t, store, nil)

	_, err := svc.GetReservation(context.Background(), &quotav1.GetReservationRequest{ReservationId: "missing-reservation"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetReservation code = %s, want NotFound; err=%v", status.Code(err), err)
	}

	_, err = svc.GetLease(context.Background(), &quotav1.GetLeaseRequest{LeaseId: "missing-lease"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetLease code = %s, want NotFound; err=%v", status.Code(err), err)
	}
}

func TestReadRPCsReturnStoredValues(t *testing.T) {
	store := &readResultBackend{
		reservation: &quotav1.Reservation{ReservationId: "reservation-1", Status: quotav1.ReservationStatus_RESERVATION_STATUS_ACTIVE},
		lease:       &quotav1.Lease{LeaseId: "lease-1", Status: quotav1.LeaseStatus_LEASE_STATUS_ACTIVE},
	}
	svc := newUnitService(t, store, nil)

	reservation, err := svc.GetReservation(context.Background(), &quotav1.GetReservationRequest{ReservationId: "reservation-1"})
	if err != nil {
		t.Fatalf("GetReservation: %v", err)
	}
	if reservation.GetReservationId() != "reservation-1" {
		t.Fatalf("unexpected reservation: %v", reservation)
	}

	lease, err := svc.GetLease(context.Background(), &quotav1.GetLeaseRequest{LeaseId: "lease-1"})
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if lease.GetLeaseId() != "lease-1" {
		t.Fatalf("unexpected lease: %v", lease)
	}
}

func TestMetricsReturnsCollector(t *testing.T) {
	svc := newUnitService(t, &healthBackend{loaded: []bool{true}}, nil)
	if svc.Metrics() == nil {
		t.Fatal("Metrics returned nil")
	}
}

func TestValidationDecisionFromInvalidLimits(t *testing.T) {
	limit := unitDurationLimit("mismatch", 10)
	limit.Action = "different.action"
	svc := newUnitService(t, &consumeBackend{}, nil)

	resp, err := svc.Consume(context.Background(), &quotav1.ConsumeRequest{
		RequestId: "req-invalid-limits",
		Action:    "test.mismatch",
		Cost:      1,
		Limits:    []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if resp.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST {
		t.Fatalf("reason = %s, want invalid request", resp.GetDecision().GetReason())
	}
	if !strings.Contains(resp.GetDecision().GetMessage(), "action") {
		t.Fatalf("validation message should mention action: %v", resp.GetDecision())
	}
}

func TestValidationDecisionBranches(t *testing.T) {
	tests := []struct {
		name string
		call func(*Server) (*quotav1.Decision, error)
	}{
		{
			name: "reserve_invalid_limits",
			call: func(svc *Server) (*quotav1.Decision, error) {
				limit := unitDurationLimit("reserve_mismatch", 10)
				limit.Action = "different.action"
				resp, err := svc.Reserve(context.Background(), &quotav1.ReserveRequest{
					RequestId:        "req-reserve-mismatch",
					Action:           "test.reserve_mismatch",
					ReserveCost:      1,
					ReservationTtlMs: 1000,
					Limits:           []*quotav1.Limit{limit},
				})
				return resp.GetDecision(), err
			},
		},
		{
			name: "acquire_lease_invalid_limits",
			call: func(svc *Server) (*quotav1.Decision, error) {
				limit := unitConcurrencyLimit("lease_mismatch", 1)
				limit.Action = "different.action"
				resp, err := svc.AcquireLease(context.Background(), &quotav1.AcquireLeaseRequest{
					RequestId:  "req-lease-mismatch",
					Action:     "test.lease_mismatch",
					LeaseTtlMs: 1000,
					Limits:     []*quotav1.Limit{limit},
				})
				return resp.GetDecision(), err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := tt.call(newUnitService(t, &countingBackend{}, nil))
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			if decision.GetAllowed() || decision.GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST {
				t.Fatalf("expected invalid decision, got %v", decision)
			}
			if !strings.Contains(decision.GetMessage(), "action") {
				t.Fatalf("validation message should mention action: %v", decision)
			}
		})
	}
}

func TestReserveRejectsConcurrencyLimits(t *testing.T) {
	limit := unitConcurrencyLimit("reserve_concurrency", 1)
	svc := newUnitService(t, &countingBackend{}, nil)

	resp, err := svc.Reserve(context.Background(), &quotav1.ReserveRequest{
		RequestId:        "req-reserve-concurrency",
		Action:           limit.GetAction(),
		ReserveCost:      1,
		ReservationTtlMs: 1000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if resp.GetDecision().GetAllowed() || resp.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST {
		t.Fatalf("expected invalid decision, got %v", resp.GetDecision())
	}
	if !strings.Contains(resp.GetDecision().GetMessage(), "Consume/Reserve do not accept") {
		t.Fatalf("message = %q, want Consume/Reserve rejection", resp.GetDecision().GetMessage())
	}
}

func TestRPCValidationFailuresSkipBackend(t *testing.T) {
	svc := newUnitService(t, &countingBackend{}, nil)

	errorCases := []struct {
		name string
		call func() error
	}{
		{
			name: "consume_missing_request_id",
			call: func() error {
				_, err := svc.Consume(context.Background(), &quotav1.ConsumeRequest{Cost: 1, Limits: []*quotav1.Limit{unitDurationLimit("consume_id", 10)}})
				return err
			},
		},
		{
			name: "reserve_missing_request_id",
			call: func() error {
				_, err := svc.Reserve(context.Background(), &quotav1.ReserveRequest{ReserveCost: 1, ReservationTtlMs: 1000, Limits: []*quotav1.Limit{unitDurationLimit("reserve_id", 10)}})
				return err
			},
		},
		{
			name: "increment_missing_request_id",
			call: func() error {
				_, err := svc.IncrementReservation(context.Background(), &quotav1.IncrementReservationRequest{ReservationId: "res-1", DeltaCost: 1})
				return err
			},
		},
		{
			name: "increment_missing_reservation_id",
			call: func() error {
				_, err := svc.IncrementReservation(context.Background(), &quotav1.IncrementReservationRequest{RequestId: "req-1", DeltaCost: 1})
				return err
			},
		},
		{
			name: "finalize_missing_request_id",
			call: func() error {
				_, err := svc.FinalizeReservation(context.Background(), &quotav1.FinalizeReservationRequest{ReservationId: "res-1", ActualCost: 1})
				return err
			},
		},
		{
			name: "finalize_missing_reservation_id",
			call: func() error {
				_, err := svc.FinalizeReservation(context.Background(), &quotav1.FinalizeReservationRequest{RequestId: "req-1", ActualCost: 1})
				return err
			},
		},
		{
			name: "release_reservation_missing_request_id",
			call: func() error {
				_, err := svc.ReleaseReservation(context.Background(), &quotav1.ReleaseReservationRequest{ReservationId: "res-1"})
				return err
			},
		},
		{
			name: "release_reservation_missing_reservation_id",
			call: func() error {
				_, err := svc.ReleaseReservation(context.Background(), &quotav1.ReleaseReservationRequest{RequestId: "req-1"})
				return err
			},
		},
		{
			name: "acquire_lease_missing_request_id",
			call: func() error {
				_, err := svc.AcquireLease(context.Background(), &quotav1.AcquireLeaseRequest{LeaseTtlMs: 1000, Limits: []*quotav1.Limit{unitConcurrencyLimit("lease_id", 1)}})
				return err
			},
		},
		{
			name: "renew_lease_missing_request_id",
			call: func() error {
				_, err := svc.RenewLease(context.Background(), &quotav1.RenewLeaseRequest{LeaseId: "lease-1", ExtendTtlMs: 1000})
				return err
			},
		},
		{
			name: "renew_lease_missing_lease_id",
			call: func() error {
				_, err := svc.RenewLease(context.Background(), &quotav1.RenewLeaseRequest{RequestId: "req-1", ExtendTtlMs: 1000})
				return err
			},
		},
		{
			name: "renew_lease_invalid_extend_ttl",
			call: func() error {
				_, err := svc.RenewLease(context.Background(), &quotav1.RenewLeaseRequest{RequestId: "req-1", LeaseId: "lease-1"})
				return err
			},
		},
		{
			name: "release_lease_missing_request_id",
			call: func() error {
				_, err := svc.ReleaseLease(context.Background(), &quotav1.ReleaseLeaseRequest{LeaseId: "lease-1"})
				return err
			},
		},
		{
			name: "release_lease_missing_lease_id",
			call: func() error {
				_, err := svc.ReleaseLease(context.Background(), &quotav1.ReleaseLeaseRequest{RequestId: "req-1"})
				return err
			},
		},
		{
			name: "get_reservation_missing_id",
			call: func() error {
				_, err := svc.GetReservation(context.Background(), &quotav1.GetReservationRequest{})
				return err
			},
		},
		{
			name: "get_lease_missing_id",
			call: func() error {
				_, err := svc.GetLease(context.Background(), &quotav1.GetLeaseRequest{})
				return err
			},
		},
	}

	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			if code := status.Code(tt.call()); code != codes.InvalidArgument {
				t.Fatalf("status code = %s, want InvalidArgument", code)
			}
		})
	}

	decisionCases := []struct {
		name        string
		call        func() *quotav1.Decision
		wantMessage string
	}{
		{
			name: "consume_non_positive_cost",
			call: func() *quotav1.Decision {
				resp, err := svc.Consume(context.Background(), &quotav1.ConsumeRequest{RequestId: "req-consume-cost", Cost: -1, Limits: []*quotav1.Limit{unitDurationLimit("consume_cost", 10)}})
				if err != nil {
					t.Fatalf("Consume: %v", err)
				}
				return resp.GetDecision()
			},
			wantMessage: "cost must be greater than zero",
		},
		{
			name: "reserve_non_positive_cost",
			call: func() *quotav1.Decision {
				resp, err := svc.Reserve(context.Background(), &quotav1.ReserveRequest{RequestId: "req-reserve-cost", ReserveCost: 0, ReservationTtlMs: 1000, Limits: []*quotav1.Limit{unitDurationLimit("reserve_cost", 10)}})
				if err != nil {
					t.Fatalf("Reserve: %v", err)
				}
				return resp.GetDecision()
			},
			wantMessage: "reserve_cost must be greater than zero",
		},
		{
			name: "reserve_non_positive_ttl",
			call: func() *quotav1.Decision {
				resp, err := svc.Reserve(context.Background(), &quotav1.ReserveRequest{RequestId: "req-reserve-ttl", ReserveCost: 1, Limits: []*quotav1.Limit{unitDurationLimit("reserve_ttl", 10)}})
				if err != nil {
					t.Fatalf("Reserve: %v", err)
				}
				return resp.GetDecision()
			},
			wantMessage: "reservation_ttl_ms must be greater than zero",
		},
		{
			name: "increment_zero_delta",
			call: func() *quotav1.Decision {
				resp, err := svc.IncrementReservation(context.Background(), &quotav1.IncrementReservationRequest{RequestId: "req-increment-delta", ReservationId: "res-1"})
				if err != nil {
					t.Fatalf("IncrementReservation: %v", err)
				}
				return resp.GetDecision()
			},
			wantMessage: "delta_cost must be non-zero",
		},
		{
			name: "acquire_lease_non_positive_ttl",
			call: func() *quotav1.Decision {
				resp, err := svc.AcquireLease(context.Background(), &quotav1.AcquireLeaseRequest{RequestId: "req-lease-ttl", Limits: []*quotav1.Limit{unitConcurrencyLimit("lease_ttl", 1)}})
				if err != nil {
					t.Fatalf("AcquireLease: %v", err)
				}
				return resp.GetDecision()
			},
			wantMessage: "lease_ttl_ms must be greater than zero",
		},
	}

	for _, tt := range decisionCases {
		t.Run(tt.name, func(t *testing.T) {
			decision := tt.call()
			if decision.GetAllowed() || decision.GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST {
				t.Fatalf("expected invalid decision, got %v", decision)
			}
			if !strings.Contains(decision.GetMessage(), tt.wantMessage) {
				t.Fatalf("message = %q, want to contain %q", decision.GetMessage(), tt.wantMessage)
			}
		})
	}
}

func TestBackendErrorsMapToUnavailable(t *testing.T) {
	backendErr := errors.New("backend down")
	svc := newUnitService(t, errorBackend{err: backendErr}, nil)

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "consume",
			call: func() error {
				_, err := svc.Consume(context.Background(), &quotav1.ConsumeRequest{RequestId: "req-consume", Action: "test.consume_error", Cost: 1, Limits: []*quotav1.Limit{unitDurationLimit("consume_error", 10)}})
				return err
			},
		},
		{
			name: "reserve",
			call: func() error {
				_, err := svc.Reserve(context.Background(), &quotav1.ReserveRequest{RequestId: "req-reserve", Action: "test.reserve_error", ReserveCost: 1, ReservationTtlMs: 1000, Limits: []*quotav1.Limit{unitDurationLimit("reserve_error", 10)}})
				return err
			},
		},
		{
			name: "increment_reservation",
			call: func() error {
				_, err := svc.IncrementReservation(context.Background(), &quotav1.IncrementReservationRequest{RequestId: "req-increment", ReservationId: "res-1", DeltaCost: 1})
				return err
			},
		},
		{
			name: "finalize_reservation",
			call: func() error {
				_, err := svc.FinalizeReservation(context.Background(), &quotav1.FinalizeReservationRequest{RequestId: "req-finalize", ReservationId: "res-1", ActualCost: 1})
				return err
			},
		},
		{
			name: "release_reservation",
			call: func() error {
				_, err := svc.ReleaseReservation(context.Background(), &quotav1.ReleaseReservationRequest{RequestId: "req-release", ReservationId: "res-1"})
				return err
			},
		},
		{
			name: "acquire_lease",
			call: func() error {
				limit := unitConcurrencyLimit("lease_error", 1)
				_, err := svc.AcquireLease(context.Background(), &quotav1.AcquireLeaseRequest{RequestId: "req-acquire", Action: limit.GetAction(), Limits: []*quotav1.Limit{limit}, LeaseTtlMs: 1000})
				return err
			},
		},
		{
			name: "renew_lease",
			call: func() error {
				_, err := svc.RenewLease(context.Background(), &quotav1.RenewLeaseRequest{RequestId: "req-renew", LeaseId: "lease-1", ExtendTtlMs: 1000})
				return err
			},
		},
		{
			name: "release_lease",
			call: func() error {
				_, err := svc.ReleaseLease(context.Background(), &quotav1.ReleaseLeaseRequest{RequestId: "req-release-lease", LeaseId: "lease-1"})
				return err
			},
		},
		{
			name: "get_reservation",
			call: func() error {
				_, err := svc.GetReservation(context.Background(), &quotav1.GetReservationRequest{ReservationId: "res-1"})
				return err
			},
		},
		{
			name: "get_lease",
			call: func() error {
				_, err := svc.GetLease(context.Background(), &quotav1.GetLeaseRequest{LeaseId: "lease-1"})
				return err
			},
		},
		{
			name: "get_current_usage",
			call: func() error {
				limit := unitDurationLimit("usage_error", 10)
				_, err := svc.GetCurrentUsage(context.Background(), &quotav1.GetCurrentUsageRequest{Action: limit.GetAction(), Limits: []*quotav1.Limit{limit}})
				return err
			},
		},
		{
			name: "explain",
			call: func() error {
				limit := unitDurationLimit("explain_error", 10)
				_, err := svc.Explain(context.Background(), &quotav1.ExplainRequest{Action: limit.GetAction(), Cost: 1, Limits: []*quotav1.Limit{limit}})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("status code = %s, want Unavailable; err=%v", status.Code(err), err)
			}
			if !strings.Contains(err.Error(), backendErr.Error()) {
				t.Fatalf("error %q should include backend failure %q", err.Error(), backendErr.Error())
			}
		})
	}

	expired, err := svc.ExpireReservations(context.Background(), 0)
	if expired != 0 || !errors.Is(err, backendErr) {
		t.Fatalf("ExpireReservations = expired %d err %v, want 0/backendErr", expired, err)
	}
}

func TestReservationLifecycleResultStates(t *testing.T) {
	tests := []struct {
		name     string
		store    backend.Backend
		call     func(*Server) error
		wantCode codes.Code
	}{
		{
			name:  "increment_missing",
			store: &reservationBackend{incrementResult: backend.IncrementReservationResult{Found: false}},
			call: func(svc *Server) error {
				_, err := svc.IncrementReservation(context.Background(), &quotav1.IncrementReservationRequest{
					RequestId:     "req-increment-missing",
					ReservationId: "reservation-missing",
					DeltaCost:     1,
				})
				return err
			},
			wantCode: codes.NotFound,
		},
		{
			name:  "increment_inactive",
			store: &reservationBackend{incrementResult: backend.IncrementReservationResult{Found: true, Active: false}},
			call: func(svc *Server) error {
				_, err := svc.IncrementReservation(context.Background(), &quotav1.IncrementReservationRequest{
					RequestId:     "req-increment-inactive",
					ReservationId: "reservation-inactive",
					DeltaCost:     1,
				})
				return err
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:  "finalize_negative_actual_cost",
			store: &countingBackend{},
			call: func(svc *Server) error {
				_, err := svc.FinalizeReservation(context.Background(), &quotav1.FinalizeReservationRequest{
					RequestId:     "req-finalize-negative",
					ReservationId: "reservation-active",
					ActualCost:    -1,
				})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name:  "finalize_missing",
			store: &reservationBackend{finalizeResult: backend.FinalizeResult{Found: false}},
			call: func(svc *Server) error {
				_, err := svc.FinalizeReservation(context.Background(), &quotav1.FinalizeReservationRequest{
					RequestId:     "req-finalize-missing",
					ReservationId: "reservation-missing",
					ActualCost:    1,
				})
				return err
			},
			wantCode: codes.NotFound,
		},
		{
			name:  "release_missing",
			store: &reservationBackend{releaseResult: backend.ReleaseReservationResult{Found: false}},
			call: func(svc *Server) error {
				_, err := svc.ReleaseReservation(context.Background(), &quotav1.ReleaseReservationRequest{
					RequestId:     "req-release-missing",
					ReservationId: "reservation-missing",
				})
				return err
			},
			wantCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(newUnitService(t, tt.store, nil))
			if status.Code(err) != tt.wantCode {
				t.Fatalf("status code = %s, want %s; err=%v", status.Code(err), tt.wantCode, err)
			}
		})
	}
}

func TestDecisionFromReservationIncrementDefaultsDeniedMessage(t *testing.T) {
	svc := newUnitService(t, &countingBackend{}, nil)

	decision := svc.decisionFromReservationIncrement(backend.DecisionResult{
		Allowed: false,
		Statuses: []backend.ScriptStatus{{
			LimitID:   "increment_limit",
			Allowed:   false,
			Message:   "limit exceeded",
			Remaining: 0,
		}},
	}, &quotav1.Reservation{
		Action: "test.increment",
		Impacts: []*quotav1.ReservationImpact{{
			LimitId:   "increment_limit",
			ScopeKey:  "scope:increment",
			Unit:      "requests",
			Algorithm: quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION,
			Limit:     10,
		}},
	}, 3)
	if decision.GetAllowed() || decision.GetReason() != quotav1.DecisionReason_DECISION_REASON_LIMIT_EXCEEDED {
		t.Fatalf("expected limit exceeded decision, got %v", decision)
	}
	if decision.GetMessage() != "increment reservation denied" {
		t.Fatalf("message = %q, want denied default", decision.GetMessage())
	}

	invalid := svc.decisionFromReservationIncrement(backend.DecisionResult{Allowed: false}, nil, 3)
	if invalid.GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST || invalid.GetMessage() != "increment reservation denied" {
		t.Fatalf("unexpected invalid increment decision: %v", invalid)
	}

	custom := svc.decisionFromReservationIncrement(backend.DecisionResult{Allowed: false, Message: "reservation is finalized"}, nil, 3)
	if custom.GetMessage() != "reservation is finalized" {
		t.Fatalf("custom message was not preserved: %v", custom)
	}
}

func TestReserveCachedReplayFetchesReservation(t *testing.T) {
	limit := unitDurationLimit("reserve_cached", 10)
	store := &reserveBackend{
		result: backend.DecisionResult{
			Cached:        true,
			DecisionID:    "decision-reserve-cached",
			Allowed:       true,
			ReservationID: "reservation-cached",
			Statuses: []backend.ScriptStatus{{
				LimitID:   limit.GetLimitId(),
				Used:      2,
				Remaining: 8,
				Allowed:   true,
			}},
		},
		reservation: &quotav1.Reservation{
			ReservationId: "reservation-cached",
			Action:        limit.GetAction(),
			ReservedCost:  2,
			Status:        quotav1.ReservationStatus_RESERVATION_STATUS_ACTIVE,
		},
	}
	svc := newUnitService(t, store, nil)

	resp, err := svc.Reserve(context.Background(), &quotav1.ReserveRequest{
		RequestId:        "req-reserve-cached",
		Action:           limit.GetAction(),
		ReserveCost:      2,
		ReservationTtlMs: 1000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if resp.GetReservation().GetReservationId() != "reservation-cached" {
		t.Fatalf("cached reserve did not fetch reservation: %v", resp)
	}
	if resp.GetDecision().GetMetadata()["idempotency_hit"] != "true" {
		t.Fatalf("cached reserve missing idempotency metadata: %v", resp.GetDecision())
	}
}

func TestReserveDenialSuppressesReservationAndEmitsDenialEvent(t *testing.T) {
	limit := unitDurationLimit("reserve_denied", 10)
	events := &captureSink{}
	svc := newUnitService(t, &reserveBackend{result: backend.DecisionResult{
		DecisionID: "decision-reserve-denied",
		Allowed:    false,
		Statuses: []backend.ScriptStatus{{
			LimitID:   limit.GetLimitId(),
			Used:      10,
			Remaining: 0,
			Allowed:   false,
		}},
	}}, events)

	resp, err := svc.Reserve(context.Background(), &quotav1.ReserveRequest{
		RequestId:        "req-reserve-denied",
		Action:           limit.GetAction(),
		ReserveCost:      1,
		ReservationTtlMs: 1000,
		Limits:           []*quotav1.Limit{limit},
		Options:          &quotav1.RequestOptions{EmitEvent: true},
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if resp.GetDecision().GetAllowed() || resp.GetReservation() != nil {
		t.Fatalf("denied reserve should not return reservation: %v", resp)
	}
	if len(events.events) != 1 || events.events[0].EventType != "quota.denied" || events.events[0].Reservation != nil {
		t.Fatalf("unexpected denial event: %#v", events.events)
	}
}

func TestAcquireLeaseCachedReplayFetchesLease(t *testing.T) {
	limit := unitConcurrencyLimit("lease_cached", 1)
	store := &leaseBackend{
		acquireResult: backend.DecisionResult{
			Cached:     true,
			DecisionID: "decision-lease-cached",
			Allowed:    true,
			LeaseID:    "lease-cached",
			Statuses: []backend.ScriptStatus{{
				LimitID:   limit.GetLimitId(),
				Used:      1,
				Remaining: 0,
				Allowed:   true,
			}},
		},
		lease: &quotav1.Lease{
			LeaseId: "lease-cached",
			Action:  limit.GetAction(),
			Status:  quotav1.LeaseStatus_LEASE_STATUS_ACTIVE,
		},
	}
	svc := newUnitService(t, store, nil)

	resp, err := svc.AcquireLease(context.Background(), &quotav1.AcquireLeaseRequest{
		RequestId:  "req-lease-cached",
		Action:     limit.GetAction(),
		LeaseTtlMs: 1000,
		Limits:     []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if resp.GetLease().GetLeaseId() != "lease-cached" {
		t.Fatalf("cached acquire did not fetch lease: %v", resp)
	}
	if resp.GetDecision().GetMetadata()["idempotency_hit"] != "true" {
		t.Fatalf("cached acquire missing idempotency metadata: %v", resp.GetDecision())
	}
}

func TestAcquireLeaseDenialSuppressesLeaseAndEmitsDenialEvent(t *testing.T) {
	limit := unitConcurrencyLimit("lease_denied", 1)
	events := &captureSink{}
	svc := newUnitService(t, &leaseBackend{acquireResult: backend.DecisionResult{
		DecisionID: "decision-lease-denied",
		Allowed:    false,
		Statuses: []backend.ScriptStatus{{
			LimitID:   limit.GetLimitId(),
			Used:      1,
			Remaining: 0,
			Allowed:   false,
		}},
	}}, events)

	resp, err := svc.AcquireLease(context.Background(), &quotav1.AcquireLeaseRequest{
		RequestId:  "req-lease-denied",
		Action:     limit.GetAction(),
		LeaseTtlMs: 1000,
		Limits:     []*quotav1.Limit{limit},
		Options:    &quotav1.RequestOptions{EmitEvent: true},
	})
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if resp.GetDecision().GetAllowed() || resp.GetLease() != nil {
		t.Fatalf("denied acquire should not return lease: %v", resp)
	}
	if len(events.events) != 1 || events.events[0].EventType != "quota.denied" || events.events[0].Lease != nil {
		t.Fatalf("unexpected denial event: %#v", events.events)
	}
}

func TestRenewLeaseResultStates(t *testing.T) {
	tests := []struct {
		name     string
		result   backend.LeaseResult
		wantCode codes.Code
	}{
		{
			name:     "missing",
			result:   backend.LeaseResult{Found: false},
			wantCode: codes.NotFound,
		},
		{
			name:     "not_active",
			result:   backend.LeaseResult{Found: true, Renewed: false, Lease: &quotav1.Lease{LeaseId: "lease-1", Status: quotav1.LeaseStatus_LEASE_STATUS_RELEASED}},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "renewed",
			result:   backend.LeaseResult{Cached: true, Found: true, Renewed: true, Lease: &quotav1.Lease{LeaseId: "lease-1", Status: quotav1.LeaseStatus_LEASE_STATUS_ACTIVE}},
			wantCode: codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newUnitService(t, &leaseBackend{renewResult: tt.result}, nil)
			resp, err := svc.RenewLease(context.Background(), &quotav1.RenewLeaseRequest{
				RequestId:   "req-renew-" + tt.name,
				LeaseId:     "lease-1",
				ExtendTtlMs: 1000,
			})
			if tt.wantCode != codes.OK {
				if status.Code(err) != tt.wantCode {
					t.Fatalf("code = %s, want %s; err=%v", status.Code(err), tt.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("RenewLease: %v", err)
			}
			if !resp.GetRenewed() || resp.GetLease().GetLeaseId() != "lease-1" {
				t.Fatalf("unexpected renew response: %v", resp)
			}
		})
	}
}

func TestReleaseLeaseNotFound(t *testing.T) {
	svc := newUnitService(t, &leaseBackend{releaseResult: backend.LeaseResult{Found: false}}, nil)
	_, err := svc.ReleaseLease(context.Background(), &quotav1.ReleaseLeaseRequest{
		RequestId: "req-release-missing",
		LeaseId:   "lease-missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %s, want NotFound; err=%v", status.Code(err), err)
	}
}

func TestExplainValidationAndAllowedPath(t *testing.T) {
	invalid := unitDurationLimit("explain_invalid", 10)
	invalid.Action = "different.action"
	svc := newUnitService(t, &usageBackend{}, nil)

	invalidResp, err := svc.Explain(context.Background(), &quotav1.ExplainRequest{
		Action: "test.explain_invalid",
		Cost:   1,
		Limits: []*quotav1.Limit{invalid},
	})
	if err != nil {
		t.Fatalf("Explain invalid: %v", err)
	}
	if invalidResp.GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST || len(invalidResp.GetEvaluations()[0].GetValidationErrors()) == 0 {
		t.Fatalf("unexpected invalid explain response: %v", invalidResp)
	}

	limit := unitDurationLimit("explain_allowed", 10)
	allowedSvc := newUnitService(t, &usageBackend{counterValue: 3}, nil)
	allowedResp, err := allowedSvc.Explain(context.Background(), &quotav1.ExplainRequest{
		Action: limit.GetAction(),
		Cost:   4,
		Limits: []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("Explain allowed: %v", err)
	}
	if !allowedResp.GetWouldAllow() || allowedResp.GetReason() != quotav1.DecisionReason_DECISION_REASON_ALLOWED {
		t.Fatalf("unexpected allowed explain response: %v", allowedResp)
	}
	if got := allowedResp.GetEvaluations()[0].GetCurrentStatus().GetRemaining(); got != 7 {
		t.Fatalf("remaining = %d, want 7", got)
	}
}

func TestGetCurrentUsageRejectsInvalidLimits(t *testing.T) {
	limit := unitDurationLimit("usage_invalid", 10)
	limit.Action = "different.action"
	svc := newUnitService(t, &usageBackend{}, nil)

	_, err := svc.GetCurrentUsage(context.Background(), &quotav1.GetCurrentUsageRequest{
		Action: "test.usage_invalid",
		Limits: []*quotav1.Limit{limit},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status code = %s, want InvalidArgument; err=%v", status.Code(err), err)
	}
}

func TestCurrentUsageProjectsAllAlgorithms(t *testing.T) {
	now := time.UnixMilli(180000)
	token := unitContinuousLimit("token_usage", quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET, 10, 10, 2)
	leaky := unitContinuousLimit("leaky_usage", quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET, 10, 10, 2)
	gcra := unitContinuousLimit("gcra_usage", quotav1.Algorithm_ALGORITHM_GCRA, 1, 1, 1)
	concurrency := &quotav1.Limit{
		LimitId:   "concurrency_usage",
		ScopeKey:  "scope:concurrency_usage",
		Action:    "test.concurrency_usage",
		Unit:      "requests",
		Algorithm: quotav1.Algorithm_ALGORITHM_CONCURRENCY,
		Limit:     3,
	}
	store := &usageBackend{
		counterValue:      4,
		bucketState:       backend.BucketState{Tokens: 4, LastRefillMs: now.Add(-time.Second).UnixMilli(), Exists: true},
		gcraValue:         float64(now.Add(2 * time.Second).UnixMilli()),
		gcraExists:        true,
		concurrencyActive: 2,
	}
	svc := newUnitService(t, store, nil)

	statuses, err := svc.currentUsage(context.Background(), []*quotav1.Limit{
		unitDurationLimit("duration_usage", 10),
		unitSlidingLimit("sliding_usage", 10),
		token,
		leaky,
		gcra,
		concurrency,
	}, now)
	if err != nil {
		t.Fatalf("currentUsage: %v", err)
	}
	if len(statuses) != 6 {
		t.Fatalf("statuses len = %d, want 6", len(statuses))
	}
	if statuses[0].GetUsed() != 4 || statuses[0].GetRemaining() != 6 {
		t.Fatalf("duration status mismatch: %v", statuses[0])
	}
	if statuses[1].GetUsed() != 24 || statuses[1].GetRemaining() != 0 {
		t.Fatalf("sliding status mismatch: %v", statuses[1])
	}
	if statuses[2].GetUsed() != 4 || statuses[2].GetRemaining() != 6 {
		t.Fatalf("token bucket status mismatch: %v", statuses[2])
	}
	if statuses[3].GetUsed() != 4 || statuses[3].GetRemaining() != 6 {
		t.Fatalf("leaky bucket status mismatch: %v", statuses[3])
	}
	if statuses[4].GetRemaining() != 0 || statuses[4].GetRetryAfterMs() <= 0 {
		t.Fatalf("GCRA status mismatch: %v", statuses[4])
	}
	if statuses[5].GetUsed() != 2 || statuses[5].GetRemaining() != 1 {
		t.Fatalf("concurrency status mismatch: %v", statuses[5])
	}
}

func TestCurrentUsagePropagatesReadErrors(t *testing.T) {
	now := time.UnixMilli(180000)
	readErr := errors.New("read failed")
	tests := []struct {
		name  string
		store *usageBackend
		limit *quotav1.Limit
	}{
		{
			name:  "fixed_calendar",
			store: &usageBackend{counterErr: readErr},
			limit: unitCalendarLimit("calendar_read_error", 10),
		},
		{
			name:  "fixed_duration",
			store: &usageBackend{counterErr: readErr},
			limit: unitDurationLimit("duration_read_error", 10),
		},
		{
			name:  "sliding_window",
			store: &usageBackend{counterErr: readErr},
			limit: unitSlidingLimit("sliding_read_error", 10),
		},
		{
			name:  "token_bucket",
			store: &usageBackend{bucketErr: readErr},
			limit: unitContinuousLimit("token_read_error", quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET, 10, 10, 1),
		},
		{
			name:  "leaky_bucket",
			store: &usageBackend{bucketErr: readErr},
			limit: unitContinuousLimit("leaky_read_error", quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET, 10, 10, 1),
		},
		{
			name:  "gcra",
			store: &usageBackend{gcraErr: readErr},
			limit: unitContinuousLimit("gcra_read_error", quotav1.Algorithm_ALGORITHM_GCRA, 10, 10, 1),
		},
		{
			name:  "concurrency",
			store: &usageBackend{concurrencyErr: readErr},
			limit: unitConcurrencyLimit("concurrency_read_error", 10),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newUnitService(t, tt.store, nil)
			_, err := svc.currentUsage(context.Background(), []*quotav1.Limit{tt.limit}, now)
			if !errors.Is(err, readErr) {
				t.Fatalf("currentUsage err = %v, want %v", err, readErr)
			}
		})
	}
}

func TestBuildLimitOpsRejectsAlgorithmsForWrongRPCMode(t *testing.T) {
	now := time.UnixMilli(180000)
	svc := newUnitService(t, &countingBackend{}, nil)
	consumeOnly := []*quotav1.Limit{
		unitCalendarLimit("calendar_not_lease", 10),
		unitDurationLimit("duration_not_lease", 10),
		unitSlidingLimit("sliding_not_lease", 10),
		unitContinuousLimit("token_not_lease", quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET, 10, 10, 1),
		unitContinuousLimit("leaky_not_lease", quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET, 10, 10, 1),
		unitContinuousLimit("gcra_not_lease", quotav1.Algorithm_ALGORITHM_GCRA, 10, 10, 1),
	}

	for _, limit := range consumeOnly {
		t.Run(limit.GetLimitId(), func(t *testing.T) {
			_, err := svc.buildLimitOps([]*quotav1.Limit{limit}, 1, now, true)
			if err == nil || !strings.Contains(err.Error(), "AcquireLease only accepts") {
				t.Fatalf("buildLimitOps err = %v, want AcquireLease rejection", err)
			}
		})
	}

	_, err := svc.buildLimitOps([]*quotav1.Limit{unitConcurrencyLimit("concurrency_not_consume", 1)}, 1, now, false)
	if err == nil || !strings.Contains(err.Error(), "Consume/Reserve do not accept") {
		t.Fatalf("buildLimitOps err = %v, want Consume/Reserve rejection", err)
	}

	_, err = svc.buildLimitOps([]*quotav1.Limit{{
		LimitId:   "unsupported_algorithm",
		ScopeKey:  "scope:unsupported_algorithm",
		Action:    "test.unsupported_algorithm",
		Unit:      "requests",
		Algorithm: quotav1.Algorithm(9999),
		Limit:     1,
	}}, 1, now, false)
	if err == nil || !strings.Contains(err.Error(), "unsupported algorithm") {
		t.Fatalf("buildLimitOps err = %v, want unsupported algorithm rejection", err)
	}
}

func TestBucketAndGCRAUsageEmptyState(t *testing.T) {
	now := time.UnixMilli(180000)
	svc := newUnitService(t, &usageBackend{}, nil)

	used, remaining, err := svc.bucketUsage(context.Background(), "missing-bucket", unitContinuousLimit("bucket_empty", quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET, 5, 0, 1), now)
	if err != nil {
		t.Fatalf("bucketUsage: %v", err)
	}
	if used != 0 || remaining != 5 {
		t.Fatalf("empty bucket usage = used %d remaining %d, want 0/5", used, remaining)
	}

	remaining, retry, err := svc.gcraUsage(context.Background(), "missing-gcra", unitContinuousLimit("gcra_empty", quotav1.Algorithm_ALGORITHM_GCRA, 1, 0, 1), now)
	if err != nil {
		t.Fatalf("gcraUsage: %v", err)
	}
	if remaining != 1 || retry != 0 {
		t.Fatalf("empty GCRA usage = remaining %d retry %d, want 1/0", remaining, retry)
	}
}

func TestDecisionFromResultMessageAndIDFallback(t *testing.T) {
	limit := unitDurationLimit("decision_fallback", 10)
	svc := newUnitService(t, &countingBackend{}, nil)

	decision := svc.decisionFromResult(backend.DecisionResult{
		Allowed: true,
		Message: "backend message",
		Statuses: []backend.ScriptStatus{{
			LimitID:   limit.GetLimitId(),
			Used:      1,
			Remaining: 9,
			Allowed:   true,
			Message:   "allowed",
		}},
	}, []*quotav1.Limit{limit}, 1, false, "consume")
	if decision.GetDecisionId() == "" {
		t.Fatalf("decision id should be generated: %v", decision)
	}
	if decision.GetMessage() != "backend message" {
		t.Fatalf("message = %q, want backend message", decision.GetMessage())
	}
}

func TestGCRAUsageUsesLimitAsDefaultBurst(t *testing.T) {
	now := time.UnixMilli(180000)
	store := &usageBackend{
		gcraValue:  float64(now.Add(time.Second).UnixMilli()),
		gcraExists: true,
	}
	svc := newUnitService(t, store, nil)

	remaining, retry, err := svc.gcraUsage(context.Background(), "gcra-default-burst", unitContinuousLimit("gcra_default_burst", quotav1.Algorithm_ALGORITHM_GCRA, 2, 0, 1), now)
	if err != nil {
		t.Fatalf("gcraUsage: %v", err)
	}
	if remaining != 1 || retry != 0 {
		t.Fatalf("GCRA default burst usage = remaining %d retry %d, want 1/0", remaining, retry)
	}
}

func newUnitService(t *testing.T, store backend.Backend, sink EventSink) *Server {
	t.Helper()
	svc, err := New(context.Background(), Options{
		Product:     "unit-product",
		Environment: "unit-env",
		Backend:     store,
		EventSink:   sink,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc
}

type captureSink struct {
	events []Event
}

func (s *captureSink) Emit(_ context.Context, event Event) {
	s.events = append(s.events, event)
}

func (s *captureSink) Close() error { return nil }

type countingBackend struct {
	backend.Backend
}

type consumeBackend struct {
	backend.Backend
	result  backend.DecisionResult
	err     error
	calls   int
	idemKey string
	ops     []backend.LimitOp
}

func (b *consumeBackend) Consume(_ context.Context, idemKey string, _ time.Time, ops []backend.LimitOp, _ bool, _ string) (backend.DecisionResult, error) {
	b.calls++
	b.idemKey = idemKey
	b.ops = append([]backend.LimitOp(nil), ops...)
	return b.result, b.err
}

type reserveBackend struct {
	backend.Backend
	result      backend.DecisionResult
	reservation *quotav1.Reservation
}

func (b *reserveBackend) Reserve(context.Context, string, time.Time, []backend.LimitOp, bool, string, string, *quotav1.Reservation, string) (backend.DecisionResult, error) {
	return b.result, nil
}

func (b *reserveBackend) GetReservation(context.Context, string) (*quotav1.Reservation, error) {
	return b.reservation, nil
}

type reservationBackend struct {
	backend.Backend
	incrementResult backend.IncrementReservationResult
	finalizeResult  backend.FinalizeResult
	releaseResult   backend.ReleaseReservationResult
}

func (b *reservationBackend) IncrementReservation(context.Context, string, string, string, int64, time.Time, string) (backend.IncrementReservationResult, error) {
	return b.incrementResult, nil
}

func (b *reservationBackend) FinalizeReservation(context.Context, string, string, string, int64, time.Time) (backend.FinalizeResult, error) {
	return b.finalizeResult, nil
}

func (b *reservationBackend) ReleaseReservation(context.Context, string, string, string, time.Time) (backend.ReleaseReservationResult, error) {
	return b.releaseResult, nil
}

type errorBackend struct {
	backend.Backend
	err error
}

func (b errorBackend) Consume(context.Context, string, time.Time, []backend.LimitOp, bool, string) (backend.DecisionResult, error) {
	return backend.DecisionResult{}, b.err
}

func (b errorBackend) Reserve(context.Context, string, time.Time, []backend.LimitOp, bool, string, string, *quotav1.Reservation, string) (backend.DecisionResult, error) {
	return backend.DecisionResult{}, b.err
}

func (b errorBackend) IncrementReservation(context.Context, string, string, string, int64, time.Time, string) (backend.IncrementReservationResult, error) {
	return backend.IncrementReservationResult{}, b.err
}

func (b errorBackend) FinalizeReservation(context.Context, string, string, string, int64, time.Time) (backend.FinalizeResult, error) {
	return backend.FinalizeResult{}, b.err
}

func (b errorBackend) ReleaseReservation(context.Context, string, string, string, time.Time) (backend.ReleaseReservationResult, error) {
	return backend.ReleaseReservationResult{}, b.err
}

func (b errorBackend) ExpireReservations(context.Context, string, time.Time, int64) (backend.ExpireReservationsResult, error) {
	return backend.ExpireReservationsResult{}, b.err
}

func (b errorBackend) AcquireLease(context.Context, string, string, *quotav1.Lease, time.Duration, []backend.LimitOp, bool, string) (backend.DecisionResult, error) {
	return backend.DecisionResult{}, b.err
}

func (b errorBackend) RenewLease(context.Context, string, string, string, time.Duration, time.Time) (backend.LeaseResult, error) {
	return backend.LeaseResult{}, b.err
}

func (b errorBackend) ReleaseLease(context.Context, string, string, string) (backend.LeaseResult, error) {
	return backend.LeaseResult{}, b.err
}

func (b errorBackend) GetReservation(context.Context, string) (*quotav1.Reservation, error) {
	return nil, b.err
}

func (b errorBackend) GetLease(context.Context, string) (*quotav1.Lease, error) {
	return nil, b.err
}

func (b errorBackend) CounterValue(context.Context, string) (int64, error) {
	return 0, b.err
}

type usageBackend struct {
	backend.Backend
	counterValue      int64
	counterCalls      int
	counterErr        error
	bucketState       backend.BucketState
	bucketErr         error
	gcraValue         float64
	gcraExists        bool
	gcraErr           error
	concurrencyActive int64
	concurrencyErr    error
}

func (b *usageBackend) CounterValue(context.Context, string) (int64, error) {
	b.counterCalls++
	if b.counterErr != nil {
		return 0, b.counterErr
	}
	return b.counterValue, nil
}

func (b *usageBackend) BucketState(context.Context, string) (backend.BucketState, error) {
	if b.bucketErr != nil {
		return backend.BucketState{}, b.bucketErr
	}
	return b.bucketState, nil
}

func (b *usageBackend) GCRAValue(context.Context, string) (float64, bool, error) {
	if b.gcraErr != nil {
		return 0, false, b.gcraErr
	}
	return b.gcraValue, b.gcraExists, nil
}

func (b *usageBackend) ConcurrencyCount(context.Context, string, time.Time) (int64, error) {
	if b.concurrencyErr != nil {
		return 0, b.concurrencyErr
	}
	return b.concurrencyActive, nil
}

type healthBackend struct {
	backend.Backend
	pingErr    error
	loaded     []bool
	loadedErrs []error
	loadErr    error
	loadCalls  int
}

func (b *healthBackend) Ping(context.Context) error {
	return b.pingErr
}

func (b *healthBackend) ScriptsLoaded(context.Context) (bool, error) {
	if len(b.loadedErrs) > 0 {
		err := b.loadedErrs[0]
		b.loadedErrs = b.loadedErrs[1:]
		if err != nil {
			return false, err
		}
	}
	if len(b.loaded) == 0 {
		return false, nil
	}
	loaded := b.loaded[0]
	b.loaded = b.loaded[1:]
	return loaded, nil
}

func (b *healthBackend) LoadScripts(context.Context) error {
	b.loadCalls++
	return b.loadErr
}

type readBackend struct {
	backend.Backend
}

func (readBackend) GetReservation(context.Context, string) (*quotav1.Reservation, error) {
	return nil, backend.ErrNotFound
}

func (readBackend) GetLease(context.Context, string) (*quotav1.Lease, error) {
	return nil, backend.ErrNotFound
}

type readResultBackend struct {
	backend.Backend
	reservation *quotav1.Reservation
	lease       *quotav1.Lease
}

func (b *readResultBackend) GetReservation(context.Context, string) (*quotav1.Reservation, error) {
	return b.reservation, nil
}

func (b *readResultBackend) GetLease(context.Context, string) (*quotav1.Lease, error) {
	return b.lease, nil
}

type leaseBackend struct {
	backend.Backend
	acquireResult backend.DecisionResult
	renewResult   backend.LeaseResult
	releaseResult backend.LeaseResult
	lease         *quotav1.Lease
}

func (b *leaseBackend) AcquireLease(context.Context, string, string, *quotav1.Lease, time.Duration, []backend.LimitOp, bool, string) (backend.DecisionResult, error) {
	return b.acquireResult, nil
}

func (b *leaseBackend) RenewLease(context.Context, string, string, string, time.Duration, time.Time) (backend.LeaseResult, error) {
	return b.renewResult, nil
}

func (b *leaseBackend) ReleaseLease(context.Context, string, string, string) (backend.LeaseResult, error) {
	return b.releaseResult, nil
}

func (b *leaseBackend) GetLease(context.Context, string) (*quotav1.Lease, error) {
	return b.lease, nil
}

func unitDurationLimit(name string, limit int64) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:   name,
		ScopeKey:  "scope:" + name,
		Action:    "test." + name,
		Unit:      "requests",
		Algorithm: quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION,
		Window: &quotav1.Window{
			Type:       quotav1.WindowType_WINDOW_TYPE_DURATION,
			DurationMs: 60000,
		},
		Limit: limit,
	}
}

func unitCalendarLimit(name string, limit int64) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:   name,
		ScopeKey:  "scope:" + name,
		Action:    "test." + name,
		Unit:      "requests",
		Algorithm: quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR,
		Window: &quotav1.Window{
			Type:         quotav1.WindowType_WINDOW_TYPE_CALENDAR,
			CalendarUnit: quotav1.CalendarUnit_CALENDAR_UNIT_DAY,
			Timezone:     "UTC",
		},
		Limit: limit,
	}
}

func unitSlidingLimit(name string, limit int64) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:   name,
		ScopeKey:  "scope:" + name,
		Action:    "test." + name,
		Unit:      "requests",
		Algorithm: quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW,
		Window: &quotav1.Window{
			Type:        quotav1.WindowType_WINDOW_TYPE_SLIDING,
			DurationMs:  60000,
			BucketCount: 6,
		},
		Limit: limit,
	}
}

func unitContinuousLimit(name string, algorithm quotav1.Algorithm, limit, burst int64, refill float64) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:          name,
		ScopeKey:         "scope:" + name,
		Action:           "test." + name,
		Unit:             "tokens",
		Algorithm:        algorithm,
		Window:           &quotav1.Window{Type: quotav1.WindowType_WINDOW_TYPE_CONTINUOUS, DurationMs: 60000},
		Limit:            limit,
		Burst:            burst,
		RefillRatePerSec: refill,
	}
}

func unitConcurrencyLimit(name string, limit int64) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:   name,
		ScopeKey:  "scope:" + name,
		Action:    "test." + name,
		Unit:      "requests",
		Algorithm: quotav1.Algorithm_ALGORITHM_CONCURRENCY,
		Limit:     limit,
	}
}
