package ratelimiterserver

import (
	"context"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	rlredis "github.com/elloloop/rate-limiter/ratelimiterserver/backend/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func TestConsumeIsAtomicAndIdempotentWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := fixedDayLimit()
	first, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
		RequestId: "req-1",
		Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:    "workspace.email.recipients",
		Cost:      25,
		Limits:    []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if !first.GetDecision().GetAllowed() {
		t.Fatalf("expected first request allowed: %v", first.GetDecision())
	}
	if got := first.GetDecision().GetLimitStatuses()[0].GetUsed(); got != 25 {
		t.Fatalf("used after first request = %d, want 25", got)
	}

	replay, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
		RequestId: "req-1",
		Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:    "workspace.email.recipients",
		Cost:      25,
		Limits:    []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("replay consume: %v", err)
	}
	if replay.GetDecision().GetDecisionId() != first.GetDecision().GetDecisionId() {
		t.Fatalf("idempotent replay changed decision id: got %q want %q", replay.GetDecision().GetDecisionId(), first.GetDecision().GetDecisionId())
	}
	if replay.GetDecision().GetMetadata()["idempotency_hit"] != "true" {
		t.Fatalf("expected idempotency metadata on replay")
	}

	denied, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
		RequestId: "req-2",
		Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:    "workspace.email.recipients",
		Cost:      10,
		Limits:    []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("denied consume: %v", err)
	}
	if denied.GetDecision().GetAllowed() {
		t.Fatalf("expected second distinct request to deny")
	}

	usage, err := svc.GetCurrentUsage(ctx, &quotav1.GetCurrentUsageRequest{
		Context: &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:  "workspace.email.recipients",
		Limits:  []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("usage: %v", err)
	}
	if got := usage.GetLimitStatuses()[0].GetUsed(); got != 25 {
		t.Fatalf("denied request mutated usage: got %d want 25", got)
	}
}

func TestConsumeConcurrentContentionUsesRedisAtomicity(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	const allowedLimit = 20
	const attempts = 80

	limit := fixedDurationLimit("concurrent_atomic", allowedLimit)
	start := make(chan struct{})
	results := make(chan bool, attempts)
	errs := make(chan error, attempts)

	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			resp, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
				RequestId: "req-concurrent-" + strconv.Itoa(i),
				Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
				Action:    limit.GetAction(),
				Cost:      1,
				Limits:    []*quotav1.Limit{limit},
			})
			if err != nil {
				errs <- err
				return
			}
			results <- resp.GetDecision().GetAllowed()
		}(i)
	}

	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent consume: %v", err)
		}
	}

	var allowed, denied int
	for ok := range results {
		if ok {
			allowed++
		} else {
			denied++
		}
	}
	if allowed != allowedLimit {
		t.Fatalf("allowed requests = %d, want %d", allowed, allowedLimit)
	}
	if denied != attempts-allowedLimit {
		t.Fatalf("denied requests = %d, want %d", denied, attempts-allowedLimit)
	}
	assertUsed(t, ctx, svc, limit, allowedLimit)
}

func TestRedisScriptsReloadAfterScriptFlushWithRedis(t *testing.T) {
	ctx, svc, store := newRedisBackedService(t)

	if err := store.FlushScripts(ctx); err != nil {
		t.Fatalf("script flush: %v", err)
	}

	limit := fixedDurationLimit("script_reload", 5)
	resp, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
		RequestId: "req-script-reload",
		Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:    limit.GetAction(),
		Cost:      1,
		Limits:    []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("consume after script flush should reload scripts: %v", err)
	}
	if !resp.GetDecision().GetAllowed() {
		t.Fatalf("expected consume after script reload to allow: %v", resp.GetDecision())
	}

	status, err := svc.GetRedisStatus(ctx, &quotav1.GetRedisStatusRequest{})
	if err != nil {
		t.Fatalf("redis status after script reload: %v", err)
	}
	if !status.GetReachable() || status.GetMessage() != "ok" {
		t.Fatalf("unexpected redis status after script reload: %v", status)
	}
}

func TestConsumeSupportsWindowAndBucketAlgorithmsWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	tests := []struct {
		name        string
		limit       *quotav1.Limit
		costs       []int64
		wantAllowed []bool
	}{
		{
			name:        "fixed_duration",
			limit:       fixedDurationLimit("fixed_duration", 5),
			costs:       []int64{3, 3},
			wantAllowed: []bool{true, false},
		},
		{
			name:        "sliding_window",
			limit:       slidingLimit("sliding_window", 4),
			costs:       []int64{2, 3},
			wantAllowed: []bool{true, false},
		},
		{
			name:        "token_bucket",
			limit:       bucketLimit("token_bucket", quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET, 10, 10, 1),
			costs:       []int64{7, 4},
			wantAllowed: []bool{true, false},
		},
		{
			name:        "leaky_bucket",
			limit:       bucketLimit("leaky_bucket", quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET, 10, 10, 1),
			costs:       []int64{7, 4},
			wantAllowed: []bool{true, false},
		},
		{
			name:        "gcra",
			limit:       gcraLimit("gcra"),
			costs:       []int64{1, 1, 1},
			wantAllowed: []bool{true, true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, cost := range tt.costs {
				resp, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
					RequestId: "req-" + tt.name + "-" + strconv.Itoa(i),
					Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
					Action:    tt.limit.GetAction(),
					Cost:      cost,
					Limits:    []*quotav1.Limit{tt.limit},
				})
				if err != nil {
					t.Fatalf("consume %s #%d: %v", tt.name, i, err)
				}
				if got := resp.GetDecision().GetAllowed(); got != tt.wantAllowed[i] {
					t.Fatalf("%s #%d allowed = %v, want %v; decision=%v", tt.name, i, got, tt.wantAllowed[i], resp.GetDecision())
				}
				if !tt.wantAllowed[i] && resp.GetDecision().GetRetryAfterMs() < 0 {
					t.Fatalf("retry_after_ms should not be negative: %d", resp.GetDecision().GetRetryAfterMs())
				}
			}
		})
	}
}

func TestDryRunAndExplainDoNotMutateCounters(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := fixedDurationLimit("dry_run", 10)
	resp, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
		RequestId: "req-dry-run",
		Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:    limit.GetAction(),
		Cost:      7,
		Limits:    []*quotav1.Limit{limit},
		Options:   &quotav1.RequestOptions{DryRun: true},
	})
	if err != nil {
		t.Fatalf("dry-run consume: %v", err)
	}
	if !resp.GetDecision().GetAllowed() || resp.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_DRY_RUN {
		t.Fatalf("expected dry-run allowed decision, got %v", resp.GetDecision())
	}

	usage, err := svc.GetCurrentUsage(ctx, &quotav1.GetCurrentUsageRequest{
		Context: &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:  limit.GetAction(),
		Limits:  []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("usage after dry-run: %v", err)
	}
	if got := usage.GetLimitStatuses()[0].GetUsed(); got != 0 {
		t.Fatalf("dry-run mutated usage: got %d want 0", got)
	}

	explained, err := svc.Explain(ctx, &quotav1.ExplainRequest{
		Context: &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:  limit.GetAction(),
		Cost:    11,
		Limits:  []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if explained.GetWouldAllow() {
		t.Fatalf("expected explain to deny cost above limit: %v", explained)
	}
}

func TestReserveDryRunDoesNotCreateReservationOrMutateCountersWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := fixedDurationLimit("reserve_dry_run", 10)
	resp, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-reserve-dry-run",
		Context:          &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:           limit.GetAction(),
		ReserveCost:      7,
		ReservationTtlMs: 60000,
		Limits:           []*quotav1.Limit{limit},
		Options:          &quotav1.RequestOptions{DryRun: true},
	})
	if err != nil {
		t.Fatalf("dry-run reserve: %v", err)
	}
	if !resp.GetDecision().GetAllowed() || resp.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_DRY_RUN {
		t.Fatalf("expected dry-run reserve decision, got %v", resp.GetDecision())
	}
	if resp.GetReservation() != nil {
		t.Fatalf("dry-run reserve returned a reservation: %v", resp.GetReservation())
	}
	assertUsed(t, ctx, svc, limit, 0)

	real, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-reserve-after-dry-run",
		Context:          &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:           limit.GetAction(),
		ReserveCost:      7,
		ReservationTtlMs: 60000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("reserve after dry-run: %v", err)
	}
	if !real.GetDecision().GetAllowed() || real.GetReservation() == nil {
		t.Fatalf("expected real reservation after dry-run, got %v", real)
	}
	assertUsed(t, ctx, svc, limit, 7)
}

func TestLeaseDryRunDoesNotCreateLeaseOrConsumeConcurrencyWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := concurrencyLimit(1)
	resp, err := svc.AcquireLease(ctx, &quotav1.AcquireLeaseRequest{
		RequestId:  "req-lease-dry-run",
		Context:    &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:     limit.GetAction(),
		Limits:     []*quotav1.Limit{limit},
		LeaseTtlMs: 60000,
		Options:    &quotav1.RequestOptions{DryRun: true},
	})
	if err != nil {
		t.Fatalf("dry-run acquire lease: %v", err)
	}
	if !resp.GetDecision().GetAllowed() || resp.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_DRY_RUN {
		t.Fatalf("expected dry-run lease decision, got %v", resp.GetDecision())
	}
	if resp.GetLease() != nil {
		t.Fatalf("dry-run acquire returned a lease: %v", resp.GetLease())
	}
	assertUsed(t, ctx, svc, limit, 0)

	real := acquireLease(t, ctx, svc, "req-lease-after-dry-run", limit)
	if real.GetLease() == nil {
		t.Fatalf("expected real lease after dry-run")
	}
	assertUsed(t, ctx, svc, limit, 1)
}

func TestContinuousBucketsRefillWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	tests := []struct {
		name      string
		algorithm quotav1.Algorithm
	}{
		{name: "token_bucket", algorithm: quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET},
		{name: "leaky_bucket", algorithm: quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit := bucketLimit("refill_"+tt.name, tt.algorithm, 10, 10, 20)
			first, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
				RequestId: "req-refill-" + tt.name + "-first",
				Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
				Action:    limit.GetAction(),
				Cost:      10,
				Limits:    []*quotav1.Limit{limit},
			})
			if err != nil {
				t.Fatalf("first consume: %v", err)
			}
			if !first.GetDecision().GetAllowed() {
				t.Fatalf("expected first consume allowed: %v", first.GetDecision())
			}

			denied, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
				RequestId: "req-refill-" + tt.name + "-denied",
				Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
				Action:    limit.GetAction(),
				Cost:      2,
				Limits:    []*quotav1.Limit{limit},
			})
			if err != nil {
				t.Fatalf("immediate consume: %v", err)
			}
			if denied.GetDecision().GetAllowed() {
				t.Fatalf("expected exhausted bucket to deny: %v", denied.GetDecision())
			}

			time.Sleep(150 * time.Millisecond)
			refilled, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
				RequestId: "req-refill-" + tt.name + "-refilled",
				Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
				Action:    limit.GetAction(),
				Cost:      2,
				Limits:    []*quotav1.Limit{limit},
			})
			if err != nil {
				t.Fatalf("refilled consume: %v", err)
			}
			if !refilled.GetDecision().GetAllowed() {
				t.Fatalf("expected refilled bucket to allow: %v", refilled.GetDecision())
			}
		})
	}
}

func TestReservationFinalizeAndReleaseLifecycleWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := fixedDayLimitWithLimit(100)
	limit.Refundable = true
	limit.ReservationExpiryPolicy = quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_CHARGE_FULL

	reserve, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-reserve-1",
		Context:          &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:           limit.GetAction(),
		ReserveCost:      80,
		ReservationTtlMs: 60000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !reserve.GetDecision().GetAllowed() || reserve.GetReservation() == nil {
		t.Fatalf("expected reservation, got %v", reserve)
	}
	if got := reserve.GetReservation().GetImpacts()[0].GetRedisKey(); got == "" {
		t.Fatalf("reservation impact did not store Redis key")
	}
	assertUsed(t, ctx, svc, limit, 80)

	finalized, err := svc.FinalizeReservation(ctx, &quotav1.FinalizeReservationRequest{
		RequestId:     "req-finalize-1",
		ReservationId: reserve.GetReservation().GetReservationId(),
		ActualCost:    50,
		Status:        quotav1.FinalizeStatus_FINALIZE_STATUS_SUCCEEDED,
		Metadata:      map[string]string{"test": "refund"},
	})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if !finalized.GetFinalized() || finalized.GetRefundedCost() != 30 || finalized.GetOverageCost() != 0 {
		t.Fatalf("unexpected finalization response: %v", finalized)
	}
	assertUsed(t, ctx, svc, limit, 50)

	replay, err := svc.FinalizeReservation(ctx, &quotav1.FinalizeReservationRequest{
		RequestId:     "req-finalize-1",
		ReservationId: reserve.GetReservation().GetReservationId(),
		ActualCost:    10,
		Status:        quotav1.FinalizeStatus_FINALIZE_STATUS_SUCCEEDED,
	})
	if err != nil {
		t.Fatalf("finalize replay: %v", err)
	}
	if replay.GetActualCost() != 50 || replay.GetRefundedCost() != 30 {
		t.Fatalf("finalize replay was not idempotent: %v", replay)
	}

	stored, err := svc.GetReservation(ctx, &quotav1.GetReservationRequest{ReservationId: reserve.GetReservation().GetReservationId()})
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}
	if stored.GetStatus() != quotav1.ReservationStatus_RESERVATION_STATUS_FINALIZED {
		t.Fatalf("stored reservation status = %s", stored.GetStatus())
	}

	second, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-reserve-2",
		Context:          &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:           limit.GetAction(),
		ReserveCost:      20,
		ReservationTtlMs: 60000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if !second.GetDecision().GetAllowed() {
		t.Fatalf("second reservation denied: %v", second.GetDecision())
	}
	assertUsed(t, ctx, svc, limit, 70)

	released, err := svc.ReleaseReservation(ctx, &quotav1.ReleaseReservationRequest{
		RequestId:     "req-release-1",
		ReservationId: second.GetReservation().GetReservationId(),
		Reason:        "operation did not happen",
	})
	if err != nil {
		t.Fatalf("release reservation: %v", err)
	}
	if !released.GetReleased() || released.GetReleasedCost() != 20 {
		t.Fatalf("unexpected release response: %v", released)
	}
	assertUsed(t, ctx, svc, limit, 50)
}

func TestIncrementReservationLifecycleWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := fixedDayLimitWithLimit(100)
	limit.Refundable = true
	limit.ReservationExpiryPolicy = quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_CHARGE_FULL

	reserve, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-increment-reserve",
		Context:          &quotav1.RequestContext{Product: "assistant", Environment: "test"},
		Action:           limit.GetAction(),
		ReserveCost:      40,
		ReservationTtlMs: 60000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !reserve.GetDecision().GetAllowed() || reserve.GetReservation() == nil {
		t.Fatalf("expected reservation, got %v", reserve)
	}
	assertUsed(t, ctx, svc, limit, 40)

	grow, err := svc.IncrementReservation(ctx, &quotav1.IncrementReservationRequest{
		RequestId:     "req-increment-grow",
		ReservationId: reserve.GetReservation().GetReservationId(),
		DeltaCost:     30,
	})
	if err != nil {
		t.Fatalf("increment grow: %v", err)
	}
	if !grow.GetDecision().GetAllowed() || grow.GetReservedCost() != 70 || grow.GetReservation().GetReservedCost() != 70 {
		t.Fatalf("unexpected grow response: %v", grow)
	}
	assertUsed(t, ctx, svc, limit, 70)

	replay, err := svc.IncrementReservation(ctx, &quotav1.IncrementReservationRequest{
		RequestId:     "req-increment-grow",
		ReservationId: reserve.GetReservation().GetReservationId(),
		DeltaCost:     999,
	})
	if err != nil {
		t.Fatalf("increment replay: %v", err)
	}
	if replay.GetReservedCost() != 70 || replay.GetDecision().GetMetadata()["idempotency_hit"] != "true" {
		t.Fatalf("increment replay was not idempotent: %v", replay)
	}
	assertUsed(t, ctx, svc, limit, 70)

	denied, err := svc.IncrementReservation(ctx, &quotav1.IncrementReservationRequest{
		RequestId:     "req-increment-denied",
		ReservationId: reserve.GetReservation().GetReservationId(),
		DeltaCost:     40,
	})
	if err != nil {
		t.Fatalf("increment denied: %v", err)
	}
	if denied.GetDecision().GetAllowed() || denied.GetReservedCost() != 70 {
		t.Fatalf("expected increment denial without reservation growth: %v", denied)
	}
	assertUsed(t, ctx, svc, limit, 70)

	shrink, err := svc.IncrementReservation(ctx, &quotav1.IncrementReservationRequest{
		RequestId:     "req-increment-shrink",
		ReservationId: reserve.GetReservation().GetReservationId(),
		DeltaCost:     -25,
	})
	if err != nil {
		t.Fatalf("increment shrink: %v", err)
	}
	if !shrink.GetDecision().GetAllowed() || shrink.GetReservedCost() != 45 {
		t.Fatalf("unexpected shrink response: %v", shrink)
	}
	assertUsed(t, ctx, svc, limit, 45)

	finalized, err := svc.FinalizeReservation(ctx, &quotav1.FinalizeReservationRequest{
		RequestId:     "req-increment-finalize",
		ReservationId: reserve.GetReservation().GetReservationId(),
		ActualCost:    40,
		Status:        quotav1.FinalizeStatus_FINALIZE_STATUS_SUCCEEDED,
	})
	if err != nil {
		t.Fatalf("finalize incremented reservation: %v", err)
	}
	if !finalized.GetFinalized() || finalized.GetReservedCost() != 45 || finalized.GetRefundedCost() != 5 {
		t.Fatalf("unexpected finalization response: %v", finalized)
	}
	assertUsed(t, ctx, svc, limit, 40)
}

func TestExpiredReservationsRefundByExpiryPolicyWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	refundLimit := fixedDayLimitWithLimit(100)
	refundLimit.Refundable = false
	refundLimit.ReservationExpiryPolicy = quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_REFUND_FULL

	refundReserve, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-expire-refund-reserve",
		Context:          &quotav1.RequestContext{Product: "assistant", Environment: "test"},
		Action:           refundLimit.GetAction(),
		ReserveCost:      40,
		ReservationTtlMs: 100,
		Limits:           []*quotav1.Limit{refundLimit},
	})
	if err != nil {
		t.Fatalf("reserve refund-full: %v", err)
	}
	if !refundReserve.GetDecision().GetAllowed() {
		t.Fatalf("refund-full reserve denied: %v", refundReserve.GetDecision())
	}
	assertUsed(t, ctx, svc, refundLimit, 40)

	time.Sleep(200 * time.Millisecond)
	expired, err := svc.ExpireReservations(ctx, 100)
	if err != nil {
		t.Fatalf("expire refund-full reservations: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expired refund-full reservations = %d, want 1", expired)
	}
	assertUsed(t, ctx, svc, refundLimit, 0)

	storedRefund, err := svc.GetReservation(ctx, &quotav1.GetReservationRequest{ReservationId: refundReserve.GetReservation().GetReservationId()})
	if err != nil {
		t.Fatalf("get expired refund-full reservation: %v", err)
	}
	if storedRefund.GetStatus() != quotav1.ReservationStatus_RESERVATION_STATUS_EXPIRED || storedRefund.GetRefundedCost() != 40 {
		t.Fatalf("unexpected expired refund-full reservation: %v", storedRefund)
	}

	chargeLimit := fixedDurationLimit("expire_charge_full", 100)
	chargeLimit.Refundable = true
	chargeLimit.ReservationExpiryPolicy = quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_CHARGE_FULL

	chargeReserve, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-expire-charge-reserve",
		Context:          &quotav1.RequestContext{Product: "assistant", Environment: "test"},
		Action:           chargeLimit.GetAction(),
		ReserveCost:      40,
		ReservationTtlMs: 100,
		Limits:           []*quotav1.Limit{chargeLimit},
	})
	if err != nil {
		t.Fatalf("reserve charge-full: %v", err)
	}
	if !chargeReserve.GetDecision().GetAllowed() {
		t.Fatalf("charge-full reserve denied: %v", chargeReserve.GetDecision())
	}
	assertUsed(t, ctx, svc, chargeLimit, 40)

	time.Sleep(200 * time.Millisecond)
	expired, err = svc.ExpireReservations(ctx, 100)
	if err != nil {
		t.Fatalf("expire charge-full reservations: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expired charge-full reservations = %d, want 1", expired)
	}
	assertUsed(t, ctx, svc, chargeLimit, 40)

	storedCharge, err := svc.GetReservation(ctx, &quotav1.GetReservationRequest{ReservationId: chargeReserve.GetReservation().GetReservationId()})
	if err != nil {
		t.Fatalf("get expired charge-full reservation: %v", err)
	}
	if storedCharge.GetStatus() != quotav1.ReservationStatus_RESERVATION_STATUS_EXPIRED || storedCharge.GetRefundedCost() != 0 {
		t.Fatalf("unexpected expired charge-full reservation: %v", storedCharge)
	}

	body := serviceMetrics(t, svc)
	requireMetric(t, body, "quota_reservations_expired_total 2")
}

func TestLifecycleIdempotentReplaysDoNotDoubleCountActiveMetricsWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := fixedDayLimitWithLimit(100)
	limit.Refundable = true
	limit.ReservationExpiryPolicy = quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_CHARGE_FULL

	releasedReserve, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-metrics-release-reserve",
		Context:          &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:           limit.GetAction(),
		ReserveCost:      20,
		ReservationTtlMs: 60000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("reserve for release metrics: %v", err)
	}
	for i := 0; i < 2; i++ {
		released, err := svc.ReleaseReservation(ctx, &quotav1.ReleaseReservationRequest{
			RequestId:     "req-metrics-release",
			ReservationId: releasedReserve.GetReservation().GetReservationId(),
			Reason:        "idempotent replay",
		})
		if err != nil {
			t.Fatalf("release replay %d: %v", i, err)
		}
		if !released.GetReleased() {
			t.Fatalf("release replay %d should return released=true: %v", i, released)
		}
	}

	finalizedReserve, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-metrics-finalize-reserve",
		Context:          &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:           limit.GetAction(),
		ReserveCost:      30,
		ReservationTtlMs: 60000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("reserve for finalize metrics: %v", err)
	}
	for i := 0; i < 2; i++ {
		finalized, err := svc.FinalizeReservation(ctx, &quotav1.FinalizeReservationRequest{
			RequestId:     "req-metrics-finalize",
			ReservationId: finalizedReserve.GetReservation().GetReservationId(),
			ActualCost:    40,
			Status:        quotav1.FinalizeStatus_FINALIZE_STATUS_SUCCEEDED,
		})
		if err != nil {
			t.Fatalf("finalize replay %d: %v", i, err)
		}
		if !finalized.GetFinalized() || finalized.GetOverageCost() != 10 {
			t.Fatalf("unexpected finalize replay %d response: %v", i, finalized)
		}
	}

	leaseLimit := concurrencyLimit(1)
	lease := acquireLease(t, ctx, svc, "req-metrics-lease", leaseLimit)
	for i := 0; i < 2; i++ {
		released, err := svc.ReleaseLease(ctx, &quotav1.ReleaseLeaseRequest{
			RequestId: "req-metrics-release-lease",
			LeaseId:   lease.GetLease().GetLeaseId(),
		})
		if err != nil {
			t.Fatalf("release lease replay %d: %v", i, err)
		}
		if !released.GetReleased() {
			t.Fatalf("release lease replay %d should return released=true: %v", i, released)
		}
	}

	body := serviceMetrics(t, svc)
	requireMetric(t, body, "quota_reservations_active 0")
	requireMetric(t, body, "quota_leases_active 0")
	requireMetric(t, body, "quota_overages_total 1")
	requireMetric(t, body, "quota_idempotency_hits_total 3")
}

func TestReservationOverageDoesNotMutateOriginalWindowWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := fixedDayLimitWithLimit(100)
	limit.Refundable = true
	limit.ReservationExpiryPolicy = quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_CHARGE_FULL

	reserve, err := svc.Reserve(ctx, &quotav1.ReserveRequest{
		RequestId:        "req-overage-reserve",
		Context:          &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:           limit.GetAction(),
		ReserveCost:      40,
		ReservationTtlMs: 60000,
		Limits:           []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !reserve.GetDecision().GetAllowed() || reserve.GetReservation() == nil {
		t.Fatalf("expected reservation, got %v", reserve)
	}
	assertUsed(t, ctx, svc, limit, 40)

	finalized, err := svc.FinalizeReservation(ctx, &quotav1.FinalizeReservationRequest{
		RequestId:     "req-overage-finalize",
		ReservationId: reserve.GetReservation().GetReservationId(),
		ActualCost:    65,
		Status:        quotav1.FinalizeStatus_FINALIZE_STATUS_SUCCEEDED,
	})
	if err != nil {
		t.Fatalf("finalize overage: %v", err)
	}
	if !finalized.GetFinalized() || finalized.GetRefundedCost() != 0 || finalized.GetOverageCost() != 25 {
		t.Fatalf("unexpected overage finalization response: %v", finalized)
	}
	assertUsed(t, ctx, svc, limit, 40)

	stored, err := svc.GetReservation(ctx, &quotav1.GetReservationRequest{ReservationId: reserve.GetReservation().GetReservationId()})
	if err != nil {
		t.Fatalf("get overage reservation: %v", err)
	}
	if stored.GetActualCost() != 65 || stored.GetOverageCost() != 25 || stored.GetStatus() != quotav1.ReservationStatus_RESERVATION_STATUS_FINALIZED {
		t.Fatalf("stored overage reservation mismatch: %v", stored)
	}

	consume, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
		RequestId: "req-overage-consume",
		Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:    limit.GetAction(),
		Cost:      60,
		Limits:    []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("consume after overage: %v", err)
	}
	if !consume.GetDecision().GetAllowed() {
		t.Fatalf("overage mutated current window; consume denied: %v", consume.GetDecision())
	}
	assertUsed(t, ctx, svc, limit, 100)
}

func TestConcurrencyLeaseLifecycleWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := concurrencyLimit(2)
	first := acquireLease(t, ctx, svc, "req-lease-1", limit)
	second := acquireLease(t, ctx, svc, "req-lease-2", limit)
	if first.GetLease() == nil || second.GetLease() == nil {
		t.Fatalf("expected first two leases to be created")
	}

	third, err := svc.AcquireLease(ctx, &quotav1.AcquireLeaseRequest{
		RequestId:  "req-lease-3",
		Context:    &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:     limit.GetAction(),
		Limits:     []*quotav1.Limit{limit},
		LeaseTtlMs: 60000,
	})
	if err != nil {
		t.Fatalf("third acquire: %v", err)
	}
	if third.GetDecision().GetAllowed() || third.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_CONCURRENCY_EXCEEDED {
		t.Fatalf("expected concurrency denial, got %v", third.GetDecision())
	}

	renewed, err := svc.RenewLease(ctx, &quotav1.RenewLeaseRequest{
		RequestId:   "req-renew-1",
		LeaseId:     first.GetLease().GetLeaseId(),
		ExtendTtlMs: 120000,
	})
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	if !renewed.GetRenewed() || renewed.GetLease().GetExpiresAtUnixMs() <= first.GetLease().GetExpiresAtUnixMs() {
		t.Fatalf("lease was not extended: before=%v after=%v", first.GetLease(), renewed.GetLease())
	}

	released, err := svc.ReleaseLease(ctx, &quotav1.ReleaseLeaseRequest{
		RequestId: "req-release-lease-1",
		LeaseId:   first.GetLease().GetLeaseId(),
	})
	if err != nil {
		t.Fatalf("release lease: %v", err)
	}
	if !released.GetReleased() {
		t.Fatalf("expected lease release: %v", released)
	}

	fourth := acquireLease(t, ctx, svc, "req-lease-4", limit)
	if fourth.GetLease() == nil {
		t.Fatalf("expected replacement lease after release")
	}
}

func TestLeaseExpirationAllowsReplacementWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	limit := concurrencyLimit(1)
	first, err := svc.AcquireLease(ctx, &quotav1.AcquireLeaseRequest{
		RequestId:  "req-expiring-lease-1",
		Context:    &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:     limit.GetAction(),
		Limits:     []*quotav1.Limit{limit},
		LeaseTtlMs: 100,
	})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if !first.GetDecision().GetAllowed() || first.GetLease() == nil {
		t.Fatalf("expected first lease allowed: %v", first)
	}

	time.Sleep(250 * time.Millisecond)

	second, err := svc.AcquireLease(ctx, &quotav1.AcquireLeaseRequest{
		RequestId:  "req-expiring-lease-2",
		Context:    &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:     limit.GetAction(),
		Limits:     []*quotav1.Limit{limit},
		LeaseTtlMs: 60000,
	})
	if err != nil {
		t.Fatalf("second acquire after expiry: %v", err)
	}
	if !second.GetDecision().GetAllowed() || second.GetLease() == nil {
		t.Fatalf("expected replacement lease after expiry; first=%v second=%v", first.GetLease(), second)
	}
}

func TestOperationSpecificAlgorithmsReturnInvalidDecision(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	concurrency := concurrencyLimit(1)
	consume, err := svc.Consume(ctx, &quotav1.ConsumeRequest{
		RequestId: "req-consume-with-concurrency",
		Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:    concurrency.GetAction(),
		Cost:      1,
		Limits:    []*quotav1.Limit{concurrency},
	})
	if err != nil {
		t.Fatalf("consume with concurrency limit: %v", err)
	}
	if consume.GetDecision().GetAllowed() || consume.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST {
		t.Fatalf("expected invalid consume decision for concurrency limit: %v", consume.GetDecision())
	}

	fixed := fixedDurationLimit("lease_wrong_algorithm", 2)
	lease, err := svc.AcquireLease(ctx, &quotav1.AcquireLeaseRequest{
		RequestId:  "req-lease-with-fixed-window",
		Context:    &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:     fixed.GetAction(),
		Limits:     []*quotav1.Limit{fixed},
		LeaseTtlMs: 1000,
	})
	if err != nil {
		t.Fatalf("lease with fixed window limit: %v", err)
	}
	if lease.GetDecision().GetAllowed() || lease.GetDecision().GetReason() != quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST {
		t.Fatalf("expected invalid lease decision for non-concurrency limit: %v", lease.GetDecision())
	}
}

func TestGRPCRoundTripWithRedis(t *testing.T) {
	ctx, svc, _ := newRedisBackedService(t)

	server := grpc.NewServer()
	quotav1.RegisterQuotaServiceServer(server, svc)
	reflection.Register(server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(server.Stop)
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial grpc server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := quotav1.NewQuotaServiceClient(conn)

	status, err := client.GetRedisStatus(ctx, &quotav1.GetRedisStatusRequest{})
	if err != nil {
		t.Fatalf("grpc get redis status: %v", err)
	}
	if !status.GetReachable() || status.GetMessage() != "ok" {
		t.Fatalf("unexpected redis status: %v", status)
	}

	limit := fixedDayLimit()
	valid, err := client.ValidateLimits(ctx, &quotav1.ValidateLimitsRequest{Limits: []*quotav1.Limit{limit}})
	if err != nil {
		t.Fatalf("grpc validate limits: %v", err)
	}
	if !valid.GetValid() {
		t.Fatalf("expected valid limits over grpc: %v", valid)
	}

	consume, err := client.Consume(ctx, &quotav1.ConsumeRequest{
		RequestId: "req-grpc-consume",
		Context:   &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:    limit.GetAction(),
		Cost:      1,
		Limits:    []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("grpc consume: %v", err)
	}
	if !consume.GetDecision().GetAllowed() {
		t.Fatalf("expected grpc consume allowed: %v", consume.GetDecision())
	}
}

func newRedisBackedService(t *testing.T) (context.Context, *Server, *rlredis.Backend) {
	t.Helper()
	redisURL := os.Getenv("QUOTA_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set QUOTA_TEST_REDIS_URL to run Redis integration tests")
	}
	parsed, err := url.Parse(redisURL)
	if err != nil {
		t.Fatalf("parse QUOTA_TEST_REDIS_URL: %v", err)
	}
	parsed.Path = "/1"

	ctx := context.Background()
	store, err := rlredis.New(ctx, parsed.String())
	if err != nil {
		t.Fatalf("new redis store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.FlushAll(ctx); err != nil {
		t.Fatalf("flush redis: %v", err)
	}

	svc, err := New(ctx, Options{
		Product:     "workspace",
		Environment: "test",
		Backend:     store,
	})
	if err != nil {
		t.Fatalf("ratelimiterserver.New: %v", err)
	}
	return ctx, svc, store
}

func fixedDayLimit() *quotav1.Limit {
	return fixedDayLimitWithLimit(30)
}

func fixedDayLimitWithLimit(limit int64) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:   "user_email_recipients_daily",
		ScopeKey:  "user:user_123",
		Action:    "workspace.email.recipients",
		Unit:      "recipients",
		Algorithm: quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR,
		Window: &quotav1.Window{
			Type:         quotav1.WindowType_WINDOW_TYPE_CALENDAR,
			CalendarUnit: quotav1.CalendarUnit_CALENDAR_UNIT_DAY,
			Timezone:     "UTC",
		},
		Limit: limit,
	}
}

func fixedDurationLimit(name string, limit int64) *quotav1.Limit {
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

func slidingLimit(name string, limit int64) *quotav1.Limit {
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

func bucketLimit(name string, algorithm quotav1.Algorithm, limit, burst int64, refill float64) *quotav1.Limit {
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

func gcraLimit(name string) *quotav1.Limit {
	return bucketLimit(name, quotav1.Algorithm_ALGORITHM_GCRA, 1, 1, 1)
}

func concurrencyLimit(limit int64) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:   "user_llm_concurrency",
		ScopeKey:  "user:user_123",
		Action:    "assistant.llm.concurrent",
		Unit:      "requests",
		Algorithm: quotav1.Algorithm_ALGORITHM_CONCURRENCY,
		Limit:     limit,
	}
}

func assertUsed(t *testing.T, ctx context.Context, svc *Server, limit *quotav1.Limit, want int64) { //nolint:revive // test helper: *testing.T conventionally leads the parameter list
	t.Helper()
	usage, err := svc.GetCurrentUsage(ctx, &quotav1.GetCurrentUsageRequest{
		Context: &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:  limit.GetAction(),
		Limits:  []*quotav1.Limit{limit},
	})
	if err != nil {
		t.Fatalf("current usage: %v", err)
	}
	if got := usage.GetLimitStatuses()[0].GetUsed(); got != want {
		t.Fatalf("current usage = %d, want %d", got, want)
	}
}

func serviceMetrics(t *testing.T, svc *Server) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	svc.metrics.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

func requireMetric(t *testing.T, body, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Fatalf("metrics output missing %q\n%s", want, body)
	}
}

func acquireLease(t *testing.T, ctx context.Context, svc *Server, requestID string, limit *quotav1.Limit) *quotav1.AcquireLeaseResponse { //nolint:revive // test helper: *testing.T conventionally leads the parameter list
	t.Helper()
	resp, err := svc.AcquireLease(ctx, &quotav1.AcquireLeaseRequest{
		RequestId:  requestID,
		Context:    &quotav1.RequestContext{Product: "workspace", Environment: "test"},
		Action:     limit.GetAction(),
		Limits:     []*quotav1.Limit{limit},
		LeaseTtlMs: 60000,
	})
	if err != nil {
		t.Fatalf("acquire lease %s: %v", requestID, err)
	}
	if !resp.GetDecision().GetAllowed() {
		t.Fatalf("expected lease %s allowed: %v", requestID, resp.GetDecision())
	}
	return resp
}
