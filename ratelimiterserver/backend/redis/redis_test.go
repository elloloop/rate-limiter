package redis

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	"github.com/elloloop/rate-limiter/ratelimiterserver/backend"
)

func TestRequiredScriptsAreEmbedded(t *testing.T) {
	required := []string{
		"consume.lua",
		"reserve.lua",
		"increment_reservation.lua",
		"finalize_reservation.lua",
		"release_reservation.lua",
		"expire_reservations.lua",
		"acquire_lease.lua",
		"renew_lease.lua",
		"release_lease.lua",
	}

	for _, name := range required {
		body, err := scriptFS.ReadFile("scripts/" + name)
		if err != nil {
			t.Fatalf("missing script %s: %v", name, err)
		}
		if !strings.Contains(string(body), "redis.call") {
			t.Fatalf("script %s does not appear to call Redis", name)
		}
	}
}

func TestBoolArg(t *testing.T) {
	if got := boolArg(true); got != "1" {
		t.Fatalf("true encoded as %q", got)
	}
	if got := boolArg(false); got != "0" {
		t.Fatalf("false encoded as %q", got)
	}
}

func TestNewRejectsInvalidRedisURL(t *testing.T) {
	_, err := New(context.Background(), "://not-a-url")
	if err == nil {
		t.Fatal("expected invalid redis URL error")
	}
}

func TestNewRejectsUnreachableRedis(t *testing.T) {
	// Port 1 is reserved (tcpmux) and effectively never accepts
	// connections, so this exercises the synchronous ping at
	// construction without depending on test orchestration.
	_, err := New(context.Background(), "redis://127.0.0.1:1/0")
	if err == nil {
		t.Fatal("expected unreachable Redis to fail New")
	}
	if !strings.Contains(err.Error(), "ping redis") {
		t.Fatalf("expected ping error, got: %v", err)
	}
}

func TestRedisReadHelpersWithRedis(t *testing.T) {
	ctx, store := newRedisTestBackend(t)

	lease := &quotav1.Lease{
		LeaseId:         "lease-read",
		Action:          "assistant.llm.concurrent",
		CreatedAtUnixMs: time.Unix(100, 0).UnixMilli(),
		ExpiresAtUnixMs: time.Unix(160, 0).UnixMilli(),
		Status:          quotav1.LeaseStatus_LEASE_STATUS_ACTIVE,
	}
	encodedLease, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(lease)
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}
	if err := store.client.Set(ctx, "lease:active", encodedLease, time.Minute).Err(); err != nil {
		t.Fatalf("set lease: %v", err)
	}

	gotLease, err := store.GetLease(ctx, "lease:active")
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if gotLease.GetLeaseId() != lease.GetLeaseId() || gotLease.GetStatus() != quotav1.LeaseStatus_LEASE_STATUS_ACTIVE {
		t.Fatalf("unexpected lease: %v", gotLease)
	}

	_, err = store.GetLease(ctx, "lease:missing")
	if !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("missing lease error = %v, want backend.ErrNotFound", err)
	}

	if err := store.client.Set(ctx, "lease:invalid", "{", time.Minute).Err(); err != nil {
		t.Fatalf("set invalid lease: %v", err)
	}
	if _, err := store.GetLease(ctx, "lease:invalid"); err == nil {
		t.Fatal("expected invalid stored lease JSON to fail")
	}

	missingBucket, err := store.BucketState(ctx, "bucket:missing")
	if err != nil {
		t.Fatalf("missing BucketState: %v", err)
	}
	if missingBucket.Exists || missingBucket.Tokens != 0 || missingBucket.LastRefillMs != 0 {
		t.Fatalf("unexpected missing bucket state: %#v", missingBucket)
	}

	if err := store.client.HSet(ctx, "bucket:present", "tokens", "3.5", "last_refill_ms", "12345").Err(); err != nil {
		t.Fatalf("hset bucket: %v", err)
	}
	bucket, err := store.BucketState(ctx, "bucket:present")
	if err != nil {
		t.Fatalf("BucketState: %v", err)
	}
	if !bucket.Exists || bucket.Tokens != 3.5 || bucket.LastRefillMs != 12345 {
		t.Fatalf("unexpected bucket state: %#v", bucket)
	}

	if err := store.client.HSet(ctx, "bucket:partial", "tokens", "not-a-float", "last_refill_ms", "23456").Err(); err != nil {
		t.Fatalf("hset partial bucket: %v", err)
	}
	partial, err := store.BucketState(ctx, "bucket:partial")
	if err != nil {
		t.Fatalf("partial BucketState: %v", err)
	}
	if !partial.Exists || partial.Tokens != 0 || partial.LastRefillMs != 23456 {
		t.Fatalf("unexpected partial bucket state: %#v", partial)
	}

	value, ok, err := store.GCRAValue(ctx, "gcra:missing")
	if err != nil {
		t.Fatalf("missing GCRAValue: %v", err)
	}
	if ok || value != 0 {
		t.Fatalf("missing GCRAValue = value %v ok %v, want zero/false", value, ok)
	}

	if err := store.client.Set(ctx, "gcra:present", "456.75", time.Minute).Err(); err != nil {
		t.Fatalf("set gcra: %v", err)
	}
	value, ok, err = store.GCRAValue(ctx, "gcra:present")
	if err != nil {
		t.Fatalf("GCRAValue: %v", err)
	}
	if !ok || value != 456.75 {
		t.Fatalf("GCRAValue = value %v ok %v, want 456.75/true", value, ok)
	}

	if err := store.client.Set(ctx, "gcra:invalid", "not-a-float", time.Minute).Err(); err != nil {
		t.Fatalf("set invalid gcra: %v", err)
	}
	if _, _, err := store.GCRAValue(ctx, "gcra:invalid"); err == nil {
		t.Fatal("expected invalid stored GCRA value to fail")
	}
}

func TestRedisReservationReadHelperWithRedis(t *testing.T) {
	ctx, store := newRedisTestBackend(t)

	reservation := &quotav1.Reservation{
		ReservationId:   "reservation-read",
		Action:          "assistant.llm.tokens",
		ReservedCost:    12,
		CreatedAtUnixMs: time.Unix(100, 0).UnixMilli(),
		ExpiresAtUnixMs: time.Unix(160, 0).UnixMilli(),
		Status:          quotav1.ReservationStatus_RESERVATION_STATUS_ACTIVE,
	}
	encodedReservation, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(reservation)
	if err != nil {
		t.Fatalf("marshal reservation: %v", err)
	}
	if err := store.client.Set(ctx, "reservation:active", encodedReservation, time.Minute).Err(); err != nil {
		t.Fatalf("set reservation: %v", err)
	}

	gotReservation, err := store.GetReservation(ctx, "reservation:active")
	if err != nil {
		t.Fatalf("GetReservation: %v", err)
	}
	if gotReservation.GetReservationId() != reservation.GetReservationId() || gotReservation.GetReservedCost() != 12 {
		t.Fatalf("unexpected reservation: %v", gotReservation)
	}

	_, err = store.GetReservation(ctx, "reservation:missing")
	if !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("missing reservation error = %v, want backend.ErrNotFound", err)
	}

	if err := store.client.Set(ctx, "reservation:invalid", "{", time.Minute).Err(); err != nil {
		t.Fatalf("set invalid reservation: %v", err)
	}
	if _, err := store.GetReservation(ctx, "reservation:invalid"); err == nil {
		t.Fatal("expected invalid stored reservation JSON to fail")
	}
}

func TestScriptMethodsPropagateRedisScriptErrorsWithRedis(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	op := backend.LimitOp{
		LimitID:       "limit-script-error",
		Kind:          "counter",
		ReadKeys:      []string{"counter:script-error"},
		WriteKey:      "counter:script-error",
		Limit:         10,
		Cost:          1,
		ResetAtUnixMs: now.Add(time.Minute).UnixMilli(),
		TTLMS:         int64(time.Minute / time.Millisecond),
	}
	reservation := &quotav1.Reservation{
		ReservationId:   "reservation-script-error",
		Action:          "test.reserve",
		ReservedCost:    1,
		CreatedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(time.Minute).UnixMilli(),
		Status:          quotav1.ReservationStatus_RESERVATION_STATUS_ACTIVE,
	}
	lease := &quotav1.Lease{
		LeaseId:         "lease-script-error",
		Action:          "test.lease",
		CreatedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(time.Minute).UnixMilli(),
		Status:          quotav1.LeaseStatus_LEASE_STATUS_ACTIVE,
	}

	tests := []struct {
		name   string
		script string
		call   func(context.Context, *Backend) error
	}{
		{
			name:   "consume",
			script: scriptConsume,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.Consume(ctx, "idem-consume-error", now, []backend.LimitOp{op}, false, "decision-consume-error")
				return err
			},
		},
		{
			name:   "reserve",
			script: scriptReserve,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.Reserve(ctx, "idem-reserve-error", now, []backend.LimitOp{op}, false, "reservation:error", "reservation-expiry:error", reservation, "decision-reserve-error")
				return err
			},
		},
		{
			name:   "increment_reservation",
			script: scriptIncrementReservation,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.IncrementReservation(ctx, "idem-increment-error", "reservation:error", "reservation-expiry:error", 1, now, "decision-increment-error")
				return err
			},
		},
		{
			name:   "finalize_reservation",
			script: scriptFinalizeReservation,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.FinalizeReservation(ctx, "idem-finalize-error", "reservation:error", "reservation-expiry:error", 1, now)
				return err
			},
		},
		{
			name:   "release_reservation",
			script: scriptReleaseReservation,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.ReleaseReservation(ctx, "idem-release-reservation-error", "reservation:error", "reservation-expiry:error", now)
				return err
			},
		},
		{
			name:   "expire_reservations",
			script: scriptExpireReservations,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.ExpireReservations(ctx, "reservation-expiry:error", now, 100)
				return err
			},
		},
		{
			name:   "acquire_lease",
			script: scriptAcquireLease,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.AcquireLease(ctx, "idem-acquire-error", "lease:error", lease, time.Minute, []backend.LimitOp{op}, false, "decision-acquire-error")
				return err
			},
		},
		{
			name:   "renew_lease",
			script: scriptRenewLease,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.RenewLease(ctx, "idem-renew-error", "lease:error", lease.GetLeaseId(), time.Minute, now)
				return err
			},
		},
		{
			name:   "release_lease",
			script: scriptReleaseLease,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.ReleaseLease(ctx, "idem-release-lease-error", "lease:error", lease.GetLeaseId())
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, store := newRedisTestBackend(t)
			breakScript(store, tt.script)
			err := tt.call(ctx, store)
			if err == nil {
				t.Fatal("expected script error")
			}
			if !strings.Contains(strings.ToLower(err.Error()), "forced script failure") {
				t.Fatalf("error = %v, want forced script failure", err)
			}
		})
	}
}

func TestParseDecisionRejectsInvalidJSON(t *testing.T) {
	if _, err := parseDecision("{"); err == nil {
		t.Fatal("expected invalid decision JSON to fail")
	}
}

func TestParseLeaseResultShapes(t *testing.T) {
	result, err := parseLeaseResult(`{"cached":true,"found":true,"renewed":true,"lease":{"lease_id":"lease-1","status":"LEASE_STATUS_ACTIVE"}}`)
	if err != nil {
		t.Fatalf("parseLeaseResult: %v", err)
	}
	if !result.Cached || !result.Found || !result.Renewed || result.Lease.GetLeaseId() != "lease-1" {
		t.Fatalf("unexpected lease result: %#v", result)
	}

	result, err = parseLeaseResult(`{"found":false}`)
	if err != nil {
		t.Fatalf("parse missing lease result: %v", err)
	}
	if result.Found || result.Lease != nil {
		t.Fatalf("unexpected missing lease result: %#v", result)
	}

	if _, err := parseLeaseResult(`{"lease":{`); err == nil {
		t.Fatal("expected malformed lease result to fail")
	}
	if _, err := parseLeaseResult(`{"found":true,"lease":{"status":"LEASE_STATUS_NOT_REAL"}}`); err == nil {
		t.Fatal("expected invalid lease protobuf JSON to fail")
	}
}

func TestMapNotFound(t *testing.T) {
	if got := mapNotFound(redisclient.Nil); !errors.Is(got, backend.ErrNotFound) {
		t.Fatalf("mapNotFound(redis.Nil) = %v, want backend.ErrNotFound", got)
	}
	err := errors.New("boom")
	if got := mapNotFound(err); !errors.Is(got, err) {
		t.Fatalf("mapNotFound(non-nil) = %v, want original", got)
	}
}

func TestIsNoScriptIsCaseInsensitive(t *testing.T) {
	for _, message := range []string{"NOSCRIPT No matching script", "noscript no matching script"} {
		if !isNoScript(errors.New(message)) {
			t.Fatalf("isNoScript(%q) = false", message)
		}
	}
	if isNoScript(errors.New("WRONGTYPE")) {
		t.Fatal("WRONGTYPE should not be treated as NOSCRIPT")
	}
}

func newRedisTestBackend(t *testing.T) (context.Context, *Backend) {
	t.Helper()
	redisURL := os.Getenv("QUOTA_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set QUOTA_TEST_REDIS_URL to run Redis integration tests")
	}
	parsed, err := url.Parse(redisURL)
	if err != nil {
		t.Fatalf("parse QUOTA_TEST_REDIS_URL: %v", err)
	}
	parsed.Path = "/2"
	ctx := context.Background()
	store, err := New(ctx, parsed.String())
	if err != nil {
		t.Fatalf("New redis backend: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.FlushAll(ctx); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	return ctx, store
}

func breakScript(store *Backend, script string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.scripts[script] = "0000000000000000000000000000000000000000"
	store.scriptBodies[script] = `return redis.error_reply("forced script failure")`
}
