package redis

import (
	"context"
	"embed"
	"errors"
	"math"
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

func TestLoadScriptsReturnsEmbeddedReadError(t *testing.T) {
	original := scriptFS
	var empty embed.FS
	scriptFS = empty
	t.Cleanup(func() { scriptFS = original })

	err := (&Backend{}).LoadScripts(context.Background())
	if err == nil {
		t.Fatal("expected missing embedded script error")
	}
	if !strings.Contains(err.Error(), "scripts/consume.lua") {
		t.Fatalf("LoadScripts error = %v, want missing consume script", err)
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

func TestNewReturnsScriptLoadErrorWithRedis(t *testing.T) {
	adminURL := os.Getenv("QUOTA_TEST_REDIS_URL")
	if adminURL == "" {
		t.Skip("set QUOTA_TEST_REDIS_URL to run Redis integration tests")
	}
	parsed, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("parse QUOTA_TEST_REDIS_URL: %v", err)
	}
	parsed.Path = "/2"

	ctx := context.Background()
	opts, err := redisclient.ParseURL(parsed.String())
	if err != nil {
		t.Fatalf("parse admin redis URL: %v", err)
	}
	admin := redisclient.NewClient(opts)
	t.Cleanup(func() { _ = admin.Close() })

	username := "quota_test_no_script_" + strings.NewReplacer("-", "_", ".", "_", ":", "_").Replace(t.Name())
	password := "test-password"
	if err := admin.Do(ctx, "ACL", "SETUSER", username, "reset", "on", ">"+password, "~*", "+@all", "-script|load").Err(); err != nil {
		t.Fatalf("create restricted Redis user: %v", err)
	}
	t.Cleanup(func() { _ = admin.Do(context.Background(), "ACL", "DELUSER", username).Err() })

	parsed.User = url.UserPassword(username, password)
	store, err := New(ctx, parsed.String())
	if err == nil {
		_ = store.Close()
		t.Fatal("expected script load permission error")
	}
	if store != nil {
		t.Fatalf("New returned store %v with error %v, want nil store", store, err)
	}
	if !strings.Contains(err.Error(), "load consume.lua") {
		t.Fatalf("New error = %v, want script load failure", err)
	}
}

func TestRedisHealthHelpersWithRedis(t *testing.T) {
	ctx, store := newRedisTestBackend(t)

	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	loaded, err := store.ScriptsLoaded(ctx)
	if err != nil {
		t.Fatalf("ScriptsLoaded: %v", err)
	}
	if !loaded {
		t.Fatal("ScriptsLoaded = false, want true")
	}

	if err := store.FlushScripts(ctx); err != nil {
		t.Fatalf("FlushScripts: %v", err)
	}
	loaded, err = store.ScriptsLoaded(ctx)
	if err != nil {
		t.Fatalf("ScriptsLoaded after flush: %v", err)
	}
	if loaded {
		t.Fatal("ScriptsLoaded after flush = true, want false")
	}

	store.mu.Lock()
	store.scripts = map[string]string{}
	store.mu.Unlock()
	loaded, err = store.ScriptsLoaded(ctx)
	if err != nil {
		t.Fatalf("ScriptsLoaded empty map: %v", err)
	}
	if loaded {
		t.Fatal("ScriptsLoaded empty map = true, want false")
	}

	if err := store.LoadScripts(ctx); err != nil {
		t.Fatalf("reload scripts: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.ScriptsLoaded(canceled); err == nil {
		t.Fatal("expected canceled context to fail ScriptsLoaded")
	}
}

func TestLoadScriptsFailsWhenClientClosedWithRedis(t *testing.T) {
	ctx, store := newRedisTestBackend(t)

	if err := store.client.Close(); err != nil {
		t.Fatalf("close redis client: %v", err)
	}
	if err := store.LoadScripts(ctx); err == nil {
		t.Fatal("expected closed client to fail LoadScripts")
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

func TestRedisReadHelpersPropagateContextErrorsWithRedis(t *testing.T) {
	ctx, store := newRedisTestBackend(t)
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := store.CounterValue(canceled, "counter:canceled"); err == nil {
		t.Fatal("expected CounterValue to fail with canceled context")
	}
	if _, err := store.BucketState(canceled, "bucket:canceled"); err == nil {
		t.Fatal("expected BucketState to fail with canceled context")
	}
	if _, err := store.ConcurrencyCount(canceled, "lease-set:canceled", time.Now()); err == nil {
		t.Fatal("expected ConcurrencyCount to fail with canceled context")
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

func TestScriptMethodsRejectUnmarshalableLimitOps(t *testing.T) {
	store := &Backend{}
	now := time.Unix(200, 0).UTC()
	badOp := backend.LimitOp{
		LimitID:          "bad-json",
		RefillRatePerSec: math.NaN(),
	}
	reservation := &quotav1.Reservation{
		ReservationId:   "reservation-bad-json",
		CreatedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(time.Minute).UnixMilli(),
	}
	lease := &quotav1.Lease{
		LeaseId:         "lease-bad-json",
		CreatedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(time.Minute).UnixMilli(),
	}

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "consume",
			call: func() error {
				_, err := store.Consume(context.Background(), "idem-bad-json-consume", now, []backend.LimitOp{badOp}, false, "decision-bad-json-consume")
				return err
			},
		},
		{
			name: "reserve",
			call: func() error {
				_, err := store.Reserve(context.Background(), "idem-bad-json-reserve", now, []backend.LimitOp{badOp}, false, "reservation:bad-json", "reservation-expiry:bad-json", reservation, "decision-bad-json-reserve")
				return err
			},
		},
		{
			name: "acquire_lease",
			call: func() error {
				_, err := store.AcquireLease(context.Background(), "idem-bad-json-acquire", "lease:bad-json", lease, time.Minute, []backend.LimitOp{badOp}, false, "decision-bad-json-acquire")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err == nil {
				t.Fatal("expected unmarshalable limit operation to fail")
			}
		})
	}
}

func TestScriptMethodsRejectInvalidProtoJSONPayloads(t *testing.T) {
	store := &Backend{}
	now := time.Unix(200, 0).UTC()
	op := validLimitOp(now, "nil-proto")
	invalidUTF8 := string([]byte{0xff})
	reservation := validReservation(now, invalidUTF8)
	lease := validLease(now, invalidUTF8)

	if _, err := store.Reserve(context.Background(), "idem-invalid-reservation-json", now, []backend.LimitOp{op}, false, "reservation:invalid-json", "reservation-expiry:invalid-json", reservation, "decision-invalid-reservation-json"); err == nil {
		t.Fatal("expected invalid reservation JSON to fail Reserve")
	}
	if _, err := store.AcquireLease(context.Background(), "idem-invalid-lease-json", "lease:invalid-json", lease, time.Minute, []backend.LimitOp{op}, false, "decision-invalid-lease-json"); err == nil {
		t.Fatal("expected invalid lease JSON to fail AcquireLease")
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

func TestScriptMethodsRejectMalformedScriptJSONWithRedis(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	op := validLimitOp(now, "malformed-json")
	reservation := validReservation(now, "reservation-malformed-json")
	lease := validLease(now, "lease-malformed-json")

	tests := []struct {
		name   string
		script string
		call   func(context.Context, *Backend) error
	}{
		{
			name:   "consume",
			script: scriptConsume,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.Consume(ctx, "idem-malformed-consume", now, []backend.LimitOp{op}, false, "decision-malformed-consume")
				return err
			},
		},
		{
			name:   "reserve",
			script: scriptReserve,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.Reserve(ctx, "idem-malformed-reserve", now, []backend.LimitOp{op}, false, "reservation:malformed", "reservation-expiry:malformed", reservation, "decision-malformed-reserve")
				return err
			},
		},
		{
			name:   "increment_reservation",
			script: scriptIncrementReservation,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.IncrementReservation(ctx, "idem-malformed-increment", "reservation:malformed", "reservation-expiry:malformed", 1, now, "decision-malformed-increment")
				return err
			},
		},
		{
			name:   "finalize_reservation",
			script: scriptFinalizeReservation,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.FinalizeReservation(ctx, "idem-malformed-finalize", "reservation:malformed", "reservation-expiry:malformed", 1, now)
				return err
			},
		},
		{
			name:   "release_reservation",
			script: scriptReleaseReservation,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.ReleaseReservation(ctx, "idem-malformed-release-reservation", "reservation:malformed", "reservation-expiry:malformed", now)
				return err
			},
		},
		{
			name:   "expire_reservations",
			script: scriptExpireReservations,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.ExpireReservations(ctx, "reservation-expiry:malformed", now, 100)
				return err
			},
		},
		{
			name:   "acquire_lease",
			script: scriptAcquireLease,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.AcquireLease(ctx, "idem-malformed-acquire", "lease:malformed", lease, time.Minute, []backend.LimitOp{op}, false, "decision-malformed-acquire")
				return err
			},
		},
		{
			name:   "renew_lease",
			script: scriptRenewLease,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.RenewLease(ctx, "idem-malformed-renew", "lease:malformed", lease.GetLeaseId(), time.Minute, now)
				return err
			},
		},
		{
			name:   "release_lease",
			script: scriptReleaseLease,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.ReleaseLease(ctx, "idem-malformed-release-lease", "lease:malformed", lease.GetLeaseId())
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, store := newRedisTestBackend(t)
			replaceScript(ctx, t, store, tt.script, `return "{"`)
			if err := tt.call(ctx, store); err == nil {
				t.Fatal("expected malformed script JSON to fail")
			}
		})
	}
}

func TestReservationScriptMethodsRejectInvalidReservationPayloadWithRedis(t *testing.T) {
	now := time.Unix(200, 0).UTC()

	tests := []struct {
		name   string
		script string
		body   string
		call   func(context.Context, *Backend) error
	}{
		{
			name:   "increment_reservation",
			script: scriptIncrementReservation,
			body:   `return '{"found":true,"active":true,"reserved_cost":1,"decision":{"allowed":true},"reservation":{"status":"RESERVATION_STATUS_NOT_REAL"}}'`,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.IncrementReservation(ctx, "idem-invalid-reservation-increment", "reservation:invalid", "reservation-expiry:invalid", 1, now, "decision-invalid-reservation-increment")
				return err
			},
		},
		{
			name:   "finalize_reservation",
			script: scriptFinalizeReservation,
			body:   `return '{"found":true,"finalized":true,"reservation":{"status":"RESERVATION_STATUS_NOT_REAL"}}'`,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.FinalizeReservation(ctx, "idem-invalid-reservation-finalize", "reservation:invalid", "reservation-expiry:invalid", 1, now)
				return err
			},
		},
		{
			name:   "release_reservation",
			script: scriptReleaseReservation,
			body:   `return '{"found":true,"released":true,"reservation":{"status":"RESERVATION_STATUS_NOT_REAL"}}'`,
			call: func(ctx context.Context, store *Backend) error {
				_, err := store.ReleaseReservation(ctx, "idem-invalid-reservation-release", "reservation:invalid", "reservation-expiry:invalid", now)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, store := newRedisTestBackend(t)
			replaceScript(ctx, t, store, tt.script, tt.body)
			if err := tt.call(ctx, store); err == nil {
				t.Fatal("expected invalid reservation payload to fail")
			}
		})
	}
}

func TestEvalTextReturnsReloadErrorsWithRedis(t *testing.T) {
	ctx, store := newRedisTestBackend(t)
	if err := store.FlushScripts(ctx); err != nil {
		t.Fatalf("FlushScripts: %v", err)
	}
	setScriptSHAAndBody(store, scriptConsume, "0000000000000000000000000000000000000000", "this is not lua")

	now := time.Unix(200, 0).UTC()
	op := validLimitOp(now, "reload-error")
	_, err := store.Consume(ctx, "idem-reload-error", now, []backend.LimitOp{op}, false, "decision-reload-error")
	if err == nil {
		t.Fatal("expected script reload to fail")
	}
	if !strings.Contains(err.Error(), "reload consume.lua after NOSCRIPT") {
		t.Fatalf("reload error = %v, want consume reload context", err)
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

func replaceScript(ctx context.Context, t *testing.T, store *Backend, script, body string) {
	t.Helper()
	sha, err := store.client.ScriptLoad(ctx, body).Result()
	if err != nil {
		t.Fatalf("load replacement script %s: %v", script, err)
	}
	setScriptSHAAndBody(store, script, sha, body)
}

func setScriptSHAAndBody(store *Backend, script, sha, body string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.scripts[script] = sha
	store.scriptBodies[script] = body
}

func validLimitOp(now time.Time, suffix string) backend.LimitOp {
	return backend.LimitOp{
		LimitID:       "limit-" + suffix,
		Kind:          "counter",
		ReadKeys:      []string{"counter:" + suffix},
		WriteKey:      "counter:" + suffix,
		Limit:         10,
		Cost:          1,
		ResetAtUnixMs: now.Add(time.Minute).UnixMilli(),
		TTLMS:         int64(time.Minute / time.Millisecond),
	}
}

func validReservation(now time.Time, id string) *quotav1.Reservation {
	return &quotav1.Reservation{
		ReservationId:   id,
		Action:          "test.reserve",
		ReservedCost:    1,
		CreatedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(time.Minute).UnixMilli(),
		Status:          quotav1.ReservationStatus_RESERVATION_STATUS_ACTIVE,
	}
}

func validLease(now time.Time, id string) *quotav1.Lease {
	return &quotav1.Lease{
		LeaseId:         id,
		Action:          "test.lease",
		CreatedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(time.Minute).UnixMilli(),
		Status:          quotav1.LeaseStatus_LEASE_STATUS_ACTIVE,
	}
}
