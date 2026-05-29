package redis

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	"github.com/elloloop/rate-limiter/ratelimiterserver/backend"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

const (
	scriptConsume              = "consume"
	scriptReserve              = "reserve"
	scriptIncrementReservation = "increment_reservation"
	scriptFinalizeReservation  = "finalize_reservation"
	scriptReleaseReservation   = "release_reservation"
	scriptExpireReservations   = "expire_reservations"
	scriptAcquireLease         = "acquire_lease"
	scriptRenewLease           = "renew_lease"
	scriptReleaseLease         = "release_lease"
	defaultIdempotencyTTL      = 24 * time.Hour
	defaultReservationExtraTTL = 24 * time.Hour
	defaultLeaseExtraTTL       = time.Hour
)

// Backend is the Redis-backed implementation of the
// backend.Backend interface. Construct it with [New].
type Backend struct {
	client       *redisclient.Client
	mu           sync.RWMutex
	scripts      map[string]string
	scriptBodies map[string]string
}

// New dials Redis at dsn, pings to confirm reachability, and
// pre-loads every Lua script the rate-limiter algorithms depend on.
// A failure at any of those steps closes the client and returns the
// underlying error so construction never silently leaves a broken
// backend.
func New(ctx context.Context, dsn string) (*Backend, error) {
	opts, err := redisclient.ParseURL(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse redis dsn: %w", err)
	}
	client := redisclient.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	b := &Backend{client: client, scripts: map[string]string{}, scriptBodies: map[string]string{}}
	if err := b.LoadScripts(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return b, nil
}

func (b *Backend) Close() error {
	return b.client.Close()
}

// FlushAll deletes every key in the connected Redis database. It
// exists solely for integration-test setup; production code must not
// call it.
func (b *Backend) FlushAll(ctx context.Context) error {
	return b.client.FlushDB(ctx).Err()
}

// FlushScripts removes every loaded script from the Redis script
// cache, forcing the next EVALSHA to fall back through the NOSCRIPT
// reload path. It exists for the integration test that exercises
// that fallback; production code must not call it.
func (b *Backend) FlushScripts(ctx context.Context) error {
	return b.client.ScriptFlush(ctx).Err()
}

func (b *Backend) LoadScripts(ctx context.Context) error {
	for _, name := range []string{
		scriptConsume,
		scriptReserve,
		scriptIncrementReservation,
		scriptFinalizeReservation,
		scriptReleaseReservation,
		scriptExpireReservations,
		scriptAcquireLease,
		scriptRenewLease,
		scriptReleaseLease,
	} {
		body, err := scriptFS.ReadFile("scripts/" + name + ".lua")
		if err != nil {
			return err
		}
		sha, err := b.client.ScriptLoad(ctx, string(body)).Result()
		if err != nil {
			return fmt.Errorf("load %s.lua: %w", name, err)
		}
		b.mu.Lock()
		b.scripts[name] = sha
		b.scriptBodies[name] = string(body)
		b.mu.Unlock()
	}
	return nil
}

func (b *Backend) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *Backend) ScriptsLoaded(ctx context.Context) (bool, error) {
	b.mu.RLock()
	shas := make([]string, 0, len(b.scripts))
	for _, sha := range b.scripts {
		shas = append(shas, sha)
	}
	b.mu.RUnlock()
	if len(shas) == 0 {
		return false, nil
	}
	exists, err := b.client.ScriptExists(ctx, shas...).Result()
	if err != nil {
		return false, err
	}
	for _, ok := range exists {
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (b *Backend) Consume(ctx context.Context, idemKey string, now time.Time, ops []backend.LimitOp, dryRun bool, decisionID string) (backend.DecisionResult, error) {
	args, err := json.Marshal(ops)
	if err != nil {
		return backend.DecisionResult{}, err
	}
	raw, err := b.evalText(ctx, scriptConsume, nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		now.UnixMilli(),
		boolArg(dryRun),
		string(args),
		decisionID,
	)
	if err != nil {
		return backend.DecisionResult{}, err
	}
	return parseDecision(raw)
}

func (b *Backend) Reserve(ctx context.Context, idemKey string, now time.Time, ops []backend.LimitOp, dryRun bool, reservationKey, reservationExpiryIndexKey string, reservation *quotav1.Reservation, decisionID string) (backend.DecisionResult, error) {
	args, err := json.Marshal(ops)
	if err != nil {
		return backend.DecisionResult{}, err
	}
	reservationJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(reservation)
	if err != nil {
		return backend.DecisionResult{}, err
	}
	ttl := time.Until(time.UnixMilli(reservation.GetExpiresAtUnixMs())) + defaultReservationExtraTTL
	if ttl < defaultReservationExtraTTL {
		ttl = defaultReservationExtraTTL
	}
	raw, err := b.evalText(ctx, scriptReserve, nil,
		idemKey,
		millis(ttl),
		now.UnixMilli(),
		boolArg(dryRun),
		string(args),
		reservationKey,
		reservationExpiryIndexKey,
		string(reservationJSON),
		millis(ttl),
		decisionID,
	)
	if err != nil {
		return backend.DecisionResult{}, err
	}
	return parseDecision(raw)
}

func (b *Backend) IncrementReservation(ctx context.Context, idemKey, reservationKey, reservationExpiryIndexKey string, deltaCost int64, now time.Time, decisionID string) (backend.IncrementReservationResult, error) {
	raw, err := b.evalText(ctx, scriptIncrementReservation, nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		reservationKey,
		reservationExpiryIndexKey,
		deltaCost,
		now.UnixMilli(),
		decisionID,
	)
	if err != nil {
		return backend.IncrementReservationResult{}, err
	}
	var envelope struct {
		Cached       bool                   `json:"cached"`
		Found        bool                   `json:"found"`
		Active       bool                   `json:"active"`
		ReservedCost int64                  `json:"reserved_cost"`
		Decision     backend.DecisionResult `json:"decision"`
		Reservation  json.RawMessage        `json:"reservation"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return backend.IncrementReservationResult{}, err
	}
	result := backend.IncrementReservationResult{
		Cached:       envelope.Cached,
		Found:        envelope.Found,
		Active:       envelope.Active,
		ReservedCost: envelope.ReservedCost,
		Decision:     envelope.Decision,
	}
	if len(envelope.Reservation) > 0 {
		result.Reservation = &quotav1.Reservation{}
		if err := protojson.Unmarshal(envelope.Reservation, result.Reservation); err != nil {
			return backend.IncrementReservationResult{}, err
		}
	}
	return result, nil
}

func (b *Backend) FinalizeReservation(ctx context.Context, idemKey, reservationKey, reservationExpiryIndexKey string, actualCost int64, now time.Time) (backend.FinalizeResult, error) {
	raw, err := b.evalText(ctx, scriptFinalizeReservation, nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		reservationKey,
		reservationExpiryIndexKey,
		actualCost,
		now.UnixMilli(),
	)
	if err != nil {
		return backend.FinalizeResult{}, err
	}
	var envelope struct {
		Cached       bool            `json:"cached"`
		Found        bool            `json:"found"`
		Finalized    bool            `json:"finalized"`
		ReservedCost int64           `json:"reserved_cost"`
		ActualCost   int64           `json:"actual_cost"`
		RefundedCost int64           `json:"refunded_cost"`
		OverageCost  int64           `json:"overage_cost"`
		Reservation  json.RawMessage `json:"reservation"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return backend.FinalizeResult{}, err
	}
	result := backend.FinalizeResult{
		Cached:       envelope.Cached,
		Found:        envelope.Found,
		Finalized:    envelope.Finalized,
		ReservedCost: envelope.ReservedCost,
		ActualCost:   envelope.ActualCost,
		RefundedCost: envelope.RefundedCost,
		OverageCost:  envelope.OverageCost,
	}
	if len(envelope.Reservation) > 0 {
		result.Reservation = &quotav1.Reservation{}
		if err := protojson.Unmarshal(envelope.Reservation, result.Reservation); err != nil {
			return backend.FinalizeResult{}, err
		}
	}
	return result, nil
}

func (b *Backend) ReleaseReservation(ctx context.Context, idemKey, reservationKey, reservationExpiryIndexKey string, now time.Time) (backend.ReleaseReservationResult, error) {
	raw, err := b.evalText(ctx, scriptReleaseReservation, nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		reservationKey,
		reservationExpiryIndexKey,
		now.UnixMilli(),
	)
	if err != nil {
		return backend.ReleaseReservationResult{}, err
	}
	var envelope struct {
		Cached       bool            `json:"cached"`
		Found        bool            `json:"found"`
		Released     bool            `json:"released"`
		ReleasedCost int64           `json:"released_cost"`
		Reservation  json.RawMessage `json:"reservation"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return backend.ReleaseReservationResult{}, err
	}
	result := backend.ReleaseReservationResult{
		Cached:       envelope.Cached,
		Found:        envelope.Found,
		Released:     envelope.Released,
		ReleasedCost: envelope.ReleasedCost,
	}
	if len(envelope.Reservation) > 0 {
		result.Reservation = &quotav1.Reservation{}
		if err := protojson.Unmarshal(envelope.Reservation, result.Reservation); err != nil {
			return backend.ReleaseReservationResult{}, err
		}
	}
	return result, nil
}

func (b *Backend) ExpireReservations(ctx context.Context, reservationExpiryIndexKey string, now time.Time, batchSize int64) (backend.ExpireReservationsResult, error) {
	raw, err := b.evalText(ctx, scriptExpireReservations, nil,
		reservationExpiryIndexKey,
		now.UnixMilli(),
		batchSize,
	)
	if err != nil {
		return backend.ExpireReservationsResult{}, err
	}
	var result backend.ExpireReservationsResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return backend.ExpireReservationsResult{}, err
	}
	return result, nil
}

func (b *Backend) AcquireLease(ctx context.Context, idemKey, leaseKey string, lease *quotav1.Lease, leaseTTL time.Duration, ops []backend.LimitOp, dryRun bool, decisionID string) (backend.DecisionResult, error) {
	args, err := json.Marshal(ops)
	if err != nil {
		return backend.DecisionResult{}, err
	}
	leaseJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(lease)
	if err != nil {
		return backend.DecisionResult{}, err
	}
	raw, err := b.evalText(ctx, scriptAcquireLease, nil,
		idemKey,
		millis(leaseTTL+defaultLeaseExtraTTL),
		leaseKey,
		string(leaseJSON),
		lease.GetLeaseId(),
		millis(leaseTTL),
		lease.GetCreatedAtUnixMs(),
		lease.GetExpiresAtUnixMs(),
		boolArg(dryRun),
		string(args),
		decisionID,
	)
	if err != nil {
		return backend.DecisionResult{}, err
	}
	return parseDecision(raw)
}

func (b *Backend) RenewLease(ctx context.Context, idemKey, leaseKey, leaseID string, extendTTL time.Duration, now time.Time) (backend.LeaseResult, error) {
	raw, err := b.evalText(ctx, scriptRenewLease, nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		leaseKey,
		leaseID,
		millis(extendTTL),
		now.UnixMilli(),
	)
	if err != nil {
		return backend.LeaseResult{}, err
	}
	return parseLeaseResult(raw)
}

func (b *Backend) ReleaseLease(ctx context.Context, idemKey, leaseKey, leaseID string) (backend.LeaseResult, error) {
	raw, err := b.evalText(ctx, scriptReleaseLease, nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		leaseKey,
		leaseID,
	)
	if err != nil {
		return backend.LeaseResult{}, err
	}
	return parseLeaseResult(raw)
}

func (b *Backend) GetReservation(ctx context.Context, key string) (*quotav1.Reservation, error) {
	raw, err := b.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, mapNotFound(err)
	}
	res := &quotav1.Reservation{}
	if err := protojson.Unmarshal(raw, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (b *Backend) GetLease(ctx context.Context, key string) (*quotav1.Lease, error) {
	raw, err := b.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, mapNotFound(err)
	}
	lease := &quotav1.Lease{}
	if err := protojson.Unmarshal(raw, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

func (b *Backend) CounterValue(ctx context.Context, key string) (int64, error) {
	value, err := b.client.Get(ctx, key).Int64()
	if errors.Is(err, redisclient.Nil) {
		return 0, nil
	}
	return value, err
}

func (b *Backend) BucketState(ctx context.Context, key string) (backend.BucketState, error) {
	fields, err := b.client.HMGet(ctx, key, "tokens", "last_refill_ms").Result()
	if err != nil {
		return backend.BucketState{}, err
	}
	state := backend.BucketState{}
	if fields[0] != nil {
		tokens, parseErr := strconv.ParseFloat(fmt.Sprint(fields[0]), 64)
		if parseErr == nil {
			state.Tokens = tokens
			state.Exists = true
		}
	}
	if fields[1] != nil {
		last, parseErr := strconv.ParseInt(fmt.Sprint(fields[1]), 10, 64)
		if parseErr == nil {
			state.LastRefillMs = last
			state.Exists = true
		}
	}
	return state, nil
}

func (b *Backend) GCRAValue(ctx context.Context, key string) (float64, bool, error) {
	raw, err := b.client.Get(ctx, key).Float64()
	if errors.Is(err, redisclient.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return raw, true, nil
}

func (b *Backend) ConcurrencyCount(ctx context.Context, key string, now time.Time) (int64, error) {
	return b.client.ZCount(ctx, key, strconv.FormatInt(now.UnixMilli(), 10), "+inf").Result()
}

func parseDecision(raw string) (backend.DecisionResult, error) {
	var result backend.DecisionResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return backend.DecisionResult{}, err
	}
	return result, nil
}

func parseLeaseResult(raw string) (backend.LeaseResult, error) {
	var envelope struct {
		Cached   bool            `json:"cached"`
		Found    bool            `json:"found"`
		Renewed  bool            `json:"renewed"`
		Released bool            `json:"released"`
		Lease    json.RawMessage `json:"lease"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return backend.LeaseResult{}, err
	}
	result := backend.LeaseResult{
		Cached:   envelope.Cached,
		Found:    envelope.Found,
		Renewed:  envelope.Renewed,
		Released: envelope.Released,
	}
	if len(envelope.Lease) > 0 {
		result.Lease = &quotav1.Lease{}
		if err := protojson.Unmarshal(envelope.Lease, result.Lease); err != nil {
			return backend.LeaseResult{}, err
		}
	}
	return result, nil
}

func (b *Backend) evalText(ctx context.Context, scriptName string, keys []string, args ...any) (string, error) {
	b.mu.RLock()
	sha := b.scripts[scriptName]
	b.mu.RUnlock()

	raw, err := b.client.EvalSha(ctx, sha, keys, args...).Text()
	if err == nil || !isNoScript(err) {
		return raw, err
	}

	b.mu.Lock()
	body := b.scriptBodies[scriptName]
	loadedSHA, loadErr := b.client.ScriptLoad(ctx, body).Result()
	if loadErr == nil {
		b.scripts[scriptName] = loadedSHA
		sha = loadedSHA
	}
	b.mu.Unlock()
	if loadErr != nil {
		return "", fmt.Errorf("reload %s.lua after NOSCRIPT: %w", scriptName, loadErr)
	}

	return b.client.EvalSha(ctx, sha, keys, args...).Text()
}

func mapNotFound(err error) error {
	if errors.Is(err, redisclient.Nil) {
		return backend.ErrNotFound
	}
	return err
}

func isNoScript(err error) bool {
	return strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT")
}

func boolArg(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func millis(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}

var _ backend.Backend = (*Backend)(nil)
