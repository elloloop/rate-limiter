// Package backend declares the protocol the ratelimiterserver speaks
// to its persistence driver. The Backend interface and every
// LimitOp / DecisionResult / *Result type used to exchange data
// between the server and the store live here so the Redis driver and
// the server can refer to the same shapes without either importing
// the other for implementation details.
//
// The current release ships exactly one Backend implementation, the
// Redis driver at ratelimiterserver/backend/redis. The algorithms
// depend on Redis Lua atomicity, so the boundary is intentionally
// shaped around Redis-backed quota, reservation, and lease operations.
package backend

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

// Backend persists the rate-limiter's counters, reservations, and
// leases. The ratelimiterserver Server calls these methods directly;
// every RPC handler in the server routes through this interface.
type Backend interface {
	// Lifecycle.
	Close() error

	// Health and script management.
	Ping(ctx context.Context) error
	ScriptsLoaded(ctx context.Context) (bool, error)
	LoadScripts(ctx context.Context) error

	// Quota lifecycle RPCs.
	Consume(ctx context.Context, idemKey string, now time.Time, ops []LimitOp, dryRun bool, decisionID string) (DecisionResult, error)
	Reserve(ctx context.Context, idemKey string, now time.Time, ops []LimitOp, dryRun bool, reservationKey, reservationExpiryIndexKey string, reservation *quotav1.Reservation, decisionID string) (DecisionResult, error)
	IncrementReservation(ctx context.Context, idemKey, reservationKey, reservationExpiryIndexKey string, deltaCost int64, now time.Time, decisionID string) (IncrementReservationResult, error)
	FinalizeReservation(ctx context.Context, idemKey, reservationKey, reservationExpiryIndexKey string, actualCost int64, now time.Time) (FinalizeResult, error)
	ReleaseReservation(ctx context.Context, idemKey, reservationKey, reservationExpiryIndexKey string, now time.Time) (ReleaseReservationResult, error)
	ExpireReservations(ctx context.Context, reservationExpiryIndexKey string, now time.Time, batchSize int64) (ExpireReservationsResult, error)
	AcquireLease(ctx context.Context, idemKey, leaseKey string, lease *quotav1.Lease, leaseTTL time.Duration, ops []LimitOp, dryRun bool, decisionID string) (DecisionResult, error)
	RenewLease(ctx context.Context, idemKey, leaseKey, leaseID string, extendTTL time.Duration, now time.Time) (LeaseResult, error)
	ReleaseLease(ctx context.Context, idemKey, leaseKey, leaseID string) (LeaseResult, error)

	// Read paths backing GetReservation, GetLease, and GetCurrentUsage.
	GetReservation(ctx context.Context, key string) (*quotav1.Reservation, error)
	GetLease(ctx context.Context, key string) (*quotav1.Lease, error)
	CounterValue(ctx context.Context, key string) (int64, error)
	BucketState(ctx context.Context, key string) (BucketState, error)
	GCRAValue(ctx context.Context, key string) (float64, bool, error)
	ConcurrencyCount(ctx context.Context, key string, now time.Time) (int64, error)
}

// ErrNotFound is returned by GetReservation and GetLease when the
// requested key does not exist. Drivers map their own miss sentinel
// (go-redis's redis.Nil, etc.) to this so the server can translate
// it into gRPC NotFound without importing a driver package.
var ErrNotFound = errors.New("ratelimiter backend: key not found")

// LimitOp is one limit's worth of state the server hands to the
// backend per RPC: which keys to read, which key to write, the
// limit / cost / bucket parameters the Lua scripts need, and the
// expiry metadata used to size TTLs.
type LimitOp struct {
	LimitID          string   `json:"limit_id"`
	Kind             string   `json:"kind"`
	ReadKeys         []string `json:"read_keys"`
	WriteKey         string   `json:"write_key"`
	Limit            int64    `json:"limit"`
	Cost             int64    `json:"cost"`
	ResetAtUnixMs    int64    `json:"reset_at_unix_ms"`
	TTLMS            int64    `json:"ttl_ms"`
	Burst            int64    `json:"burst"`
	RefillRatePerSec float64  `json:"refill_rate_per_sec"`
}

// ScriptStatus is one limit's evaluation result as the Lua script
// reports it.
type ScriptStatus struct {
	LimitID      string `json:"limit_id"`
	Used         int64  `json:"used"`
	Remaining    int64  `json:"remaining"`
	RetryAfterMS int64  `json:"retry_after_ms"`
	Allowed      bool   `json:"allowed"`
	Message      string `json:"message"`
}

// DecisionResult is the envelope returned from Consume, Reserve, and
// AcquireLease. Cached signals an idempotent replay; the server uses
// the embedded statuses to project per-limit detail into the gRPC
// response.
type DecisionResult struct {
	Cached        bool           `json:"cached"`
	DecisionID    string         `json:"decision_id"`
	Allowed       bool           `json:"allowed"`
	DryRun        bool           `json:"dry_run"`
	ReservationID string         `json:"reservation_id"`
	LeaseID       string         `json:"lease_id"`
	RetryAfterMS  int64          `json:"retry_after_ms"`
	Statuses      []ScriptStatus `json:"statuses"`
	Message       string         `json:"message"`
}

func (d *DecisionResult) UnmarshalJSON(data []byte) error {
	var raw struct {
		Cached        bool            `json:"cached"`
		DecisionID    string          `json:"decision_id"`
		Allowed       bool            `json:"allowed"`
		DryRun        bool            `json:"dry_run"`
		ReservationID string          `json:"reservation_id"`
		LeaseID       string          `json:"lease_id"`
		RetryAfterMS  int64           `json:"retry_after_ms"`
		Statuses      json.RawMessage `json:"statuses"`
		Message       string          `json:"message"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*d = DecisionResult{
		Cached:        raw.Cached,
		DecisionID:    raw.DecisionID,
		Allowed:       raw.Allowed,
		DryRun:        raw.DryRun,
		ReservationID: raw.ReservationID,
		LeaseID:       raw.LeaseID,
		RetryAfterMS:  raw.RetryAfterMS,
		Message:       raw.Message,
	}
	statuses := strings.TrimSpace(string(raw.Statuses))
	if statuses == "" || statuses == "null" || statuses == "{}" {
		return nil
	}
	return json.Unmarshal(raw.Statuses, &d.Statuses)
}

// IncrementReservationResult is the envelope returned from
// IncrementReservation.
type IncrementReservationResult struct {
	Cached       bool
	Found        bool
	Active       bool
	ReservedCost int64
	Decision     DecisionResult
	Reservation  *quotav1.Reservation
}

// FinalizeResult is the envelope returned from FinalizeReservation.
type FinalizeResult struct {
	Cached       bool
	Found        bool
	Finalized    bool
	ReservedCost int64
	ActualCost   int64
	RefundedCost int64
	OverageCost  int64
	Reservation  *quotav1.Reservation
}

// ReleaseReservationResult is the envelope returned from
// ReleaseReservation.
type ReleaseReservationResult struct {
	Cached       bool
	Found        bool
	Released     bool
	ReleasedCost int64
	Reservation  *quotav1.Reservation
}

// LeaseResult is the envelope returned from AcquireLease,
// RenewLease, and ReleaseLease.
type LeaseResult struct {
	Cached   bool
	Found    bool
	Renewed  bool
	Released bool
	Lease    *quotav1.Lease
}

// ExpireReservationsResult is the envelope returned from
// ExpireReservations.
type ExpireReservationsResult struct {
	Expired int64 `json:"expired"`
	Scanned int64 `json:"scanned"`
}

// BucketState is the persisted token / leak state of a continuous
// bucket key (token bucket, leaky bucket). Exists=false signals the
// key has not been written yet, which callers treat as a full bucket.
type BucketState struct {
	Tokens       float64
	LastRefillMs int64
	Exists       bool
}
