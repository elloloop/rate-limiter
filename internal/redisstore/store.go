package redisstore

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

const (
	scriptConsume              = "consume"
	scriptReserve              = "reserve"
	scriptFinalizeReservation  = "finalize_reservation"
	scriptReleaseReservation   = "release_reservation"
	scriptAcquireLease         = "acquire_lease"
	scriptRenewLease           = "renew_lease"
	scriptReleaseLease         = "release_lease"
	defaultIdempotencyTTL      = 24 * time.Hour
	defaultReservationExtraTTL = 24 * time.Hour
	defaultLeaseExtraTTL       = time.Hour
)

type Store struct {
	client  *redis.Client
	scripts map[string]string
}

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

type ScriptStatus struct {
	LimitID      string `json:"limit_id"`
	Used         int64  `json:"used"`
	Remaining    int64  `json:"remaining"`
	RetryAfterMS int64  `json:"retry_after_ms"`
	Allowed      bool   `json:"allowed"`
	Message      string `json:"message"`
}

type DecisionResult struct {
	Cached        bool           `json:"cached"`
	DecisionID    string         `json:"decision_id"`
	Allowed       bool           `json:"allowed"`
	DryRun        bool           `json:"dry_run"`
	ReservationID string         `json:"reservation_id"`
	LeaseID       string         `json:"lease_id"`
	RetryAfterMS  int64          `json:"retry_after_ms"`
	Statuses      []ScriptStatus `json:"statuses"`
}

type FinalizeResult struct {
	Found        bool                 `json:"found"`
	Finalized    bool                 `json:"finalized"`
	ReservedCost int64                `json:"reserved_cost"`
	ActualCost   int64                `json:"actual_cost"`
	RefundedCost int64                `json:"refunded_cost"`
	OverageCost  int64                `json:"overage_cost"`
	Reservation  *quotav1.Reservation `json:"-"`
	raw          json.RawMessage
}

type ReleaseReservationResult struct {
	Found        bool                 `json:"found"`
	Released     bool                 `json:"released"`
	ReleasedCost int64                `json:"released_cost"`
	Reservation  *quotav1.Reservation `json:"-"`
	raw          json.RawMessage
}

type LeaseResult struct {
	Found    bool           `json:"found"`
	Renewed  bool           `json:"renewed"`
	Released bool           `json:"released"`
	Lease    *quotav1.Lease `json:"-"`
	raw      json.RawMessage
}

func New(ctx context.Context, redisURL string) (*Store, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	store := &Store{client: client, scripts: map[string]string{}}
	if err := store.LoadScripts(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Client() *redis.Client {
	return s.client
}

func (s *Store) LoadScripts(ctx context.Context) error {
	for _, name := range []string{
		scriptConsume,
		scriptReserve,
		scriptFinalizeReservation,
		scriptReleaseReservation,
		scriptAcquireLease,
		scriptRenewLease,
		scriptReleaseLease,
	} {
		body, err := scriptFS.ReadFile("scripts/" + name + ".lua")
		if err != nil {
			return err
		}
		sha, err := s.client.ScriptLoad(ctx, string(body)).Result()
		if err != nil {
			return fmt.Errorf("load %s.lua: %w", name, err)
		}
		s.scripts[name] = sha
	}
	return nil
}

func (s *Store) ScriptSHAs() []string {
	shas := make([]string, 0, len(s.scripts))
	for _, sha := range s.scripts {
		shas = append(shas, sha)
	}
	return shas
}

func (s *Store) Consume(ctx context.Context, idemKey string, now time.Time, ops []LimitOp, dryRun bool, decisionID string) (DecisionResult, error) {
	args, err := json.Marshal(ops)
	if err != nil {
		return DecisionResult{}, err
	}
	raw, err := s.client.EvalSha(ctx, s.scripts[scriptConsume], nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		now.UnixMilli(),
		boolArg(dryRun),
		string(args),
		decisionID,
	).Text()
	if err != nil {
		return DecisionResult{}, err
	}
	return parseDecision(raw)
}

func (s *Store) Reserve(ctx context.Context, idemKey string, now time.Time, ops []LimitOp, dryRun bool, reservationKey string, reservation *quotav1.Reservation, decisionID string) (DecisionResult, error) {
	args, err := json.Marshal(ops)
	if err != nil {
		return DecisionResult{}, err
	}
	reservationJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(reservation)
	if err != nil {
		return DecisionResult{}, err
	}
	ttl := time.Until(time.UnixMilli(reservation.GetExpiresAtUnixMs())) + defaultReservationExtraTTL
	if ttl < defaultReservationExtraTTL {
		ttl = defaultReservationExtraTTL
	}
	raw, err := s.client.EvalSha(ctx, s.scripts[scriptReserve], nil,
		idemKey,
		millis(ttl),
		now.UnixMilli(),
		boolArg(dryRun),
		string(args),
		reservationKey,
		string(reservationJSON),
		millis(ttl),
		decisionID,
	).Text()
	if err != nil {
		return DecisionResult{}, err
	}
	return parseDecision(raw)
}

func (s *Store) FinalizeReservation(ctx context.Context, idemKey, reservationKey string, actualCost int64, now time.Time) (FinalizeResult, error) {
	raw, err := s.client.EvalSha(ctx, s.scripts[scriptFinalizeReservation], nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		reservationKey,
		actualCost,
		now.UnixMilli(),
	).Text()
	if err != nil {
		return FinalizeResult{}, err
	}
	var envelope struct {
		Found        bool            `json:"found"`
		Finalized    bool            `json:"finalized"`
		ReservedCost int64           `json:"reserved_cost"`
		ActualCost   int64           `json:"actual_cost"`
		RefundedCost int64           `json:"refunded_cost"`
		OverageCost  int64           `json:"overage_cost"`
		Reservation  json.RawMessage `json:"reservation"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return FinalizeResult{}, err
	}
	result := FinalizeResult{
		Found:        envelope.Found,
		Finalized:    envelope.Finalized,
		ReservedCost: envelope.ReservedCost,
		ActualCost:   envelope.ActualCost,
		RefundedCost: envelope.RefundedCost,
		OverageCost:  envelope.OverageCost,
		raw:          envelope.Reservation,
	}
	if len(envelope.Reservation) > 0 {
		result.Reservation = &quotav1.Reservation{}
		if err := protojson.Unmarshal(envelope.Reservation, result.Reservation); err != nil {
			return FinalizeResult{}, err
		}
	}
	return result, nil
}

func (s *Store) ReleaseReservation(ctx context.Context, idemKey, reservationKey string, now time.Time) (ReleaseReservationResult, error) {
	raw, err := s.client.EvalSha(ctx, s.scripts[scriptReleaseReservation], nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		reservationKey,
		now.UnixMilli(),
	).Text()
	if err != nil {
		return ReleaseReservationResult{}, err
	}
	var envelope struct {
		Found        bool            `json:"found"`
		Released     bool            `json:"released"`
		ReleasedCost int64           `json:"released_cost"`
		Reservation  json.RawMessage `json:"reservation"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return ReleaseReservationResult{}, err
	}
	result := ReleaseReservationResult{
		Found:        envelope.Found,
		Released:     envelope.Released,
		ReleasedCost: envelope.ReleasedCost,
		raw:          envelope.Reservation,
	}
	if len(envelope.Reservation) > 0 {
		result.Reservation = &quotav1.Reservation{}
		if err := protojson.Unmarshal(envelope.Reservation, result.Reservation); err != nil {
			return ReleaseReservationResult{}, err
		}
	}
	return result, nil
}

func (s *Store) AcquireLease(ctx context.Context, idemKey, leaseKey string, lease *quotav1.Lease, leaseTTL time.Duration, ops []LimitOp, dryRun bool, decisionID string) (DecisionResult, error) {
	args, err := json.Marshal(ops)
	if err != nil {
		return DecisionResult{}, err
	}
	leaseJSON, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(lease)
	if err != nil {
		return DecisionResult{}, err
	}
	raw, err := s.client.EvalSha(ctx, s.scripts[scriptAcquireLease], nil,
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
	).Text()
	if err != nil {
		return DecisionResult{}, err
	}
	return parseDecision(raw)
}

func (s *Store) RenewLease(ctx context.Context, idemKey, leaseKey, leaseID string, extendTTL time.Duration, now time.Time) (LeaseResult, error) {
	raw, err := s.client.EvalSha(ctx, s.scripts[scriptRenewLease], nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		leaseKey,
		leaseID,
		millis(extendTTL),
		now.UnixMilli(),
	).Text()
	if err != nil {
		return LeaseResult{}, err
	}
	return parseLeaseResult(raw)
}

func (s *Store) ReleaseLease(ctx context.Context, idemKey, leaseKey, leaseID string) (LeaseResult, error) {
	raw, err := s.client.EvalSha(ctx, s.scripts[scriptReleaseLease], nil,
		idemKey,
		millis(defaultIdempotencyTTL),
		leaseKey,
		leaseID,
	).Text()
	if err != nil {
		return LeaseResult{}, err
	}
	return parseLeaseResult(raw)
}

func (s *Store) GetReservation(ctx context.Context, key string) (*quotav1.Reservation, error) {
	raw, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	res := &quotav1.Reservation{}
	if err := protojson.Unmarshal(raw, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Store) GetLease(ctx context.Context, key string) (*quotav1.Lease, error) {
	raw, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	lease := &quotav1.Lease{}
	if err := protojson.Unmarshal(raw, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

func parseDecision(raw string) (DecisionResult, error) {
	var result DecisionResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return DecisionResult{}, err
	}
	return result, nil
}

func parseLeaseResult(raw string) (LeaseResult, error) {
	var envelope struct {
		Found    bool            `json:"found"`
		Renewed  bool            `json:"renewed"`
		Released bool            `json:"released"`
		Lease    json.RawMessage `json:"lease"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return LeaseResult{}, err
	}
	result := LeaseResult{
		Found:    envelope.Found,
		Renewed:  envelope.Renewed,
		Released: envelope.Released,
		raw:      envelope.Lease,
	}
	if len(envelope.Lease) > 0 {
		result.Lease = &quotav1.Lease{}
		if err := protojson.Unmarshal(envelope.Lease, result.Lease); err != nil {
			return LeaseResult{}, err
		}
	}
	return result, nil
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
