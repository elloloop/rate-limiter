package ratelimiterserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	"github.com/elloloop/rate-limiter/internal/keys"
	"github.com/elloloop/rate-limiter/internal/limits"
	"github.com/elloloop/rate-limiter/internal/metrics"
	"github.com/elloloop/rate-limiter/ratelimiterserver/backend"
)

// Server is the rate-limiter, mounted on a host *grpc.Server. It
// implements quotav1.QuotaServiceServer; build it with [New] and
// register it on the host grpc.Server with
// quotav1.RegisterQuotaServiceServer.
type Server struct {
	quotav1.UnimplementedQuotaServiceServer

	product     string
	environment string
	redisMode   string
	prefix      string

	store   backend.Backend
	events  EventSink
	metrics *metrics.Metrics
	logger  *slog.Logger
}

// New validates opts and constructs a Server. The construction ctx
// is accepted for symmetry with future backends that may need it;
// today's wiring does no I/O, so it is unused. The Server retains
// the supplied backend and emits events, metrics, and logs against
// the contexts passed to its RPC methods.
//
// New returns an error if Product or Environment is empty or if
// Backend is nil. A non-nil Logger / EventSink / Metrics is used
// as-is; nil installs the documented default (no-op logger, no-op
// event sink, isolated private metrics registry).
func New(_ context.Context, opts Options) (*Server, error) {
	if opts.Product == "" {
		return nil, errors.New("ratelimiterserver: Options.Product is required")
	}
	if opts.Environment == "" {
		return nil, errors.New("ratelimiterserver: Options.Environment is required")
	}
	if opts.Backend == nil {
		return nil, errors.New("ratelimiterserver: Options.Backend is required")
	}
	if opts.RedisMode != "" && opts.RedisMode != "single_primary" {
		return nil, fmt.Errorf("ratelimiterserver: Options.RedisMode=%q is unsupported in v0.4.0; use \"single_primary\" or leave empty", opts.RedisMode)
	}

	redisMode := opts.RedisMode
	if redisMode == "" {
		redisMode = "single_primary"
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	sink := opts.EventSink
	if sink == nil {
		sink = noopEventSink{}
	}

	return &Server{
		product:     opts.Product,
		environment: opts.Environment,
		redisMode:   redisMode,
		prefix:      keys.Prefix(opts.Environment, opts.Product),
		store:       opts.Backend,
		events:      sink,
		metrics:     metrics.New(opts.Metrics),
		logger:      logger,
	}, nil
}

// Metrics returns the Prometheus collector wrapper the server
// records its RED metrics into. When the host did not supply its
// own Registerer through Options.Metrics, the returned *Metrics
// exposes a private registry via Handler / Serve; cmd/quota-service
// uses that to serve /metrics on its dedicated bind address.
func (s *Server) Metrics() *metrics.Metrics {
	return s.metrics
}

// ExpireReservations runs one sweep of the reservation expiry index.
// Callers schedule it on a ticker — cmd/quota-service does so once
// per second. batchSize <= 0 defaults to 100.
func (s *Server) ExpireReservations(ctx context.Context, batchSize int64) (int64, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	result, err := s.store.ExpireReservations(ctx, keys.ReservationExpiryIndex(s.prefix), time.Now(), batchSize)
	if err != nil {
		s.metrics.RedisError()
		return 0, err
	}
	if result.Expired > 0 {
		s.metrics.ReservationsExpired(float64(result.Expired))
	}
	return result.Expired, nil
}

func (s *Server) Consume(ctx context.Context, req *quotav1.ConsumeRequest) (*quotav1.ConsumeResponse, error) {
	start := time.Now()
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.GetCost() <= 0 {
		return &quotav1.ConsumeResponse{Decision: invalidDecision("cost must be greater than zero")}, nil
	}
	errs, _ := limits.Validate(req.GetAction(), req.GetLimits())
	if len(errs) > 0 {
		return &quotav1.ConsumeResponse{Decision: validationDecision(errs)}, nil
	}
	ops, err := s.buildLimitOps(req.GetLimits(), req.GetCost(), time.Now(), false)
	if err != nil {
		return &quotav1.ConsumeResponse{Decision: invalidDecision(err.Error())}, nil
	}
	result, err := s.store.Consume(ctx, keys.Request(s.prefix, req.GetRequestId()), time.Now(), ops, req.GetOptions().GetDryRun(), uuid.NewString())
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis consume: %v", err)
	}
	if result.Cached {
		s.metrics.IdempotencyHit()
	}
	decision := s.decisionFromResult(result, req.GetLimits(), req.GetCost(), req.GetOptions().GetDryRun(), "consume")
	s.recordMetrics("Consume", req.GetAction(), decision, start)
	if req.GetOptions().GetEmitEvent() {
		eventType := "quota.consumed"
		if !decision.GetAllowed() {
			eventType = "quota.denied"
		}
		s.emit(ctx, eventType, req.GetRequestId(), req.GetContext(), req.GetAction(), req.GetCost(), decision, nil, nil)
	}
	return &quotav1.ConsumeResponse{Decision: decision}, nil
}

func (s *Server) Reserve(ctx context.Context, req *quotav1.ReserveRequest) (*quotav1.ReserveResponse, error) {
	start := time.Now()
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.GetReserveCost() <= 0 {
		return &quotav1.ReserveResponse{Decision: invalidDecision("reserve_cost must be greater than zero")}, nil
	}
	if req.GetReservationTtlMs() <= 0 {
		return &quotav1.ReserveResponse{Decision: invalidDecision("reservation_ttl_ms must be greater than zero")}, nil
	}
	errs, _ := limits.Validate(req.GetAction(), req.GetLimits())
	if len(errs) > 0 {
		return &quotav1.ReserveResponse{Decision: validationDecision(errs)}, nil
	}
	now := time.Now()
	ops, err := s.buildLimitOps(req.GetLimits(), req.GetReserveCost(), now, false)
	if err != nil {
		return &quotav1.ReserveResponse{Decision: invalidDecision(err.Error())}, nil
	}
	reservation := s.newReservation(req, ops, now)
	result, err := s.store.Reserve(
		ctx,
		keys.Request(s.prefix, req.GetRequestId()),
		now,
		ops,
		req.GetOptions().GetDryRun(),
		keys.Reservation(s.prefix, reservation.GetReservationId()),
		keys.ReservationExpiryIndex(s.prefix),
		reservation,
		uuid.NewString(),
	)
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis reserve: %v", err)
	}
	if result.Cached {
		s.metrics.IdempotencyHit()
		if result.ReservationID != "" {
			reservation, _ = s.store.GetReservation(ctx, keys.Reservation(s.prefix, result.ReservationID))
		}
	}
	decision := s.decisionFromResult(result, req.GetLimits(), req.GetReserveCost(), req.GetOptions().GetDryRun(), "reserve")
	if !decision.GetAllowed() || req.GetOptions().GetDryRun() {
		reservation = nil
	} else if !result.Cached {
		s.metrics.ReservationInc()
	}
	s.recordMetrics("Reserve", req.GetAction(), decision, start)
	if req.GetOptions().GetEmitEvent() {
		eventType := "quota.reserved"
		if !decision.GetAllowed() {
			eventType = "quota.denied"
		}
		s.emit(ctx, eventType, req.GetRequestId(), req.GetContext(), req.GetAction(), req.GetReserveCost(), decision, reservation, nil)
	}
	return &quotav1.ReserveResponse{Decision: decision, Reservation: reservation}, nil
}

func (s *Server) IncrementReservation(ctx context.Context, req *quotav1.IncrementReservationRequest) (*quotav1.IncrementReservationResponse, error) {
	start := time.Now()
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.GetReservationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "reservation_id is required")
	}
	if req.GetDeltaCost() == 0 {
		return &quotav1.IncrementReservationResponse{
			Decision: invalidDecision("delta_cost must be non-zero"),
		}, nil
	}
	result, err := s.store.IncrementReservation(
		ctx,
		keys.Request(s.prefix, req.GetRequestId()),
		keys.Reservation(s.prefix, req.GetReservationId()),
		keys.ReservationExpiryIndex(s.prefix),
		req.GetDeltaCost(),
		time.Now(),
		uuid.NewString(),
	)
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis increment reservation: %v", err)
	}
	if result.Cached || result.Decision.Cached {
		s.metrics.IdempotencyHit()
	}
	if !result.Found {
		return nil, status.Error(codes.NotFound, "reservation not found")
	}
	if !result.Active {
		return nil, status.Error(codes.FailedPrecondition, "reservation is not active")
	}
	decision := s.decisionFromReservationIncrement(result.Decision, result.Reservation, req.GetDeltaCost())
	action := ""
	if result.Reservation != nil {
		action = result.Reservation.GetAction()
	}
	s.recordMetrics("IncrementReservation", action, decision, start)
	return &quotav1.IncrementReservationResponse{
		Decision:     decision,
		ReservedCost: result.ReservedCost,
		Reservation:  result.Reservation,
	}, nil
}

func (s *Server) FinalizeReservation(ctx context.Context, req *quotav1.FinalizeReservationRequest) (*quotav1.FinalizeReservationResponse, error) {
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.GetReservationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "reservation_id is required")
	}
	if req.GetActualCost() < 0 {
		return nil, status.Error(codes.InvalidArgument, "actual_cost cannot be negative")
	}
	result, err := s.store.FinalizeReservation(ctx, keys.Request(s.prefix, req.GetRequestId()), keys.Reservation(s.prefix, req.GetReservationId()), keys.ReservationExpiryIndex(s.prefix), req.GetActualCost(), time.Now())
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis finalize reservation: %v", err)
	}
	if result.Cached {
		s.metrics.IdempotencyHit()
	}
	if !result.Found {
		return nil, status.Error(codes.NotFound, "reservation not found")
	}
	if result.Finalized && !result.Cached {
		s.metrics.ReservationDec()
	}
	if result.OverageCost > 0 && !result.Cached {
		s.metrics.Overage()
	}
	return &quotav1.FinalizeReservationResponse{
		ReservationId: req.GetReservationId(),
		ReservedCost:  result.ReservedCost,
		ActualCost:    result.ActualCost,
		RefundedCost:  result.RefundedCost,
		OverageCost:   result.OverageCost,
		Finalized:     result.Finalized,
	}, nil
}

func (s *Server) ReleaseReservation(ctx context.Context, req *quotav1.ReleaseReservationRequest) (*quotav1.ReleaseReservationResponse, error) {
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.GetReservationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "reservation_id is required")
	}
	result, err := s.store.ReleaseReservation(ctx, keys.Request(s.prefix, req.GetRequestId()), keys.Reservation(s.prefix, req.GetReservationId()), keys.ReservationExpiryIndex(s.prefix), time.Now())
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis release reservation: %v", err)
	}
	if result.Cached {
		s.metrics.IdempotencyHit()
	}
	if !result.Found {
		return nil, status.Error(codes.NotFound, "reservation not found")
	}
	if result.Released && !result.Cached {
		s.metrics.ReservationDec()
	}
	return &quotav1.ReleaseReservationResponse{
		ReservationId: req.GetReservationId(),
		ReleasedCost:  result.ReleasedCost,
		Released:      result.Released,
	}, nil
}

func (s *Server) AcquireLease(ctx context.Context, req *quotav1.AcquireLeaseRequest) (*quotav1.AcquireLeaseResponse, error) {
	start := time.Now()
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.GetLeaseTtlMs() <= 0 {
		return &quotav1.AcquireLeaseResponse{Decision: invalidDecision("lease_ttl_ms must be greater than zero")}, nil
	}
	errs, _ := limits.Validate(req.GetAction(), req.GetLimits())
	if len(errs) > 0 {
		return &quotav1.AcquireLeaseResponse{Decision: validationDecision(errs)}, nil
	}
	now := time.Now()
	ops, err := s.buildLimitOps(req.GetLimits(), 1, now, true)
	if err != nil {
		return &quotav1.AcquireLeaseResponse{Decision: invalidDecision(err.Error())}, nil
	}
	lease := s.newLease(req, ops, now)
	leaseTTL := time.Duration(req.GetLeaseTtlMs()) * time.Millisecond
	result, err := s.store.AcquireLease(ctx, keys.Request(s.prefix, req.GetRequestId()), keys.Lease(s.prefix, lease.GetLeaseId()), lease, leaseTTL, ops, req.GetOptions().GetDryRun(), uuid.NewString())
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis acquire lease: %v", err)
	}
	if result.Cached {
		s.metrics.IdempotencyHit()
		if result.LeaseID != "" {
			lease, _ = s.store.GetLease(ctx, keys.Lease(s.prefix, result.LeaseID))
		}
	}
	decision := s.decisionFromResult(result, req.GetLimits(), 1, req.GetOptions().GetDryRun(), "acquire lease")
	if !decision.GetAllowed() || req.GetOptions().GetDryRun() {
		lease = nil
	} else if !result.Cached {
		s.metrics.LeaseInc()
	}
	s.recordMetrics("AcquireLease", req.GetAction(), decision, start)
	if req.GetOptions().GetEmitEvent() {
		eventType := "quota.lease_acquired"
		if !decision.GetAllowed() {
			eventType = "quota.denied"
		}
		s.emit(ctx, eventType, req.GetRequestId(), req.GetContext(), req.GetAction(), 1, decision, nil, lease)
	}
	return &quotav1.AcquireLeaseResponse{Decision: decision, Lease: lease}, nil
}

func (s *Server) RenewLease(ctx context.Context, req *quotav1.RenewLeaseRequest) (*quotav1.RenewLeaseResponse, error) {
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.GetLeaseId() == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_id is required")
	}
	if req.GetExtendTtlMs() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "extend_ttl_ms must be greater than zero")
	}
	result, err := s.store.RenewLease(ctx, keys.Request(s.prefix, req.GetRequestId()), keys.Lease(s.prefix, req.GetLeaseId()), req.GetLeaseId(), time.Duration(req.GetExtendTtlMs())*time.Millisecond, time.Now())
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis renew lease: %v", err)
	}
	if result.Cached {
		s.metrics.IdempotencyHit()
	}
	if !result.Found {
		return nil, status.Error(codes.NotFound, "lease not found")
	}
	if !result.Renewed {
		return nil, status.Error(codes.FailedPrecondition, "lease is not active")
	}
	return &quotav1.RenewLeaseResponse{Lease: result.Lease, Renewed: result.Renewed}, nil
}

func (s *Server) ReleaseLease(ctx context.Context, req *quotav1.ReleaseLeaseRequest) (*quotav1.ReleaseLeaseResponse, error) {
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.GetLeaseId() == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_id is required")
	}
	result, err := s.store.ReleaseLease(ctx, keys.Request(s.prefix, req.GetRequestId()), keys.Lease(s.prefix, req.GetLeaseId()), req.GetLeaseId())
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis release lease: %v", err)
	}
	if result.Cached {
		s.metrics.IdempotencyHit()
	}
	if !result.Found {
		return nil, status.Error(codes.NotFound, "lease not found")
	}
	if result.Released && !result.Cached {
		s.metrics.LeaseDec()
	}
	return &quotav1.ReleaseLeaseResponse{LeaseId: req.GetLeaseId(), Released: result.Released}, nil
}

func (s *Server) Explain(ctx context.Context, req *quotav1.ExplainRequest) (*quotav1.ExplainResponse, error) {
	if req.GetCost() <= 0 {
		return &quotav1.ExplainResponse{
			WouldAllow: false,
			Reason:     quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST,
			Message:    "cost must be greater than zero",
			Evaluations: []*quotav1.LimitEvaluation{{
				Valid:      false,
				WouldAllow: false,
				ValidationErrors: []*quotav1.ValidationError{{
					Field:   "cost",
					Message: "must be greater than zero",
				}},
			}},
		}, nil
	}
	errs, warnings := limits.Validate(req.GetAction(), req.GetLimits())
	if len(errs) > 0 {
		return &quotav1.ExplainResponse{
			WouldAllow: false,
			Reason:     quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST,
			Message:    "invalid limits",
			Evaluations: []*quotav1.LimitEvaluation{{
				Valid:              false,
				WouldAllow:         false,
				ValidationErrors:   errs,
				ValidationWarnings: warnings,
			}},
		}, nil
	}
	usage, err := s.currentUsage(ctx, req.GetLimits(), time.Now())
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis usage: %v", err)
	}
	wouldAllow := true
	reason := quotav1.DecisionReason_DECISION_REASON_ALLOWED
	evals := make([]*quotav1.LimitEvaluation, 0, len(usage))
	for _, st := range usage {
		st.Cost = req.GetCost()
		if st.GetUsed()+req.GetCost() > st.GetLimit() {
			st.Allowed = false
			st.Message = "limit exceeded"
			wouldAllow = false
			reason = quotav1.DecisionReason_DECISION_REASON_LIMIT_EXCEEDED
		} else {
			st.Allowed = true
			st.Message = "allowed"
		}
		evals = append(evals, &quotav1.LimitEvaluation{LimitId: st.GetLimitId(), Valid: true, WouldAllow: st.GetAllowed(), CurrentStatus: st})
	}
	return &quotav1.ExplainResponse{WouldAllow: wouldAllow, Reason: reason, Message: reason.String(), Evaluations: evals}, nil
}

func (s *Server) GetCurrentUsage(ctx context.Context, req *quotav1.GetCurrentUsageRequest) (*quotav1.GetCurrentUsageResponse, error) {
	errs, _ := limits.Validate(req.GetAction(), req.GetLimits())
	if len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid limits")
	}
	usage, err := s.currentUsage(ctx, req.GetLimits(), time.Now())
	if err != nil {
		s.metrics.RedisError()
		return nil, status.Errorf(codes.Unavailable, "redis usage: %v", err)
	}
	return &quotav1.GetCurrentUsageResponse{LimitStatuses: usage}, nil
}

func (s *Server) ValidateLimits(_ context.Context, req *quotav1.ValidateLimitsRequest) (*quotav1.ValidateLimitsResponse, error) {
	errs, warnings := limits.Validate("", req.GetLimits())
	return &quotav1.ValidateLimitsResponse{Valid: len(errs) == 0, Errors: errs, Warnings: warnings}, nil
}

func (s *Server) GetReservation(ctx context.Context, req *quotav1.GetReservationRequest) (*quotav1.Reservation, error) {
	if req.GetReservationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "reservation_id is required")
	}
	res, err := s.store.GetReservation(ctx, keys.Reservation(s.prefix, req.GetReservationId()))
	if errors.Is(err, backend.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "reservation not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "redis get reservation: %v", err)
	}
	return res, nil
}

func (s *Server) GetLease(ctx context.Context, req *quotav1.GetLeaseRequest) (*quotav1.Lease, error) {
	if req.GetLeaseId() == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_id is required")
	}
	lease, err := s.store.GetLease(ctx, keys.Lease(s.prefix, req.GetLeaseId()))
	if errors.Is(err, backend.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "lease not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "redis get lease: %v", err)
	}
	return lease, nil
}

// GetRedisStatus pings the backend and reports its health. The
// response RPC error is nil even when the backend is unreachable —
// the status fields carry the result, so a health probe can be
// scheduled without inferring backend state from error patterns.
//
//nolint:nilerr // unreachable backend is health data, not an RPC error
func (s *Server) GetRedisStatus(ctx context.Context, _ *quotav1.GetRedisStatusRequest) (*quotav1.RedisStatus, error) {
	start := time.Now()
	latency := func() int64 { return time.Since(start).Milliseconds() }
	if err := s.store.Ping(ctx); err != nil {
		return &quotav1.RedisStatus{Reachable: false, Mode: s.redisMode, LatencyMs: latency(), Message: err.Error()}, nil
	}
	loaded, err := s.store.ScriptsLoaded(ctx)
	if err != nil {
		return &quotav1.RedisStatus{Reachable: false, Mode: s.redisMode, LatencyMs: latency(), Message: err.Error()}, nil
	}
	if loaded {
		return &quotav1.RedisStatus{Reachable: true, Mode: s.redisMode, LatencyMs: latency(), Message: "ok"}, nil
	}
	if err := s.store.LoadScripts(ctx); err != nil {
		return &quotav1.RedisStatus{Reachable: true, Mode: s.redisMode, LatencyMs: latency(), Message: "reload Redis scripts: " + err.Error()}, nil
	}
	reloaded, err := s.store.ScriptsLoaded(ctx)
	if err != nil {
		return &quotav1.RedisStatus{Reachable: false, Mode: s.redisMode, LatencyMs: latency(), Message: err.Error()}, nil
	}
	if !reloaded {
		return &quotav1.RedisStatus{Reachable: true, Mode: s.redisMode, LatencyMs: latency(), Message: "one or more Redis scripts are not loaded"}, nil
	}
	return &quotav1.RedisStatus{Reachable: true, Mode: s.redisMode, LatencyMs: latency(), Message: "ok"}, nil
}

func (s *Server) newReservation(req *quotav1.ReserveRequest, ops []backend.LimitOp, now time.Time) *quotav1.Reservation {
	impacts := make([]*quotav1.ReservationImpact, 0, len(req.GetLimits()))
	for i, limit := range req.GetLimits() {
		impacts = append(impacts, &quotav1.ReservationImpact{
			LimitId:          limit.GetLimitId(),
			ScopeKey:         limit.GetScopeKey(),
			RedisKey:         ops[i].WriteKey,
			Algorithm:        limit.GetAlgorithm(),
			ReservedCost:     req.GetReserveCost(),
			Refundable:       limit.GetRefundable(),
			ExpiryPolicy:     limit.GetReservationExpiryPolicy(),
			Limit:            limit.GetLimit(),
			Burst:            limit.GetBurst(),
			RefillRatePerSec: limit.GetRefillRatePerSec(),
			ResetAtUnixMs:    ops[i].ResetAtUnixMs,
			Unit:             limit.GetUnit(),
		})
	}
	return &quotav1.Reservation{
		ReservationId:   uuid.NewString(),
		Action:          req.GetAction(),
		Context:         req.GetContext(),
		ReservedCost:    req.GetReserveCost(),
		CreatedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(time.Duration(req.GetReservationTtlMs()) * time.Millisecond).UnixMilli(),
		Status:          quotav1.ReservationStatus_RESERVATION_STATUS_ACTIVE,
		Impacts:         impacts,
		Metadata:        map[string]string{"request_id": req.GetRequestId()},
	}
}

func (s *Server) newLease(req *quotav1.AcquireLeaseRequest, ops []backend.LimitOp, now time.Time) *quotav1.Lease {
	impacts := make([]*quotav1.LeaseImpact, 0, len(req.GetLimits()))
	for i, limit := range req.GetLimits() {
		impacts = append(impacts, &quotav1.LeaseImpact{
			LimitId:     limit.GetLimitId(),
			ScopeKey:    limit.GetScopeKey(),
			LeaseSetKey: ops[i].WriteKey,
		})
	}
	return &quotav1.Lease{
		LeaseId:         uuid.NewString(),
		Action:          req.GetAction(),
		Context:         req.GetContext(),
		CreatedAtUnixMs: now.UnixMilli(),
		ExpiresAtUnixMs: now.Add(time.Duration(req.GetLeaseTtlMs()) * time.Millisecond).UnixMilli(),
		Impacts:         impacts,
		Status:          quotav1.LeaseStatus_LEASE_STATUS_ACTIVE,
	}
}

func (s *Server) buildLimitOps(supplied []*quotav1.Limit, cost int64, now time.Time, concurrencyOnly bool) ([]backend.LimitOp, error) {
	ops := make([]backend.LimitOp, 0, len(supplied))
	for _, limit := range supplied {
		op := backend.LimitOp{
			LimitID:          limit.GetLimitId(),
			Limit:            limit.GetLimit(),
			Cost:             cost,
			Burst:            limit.GetBurst(),
			RefillRatePerSec: limit.GetRefillRatePerSec(),
		}
		switch limit.GetAlgorithm() {
		case quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR:
			if concurrencyOnly {
				return nil, errors.New("AcquireLease only accepts ALGORITHM_CONCURRENCY limits")
			}
			key, reset, ttl := keys.FixedWindow(s.prefix, limit, now)
			op.Kind = "counter"
			op.ReadKeys = []string{key}
			op.WriteKey = key
			op.ResetAtUnixMs = reset.UnixMilli()
			op.TTLMS = ttl.Milliseconds()
		case quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION:
			if concurrencyOnly {
				return nil, errors.New("AcquireLease only accepts ALGORITHM_CONCURRENCY limits")
			}
			key, reset, ttl := keys.DurationWindow(s.prefix, limit, now)
			op.Kind = "counter"
			op.ReadKeys = []string{key}
			op.WriteKey = key
			op.ResetAtUnixMs = reset.UnixMilli()
			op.TTLMS = ttl.Milliseconds()
		case quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW:
			if concurrencyOnly {
				return nil, errors.New("AcquireLease only accepts ALGORITHM_CONCURRENCY limits")
			}
			readKeys, writeKey, reset, ttl := keys.SlidingBuckets(s.prefix, limit, now)
			op.Kind = "counter"
			op.ReadKeys = readKeys
			op.WriteKey = writeKey
			op.ResetAtUnixMs = reset.UnixMilli()
			op.TTLMS = ttl.Milliseconds()
		case quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET:
			if concurrencyOnly {
				return nil, errors.New("AcquireLease only accepts ALGORITHM_CONCURRENCY limits")
			}
			op.Kind = "token_bucket"
			op.WriteKey = keys.TokenBucket(s.prefix, limit)
			op.TTLMS = 24 * time.Hour.Milliseconds()
		case quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET:
			if concurrencyOnly {
				return nil, errors.New("AcquireLease only accepts ALGORITHM_CONCURRENCY limits")
			}
			op.Kind = "leaky_bucket"
			op.WriteKey = keys.LeakyBucket(s.prefix, limit)
			op.TTLMS = 24 * time.Hour.Milliseconds()
		case quotav1.Algorithm_ALGORITHM_GCRA:
			if concurrencyOnly {
				return nil, errors.New("AcquireLease only accepts ALGORITHM_CONCURRENCY limits")
			}
			op.Kind = "gcra"
			op.WriteKey = keys.GCRA(s.prefix, limit)
			op.TTLMS = 24 * time.Hour.Milliseconds()
		case quotav1.Algorithm_ALGORITHM_CONCURRENCY:
			if !concurrencyOnly {
				return nil, errors.New("Consume/Reserve do not accept ALGORITHM_CONCURRENCY limits; use AcquireLease")
			}
			op.Kind = "concurrency"
			op.WriteKey = keys.LeaseSet(s.prefix, limit)
			op.TTLMS = time.Hour.Milliseconds()
		default:
			return nil, fmt.Errorf("unsupported algorithm %s", limit.GetAlgorithm())
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func (s *Server) currentUsage(ctx context.Context, supplied []*quotav1.Limit, now time.Time) ([]*quotav1.LimitStatus, error) {
	statuses := make([]*quotav1.LimitStatus, 0, len(supplied))
	for _, limit := range supplied {
		st := &quotav1.LimitStatus{
			LimitId:   limit.GetLimitId(),
			ScopeKey:  limit.GetScopeKey(),
			Action:    limit.GetAction(),
			Unit:      limit.GetUnit(),
			Algorithm: limit.GetAlgorithm(),
			Window:    limit.GetWindow(),
			Limit:     limit.GetLimit(),
			Allowed:   true,
			Message:   "current usage",
		}
		switch limit.GetAlgorithm() {
		case quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR:
			key, reset, _ := keys.FixedWindow(s.prefix, limit, now)
			used, err := s.store.CounterValue(ctx, key)
			if err != nil {
				return nil, err
			}
			st.Used = used
			st.ResetAtUnixMs = reset.UnixMilli()
		case quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION:
			key, reset, _ := keys.DurationWindow(s.prefix, limit, now)
			used, err := s.store.CounterValue(ctx, key)
			if err != nil {
				return nil, err
			}
			st.Used = used
			st.ResetAtUnixMs = reset.UnixMilli()
		case quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW:
			readKeys, _, reset, _ := keys.SlidingBuckets(s.prefix, limit, now)
			var used int64
			for _, key := range readKeys {
				value, err := s.store.CounterValue(ctx, key)
				if err != nil {
					return nil, err
				}
				used += value
			}
			st.Used = used
			st.ResetAtUnixMs = reset.UnixMilli()
		case quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET:
			used, remaining, err := s.bucketUsage(ctx, keys.TokenBucket(s.prefix, limit), limit, now)
			if err != nil {
				return nil, err
			}
			st.Used = used
			st.Remaining = remaining
		case quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET:
			used, remaining, err := s.bucketUsage(ctx, keys.LeakyBucket(s.prefix, limit), limit, now)
			if err != nil {
				return nil, err
			}
			st.Used = used
			st.Remaining = remaining
		case quotav1.Algorithm_ALGORITHM_GCRA:
			remaining, retry, err := s.gcraUsage(ctx, keys.GCRA(s.prefix, limit), limit, now)
			if err != nil {
				return nil, err
			}
			st.Remaining = remaining
			st.RetryAfterMs = retry
		case quotav1.Algorithm_ALGORITHM_CONCURRENCY:
			active, err := s.store.ConcurrencyCount(ctx, keys.LeaseSet(s.prefix, limit), now)
			if err != nil {
				return nil, err
			}
			st.Used = active
		}
		if st.GetRemaining() == 0 && st.GetAlgorithm() != quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET && st.GetAlgorithm() != quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET && st.GetAlgorithm() != quotav1.Algorithm_ALGORITHM_GCRA {
			st.Remaining = maxInt64(0, st.GetLimit()-st.GetUsed())
		}
		statuses = append(statuses, st)
	}
	return statuses, nil
}

func (s *Server) bucketUsage(ctx context.Context, key string, limit *quotav1.Limit, now time.Time) (int64, int64, error) {
	capacity := limit.GetBurst()
	if capacity <= 0 {
		capacity = limit.GetLimit()
	}
	state, err := s.store.BucketState(ctx, key)
	if err != nil {
		return 0, 0, err
	}
	tokens := float64(capacity)
	lastRefill := now.UnixMilli()
	if state.Exists {
		tokens = state.Tokens
		if state.LastRefillMs != 0 {
			lastRefill = state.LastRefillMs
		}
	}
	elapsed := float64(maxInt64(0, now.UnixMilli()-lastRefill)) / 1000
	available := math.Min(float64(capacity), tokens+(elapsed*limit.GetRefillRatePerSec()))
	return int64(math.Max(0, float64(capacity)-available)), int64(math.Floor(available)), nil
}

func (s *Server) gcraUsage(ctx context.Context, key string, limit *quotav1.Limit, now time.Time) (int64, int64, error) {
	raw, ok, err := s.store.GCRAValue(ctx, key)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 1, 0, nil
	}
	burst := limit.GetBurst()
	if burst <= 0 {
		burst = limit.GetLimit()
	}
	toleranceMS := (float64(burst) / limit.GetRefillRatePerSec()) * 1000
	earliest := raw - toleranceMS
	if float64(now.UnixMilli()) < earliest {
		return 0, int64(math.Ceil(earliest - float64(now.UnixMilli()))), nil
	}
	return 1, 0, nil
}

func (s *Server) decisionFromResult(result backend.DecisionResult, supplied []*quotav1.Limit, cost int64, dryRun bool, op string) *quotav1.Decision {
	reason := quotav1.DecisionReason_DECISION_REASON_ALLOWED
	message := "allowed"
	if !result.Allowed {
		reason = quotav1.DecisionReason_DECISION_REASON_LIMIT_EXCEEDED
		message = op + " denied"
	}
	if result.Message != "" {
		message = result.Message
	}
	if dryRun && result.Allowed {
		reason = quotav1.DecisionReason_DECISION_REASON_DRY_RUN
		message = "dry run allowed; no counters were mutated"
	}
	statuses := make([]*quotav1.LimitStatus, 0, len(result.Statuses))
	for i, scriptStatus := range result.Statuses {
		var limit *quotav1.Limit
		if i < len(supplied) {
			limit = supplied[i]
		}
		st := &quotav1.LimitStatus{
			LimitId:      scriptStatus.LimitID,
			Used:         scriptStatus.Used,
			Remaining:    scriptStatus.Remaining,
			RetryAfterMs: scriptStatus.RetryAfterMS,
			Allowed:      scriptStatus.Allowed,
			Message:      scriptStatus.Message,
		}
		if limit != nil {
			st.ScopeKey = limit.GetScopeKey()
			st.Action = limit.GetAction()
			st.Unit = limit.GetUnit()
			st.Algorithm = limit.GetAlgorithm()
			st.Window = limit.GetWindow()
			st.Limit = limit.GetLimit()
			st.Cost = cost
		}
		if !st.GetAllowed() {
			if st.GetAlgorithm() == quotav1.Algorithm_ALGORITHM_CONCURRENCY {
				reason = quotav1.DecisionReason_DECISION_REASON_CONCURRENCY_EXCEEDED
			}
			s.metrics.Denial(st.GetAction(), s.product, st.GetLimitId())
		}
		statuses = append(statuses, st)
	}
	metadata := map[string]string{}
	if result.Cached {
		metadata["idempotency_hit"] = "true"
	}
	decisionID := result.DecisionID
	if decisionID == "" {
		decisionID = uuid.NewString()
	}
	return &quotav1.Decision{
		Allowed:       result.Allowed,
		DecisionId:    decisionID,
		Reason:        reason,
		Message:       message,
		RetryAfterMs:  result.RetryAfterMS,
		LimitStatuses: statuses,
		Metadata:      metadata,
	}
}

func (s *Server) decisionFromReservationIncrement(result backend.DecisionResult, reservation *quotav1.Reservation, cost int64) *quotav1.Decision {
	reason := quotav1.DecisionReason_DECISION_REASON_ALLOWED
	message := "reservation incremented"
	if !result.Allowed {
		reason = quotav1.DecisionReason_DECISION_REASON_LIMIT_EXCEEDED
		message = "increment reservation denied"
		if len(result.Statuses) == 0 {
			reason = quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST
		}
	}
	if result.Message != "" {
		message = result.Message
	}

	statuses := make([]*quotav1.LimitStatus, 0, len(result.Statuses))
	for i, scriptStatus := range result.Statuses {
		st := &quotav1.LimitStatus{
			LimitId:      scriptStatus.LimitID,
			Used:         scriptStatus.Used,
			Remaining:    scriptStatus.Remaining,
			RetryAfterMs: scriptStatus.RetryAfterMS,
			Allowed:      scriptStatus.Allowed,
			Message:      scriptStatus.Message,
			Cost:         cost,
		}
		if reservation != nil {
			st.Action = reservation.GetAction()
			if i < len(reservation.GetImpacts()) {
				impact := reservation.GetImpacts()[i]
				st.LimitId = impact.GetLimitId()
				st.ScopeKey = impact.GetScopeKey()
				st.Unit = impact.GetUnit()
				st.Algorithm = impact.GetAlgorithm()
				st.Limit = impact.GetLimit()
				st.ResetAtUnixMs = impact.GetResetAtUnixMs()
			}
		}
		if !st.GetAllowed() {
			s.metrics.Denial(st.GetAction(), s.product, st.GetLimitId())
		}
		statuses = append(statuses, st)
	}

	metadata := map[string]string{}
	if result.Cached {
		metadata["idempotency_hit"] = "true"
	}
	decisionID := result.DecisionID
	if decisionID == "" {
		decisionID = uuid.NewString()
	}
	return &quotav1.Decision{
		Allowed:       result.Allowed,
		DecisionId:    decisionID,
		Reason:        reason,
		Message:       message,
		RetryAfterMs:  result.RetryAfterMS,
		LimitStatuses: statuses,
		Metadata:      metadata,
	}
}

func (s *Server) recordMetrics(rpc, action string, decision *quotav1.Decision, start time.Time) {
	s.metrics.Observe(rpc, action, s.product, decision.GetAllowed(), decision.GetReason().String(), start)
}

func (s *Server) emit(ctx context.Context, eventType, requestID string, reqCtx *quotav1.RequestContext, action string, cost int64, decision *quotav1.Decision, reservation *quotav1.Reservation, lease *quotav1.Lease) {
	product := s.product
	environment := s.environment
	metadata := map[string]string{}
	if reqCtx != nil {
		if reqCtx.GetProduct() != "" {
			product = reqCtx.GetProduct()
		}
		if reqCtx.GetEnvironment() != "" {
			environment = reqCtx.GetEnvironment()
		}
		for k, v := range reqCtx.GetMetadata() {
			metadata[k] = v
		}
	}
	unit := ""
	if statuses := decision.GetLimitStatuses(); len(statuses) > 0 {
		unit = statuses[0].GetUnit()
	}
	s.events.Emit(ctx, Event{
		EventType:   eventType,
		Timestamp:   time.Now().UTC(),
		RequestID:   requestID,
		DecisionID:  decision.GetDecisionId(),
		Product:     product,
		Environment: environment,
		Action:      action,
		Unit:        unit,
		Cost:        cost,
		Allowed:     decision.GetAllowed(),
		LimitStatus: decision.GetLimitStatuses(),
		Metadata:    metadata,
		Reservation: reservation,
		Lease:       lease,
	})
}

func invalidDecision(message string) *quotav1.Decision {
	return &quotav1.Decision{
		Allowed:    false,
		DecisionId: uuid.NewString(),
		Reason:     quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST,
		Message:    message,
	}
}

func validationDecision(errs []*quotav1.ValidationError) *quotav1.Decision {
	message := "invalid limits"
	if len(errs) > 0 {
		message = errs[0].GetField() + ": " + errs[0].GetMessage()
	}
	return &quotav1.Decision{
		Allowed:    false,
		DecisionId: uuid.NewString(),
		Reason:     quotav1.DecisionReason_DECISION_REASON_INVALID_REQUEST,
		Message:    message,
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// noopEventSink is the default EventSink installed when Options
// leaves it nil. It drops every event so the server's hot path
// never blocks on absent wiring.
type noopEventSink struct{}

func (noopEventSink) Emit(context.Context, Event) {}
func (noopEventSink) Close() error                { return nil }

var _ quotav1.QuotaServiceServer = (*Server)(nil)
