package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver with database/sql

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

type Sink interface {
	Emit(context.Context, Event)
	Close() error
}

type Event struct {
	EventType   string                 `json:"event_type"`
	Timestamp   time.Time              `json:"timestamp"`
	RequestID   string                 `json:"request_id"`
	DecisionID  string                 `json:"decision_id,omitempty"`
	Product     string                 `json:"product"`
	Environment string                 `json:"environment"`
	Action      string                 `json:"action"`
	Unit        string                 `json:"unit,omitempty"`
	Cost        int64                  `json:"cost,omitempty"`
	Allowed     bool                   `json:"allowed"`
	LimitStatus []*quotav1.LimitStatus `json:"limit_statuses,omitempty"`
	Metadata    map[string]string      `json:"metadata,omitempty"`
	Reservation *quotav1.Reservation   `json:"reservation,omitempty"`
	Lease       *quotav1.Lease         `json:"lease,omitempty"`
}

func New(kind, databaseURL string, logger *slog.Logger) (Sink, error) {
	switch kind {
	case "none":
		return noopSink{}, nil
	case "stdout":
		return stdoutSink{logger: logger}, nil
	case "postgres":
		db, err := sql.Open("pgx", databaseURL)
		if err != nil {
			return nil, err
		}
		sink := &postgresSink{db: db, logger: logger}
		if err := sink.init(context.Background()); err != nil {
			_ = db.Close()
			return nil, err
		}
		return sink, nil
	default:
		return nil, fmt.Errorf("unsupported event sink %q", kind)
	}
}

type noopSink struct{}

func (noopSink) Emit(context.Context, Event) {}
func (noopSink) Close() error                { return nil }

type stdoutSink struct {
	logger *slog.Logger
}

func (s stdoutSink) Emit(_ context.Context, event Event) {
	go func() {
		encoded, err := json.Marshal(event)
		if err != nil {
			s.logger.Warn("event marshal failed", "error", err)
			return
		}
		_, _ = os.Stdout.Write(append(encoded, '\n'))
	}()
}

func (stdoutSink) Close() error { return nil }

type postgresSink struct {
	db     *sql.DB
	logger *slog.Logger
}

func (s *postgresSink) init(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS quota_usage_events (
  id BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  product TEXT NOT NULL,
  environment TEXT NOT NULL,
  action TEXT NOT NULL,
  request_id TEXT NOT NULL,
  payload JSONB NOT NULL
)`)
	return err
}

func (s *postgresSink) Emit(ctx context.Context, event Event) {
	payload, err := json.Marshal(event)
	if err != nil {
		s.logger.Warn("event marshal failed", "error", err)
		return
	}
	go func() { //nolint:gosec,contextcheck // delivery is best-effort and must outlive the request context (see below)
		// Event delivery is best-effort and must not inherit an RPC context that
		// may be canceled as soon as the hot-path handler returns.
		insertCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := s.db.ExecContext(insertCtx, `
INSERT INTO quota_usage_events (event_type, event_time, product, environment, action, request_id, payload)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			event.EventType,
			event.Timestamp,
			event.Product,
			event.Environment,
			event.Action,
			event.RequestID,
			payload,
		)
		if err != nil {
			s.logger.Warn("event insert failed", "error", err)
		}
	}()
}

func (s *postgresSink) Close() error {
	return s.db.Close()
}
