package service

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
	"github.com/elloloop/rate-limiter/internal/config"
	"github.com/elloloop/rate-limiter/internal/events"
	"github.com/elloloop/rate-limiter/internal/metrics"
	"github.com/elloloop/rate-limiter/internal/redisstore"
)

func TestConsumeIsAtomicAndIdempotentWithRedis(t *testing.T) {
	redisURL := os.Getenv("QUOTA_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("set QUOTA_TEST_REDIS_URL to run Redis integration tests")
	}

	ctx := context.Background()
	store, err := redisstore.New(ctx, redisURL)
	if err != nil {
		t.Fatalf("new redis store: %v", err)
	}
	defer store.Close()
	if err := store.Client().FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}

	svc := New(
		config.Config{Product: "workspace", Environment: "test", RedisMode: "single_primary"},
		store,
		eventsMust(t),
		metrics.New(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

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

func eventsMust(t *testing.T) events.Sink {
	t.Helper()
	sink, err := events.New("none", "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("event sink: %v", err)
	}
	return sink
}

func fixedDayLimit() *quotav1.Limit {
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
		Limit: 30,
	}
}
