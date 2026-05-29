package events

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func nullLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewNoneReturnsNoopSink(t *testing.T) {
	sink, err := New("none", "", nullLogger())
	if err != nil {
		t.Fatalf("New(none): %v", err)
	}
	if _, ok := sink.(noopSink); !ok {
		t.Fatalf("New(none) returned %T, want noopSink", sink)
	}
	// Emit + Close must not panic and Close must return nil.
	sink.Emit(context.Background(), Event{EventType: "x"})
	if err := sink.Close(); err != nil {
		t.Fatalf("noop Close = %v, want nil", err)
	}
}

func TestNewStdoutReturnsStdoutSink(t *testing.T) {
	sink, err := New("stdout", "", nullLogger())
	if err != nil {
		t.Fatalf("New(stdout): %v", err)
	}
	if _, ok := sink.(stdoutSink); !ok {
		t.Fatalf("New(stdout) returned %T, want stdoutSink", sink)
	}
	// Close is trivially nil.
	if err := sink.Close(); err != nil {
		t.Fatalf("stdout Close = %v, want nil", err)
	}
	// Emit fires a goroutine that marshals + writes; no return value to
	// inspect. The contract here is "must not panic for a valid event."
	sink.Emit(context.Background(), Event{
		EventType:   "test",
		Timestamp:   time.Now().UTC(),
		Product:     "rl",
		Environment: "test",
		Action:      "evt",
	})
	// Give the goroutine a moment so a race-detector run sees the work.
	time.Sleep(10 * time.Millisecond)
}

func TestNewUnknownKindRejected(t *testing.T) {
	_, err := New("redis", "", nullLogger())
	if err == nil {
		t.Fatal("New(redis) should reject unknown kind")
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Fatalf("error should name the offending kind: %v", err)
	}
}

func TestNewPostgresFailsFastOnUnreachableDSN(t *testing.T) {
	// connect_timeout=1 keeps this snappy; the DSN points at an unroutable
	// address so init's first ExecContext fails before the test deadline.
	_, err := New("postgres",
		"postgres://nobody@127.0.0.1:1/x?sslmode=disable&connect_timeout=1",
		nullLogger())
	if err == nil {
		t.Fatal("New(postgres) with unreachable DSN should error at construction, not at first Emit")
	}
	// We don't assert on the exact wording — pgx's error text isn't stable
	// across versions — but it must be non-empty so the operator can act on it.
	if err.Error() == "" {
		t.Fatal("error message should be non-empty")
	}
}

// TestEventJSONShapeMatchesWireConvention is a structural regression test:
// the snake_case JSON field names are the contract postgres/stdout sinks
// emit and that downstream consumers (analytics, log pipelines) parse.
// Renaming a struct field without updating the json tag would silently
// break consumers.
func TestEventJSONShapeMatchesWireConvention(t *testing.T) {
	e := Event{
		EventType:   "consume",
		Timestamp:   time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC),
		RequestID:   "req-1",
		DecisionID:  "dec-1",
		Product:     "rl",
		Environment: "prod",
		Action:      "send",
		Unit:        "messages",
		Cost:        3,
		Allowed:     true,
		Metadata:    map[string]string{"k": "v"},
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"event_type":"consume"`,
		`"timestamp":"2026-05-29T12:00:00Z"`,
		`"request_id":"req-1"`,
		`"decision_id":"dec-1"`,
		`"product":"rl"`,
		`"environment":"prod"`,
		`"action":"send"`,
		`"unit":"messages"`,
		`"cost":3`,
		`"allowed":true`,
		`"metadata":{"k":"v"}`,
	} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("JSON missing %q in: %s", want, b)
		}
	}
}

// TestEventOmitsZeroOptionalFields documents the wire shape: optional
// fields use `omitempty` so a minimal event does not carry trailing nulls
// that confuse downstream parsers.
func TestEventOmitsZeroOptionalFields(t *testing.T) {
	e := Event{
		EventType:   "consume",
		Timestamp:   time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC),
		Product:     "rl",
		Environment: "prod",
		Action:      "send",
		Allowed:     false,
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, mustNotAppear := range []string{
		"decision_id", "unit", "cost", "limit_statuses", "metadata", "reservation", "lease",
	} {
		if strings.Contains(string(b), mustNotAppear) {
			t.Fatalf("zero optional field %q leaked into JSON: %s", mustNotAppear, b)
		}
	}
}
