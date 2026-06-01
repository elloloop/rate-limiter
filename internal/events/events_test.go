package events

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
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
	sink.Emit(context.Background(), Event{EventType: "quota.test"})
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
	if err := sink.Close(); err != nil {
		t.Fatalf("stdout Close = %v, want nil", err)
	}
}

func TestNewRejectsUnsupportedSink(t *testing.T) {
	_, err := New("kafka", "", nullLogger())
	if err == nil || !strings.Contains(err.Error(), "unsupported event sink") {
		t.Fatalf("New unsupported sink error = %v", err)
	}
}

func TestNewRejectsInvalidPostgresURL(t *testing.T) {
	_, err := New("postgres", "%", nullLogger())
	if err == nil {
		t.Fatal("expected invalid postgres URL to fail")
	}
}

func TestNewPostgresFailsFastOnUnreachableDSN(t *testing.T) {
	_, err := New("postgres",
		"postgres://nobody@127.0.0.1:1/x?sslmode=disable&connect_timeout=1",
		nullLogger())
	if err == nil {
		t.Fatal("New(postgres) with unreachable DSN should fail")
	}
	if err.Error() == "" {
		t.Fatal("error message should be non-empty")
	}
}

func TestNewPostgresWithRealDatabase(t *testing.T) {
	dsn := os.Getenv("QUOTA_TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("set QUOTA_TEST_POSTGRES_URL to run Postgres-backed event sink tests")
	}

	sink, err := New("postgres", dsn, nullLogger())
	if err != nil {
		t.Fatalf("New(postgres): %v", err)
	}
	t.Cleanup(func() {
		if err := sink.Close(); err != nil {
			t.Fatalf("close postgres sink: %v", err)
		}
	})

	postgres, ok := sink.(*postgresSink)
	if !ok {
		t.Fatalf("New(postgres) returned %T, want *postgresSink", sink)
	}

	ctx := context.Background()
	if _, err := postgres.db.ExecContext(ctx, "DELETE FROM quota_usage_events WHERE request_id = 'req-real-postgres'"); err != nil {
		t.Fatalf("clear prior event: %v", err)
	}

	sink.Emit(ctx, Event{
		EventType:   "quota.consumed",
		Timestamp:   time.Unix(100, 0).UTC(),
		RequestID:   "req-real-postgres",
		DecisionID:  "decision-real-postgres",
		Product:     "workspace",
		Environment: "test",
		Action:      "workspace.events.consume",
		Unit:        "requests",
		Cost:        1,
		Allowed:     true,
		Metadata:    map[string]string{"scenario": "postgres-unit"},
	})

	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var count int
		err := postgres.db.QueryRowContext(ctx, `
SELECT count(*)
FROM quota_usage_events
WHERE event_type = 'quota.consumed'
  AND request_id = 'req-real-postgres'
  AND product = 'workspace'
  AND environment = 'test'
  AND action = 'workspace.events.consume'
  AND payload->>'decision_id' = 'decision-real-postgres'
  AND payload->'metadata'->>'scenario' = 'postgres-unit'`).Scan(&count)
		if err != nil {
			t.Fatalf("query inserted event: %v", err)
		}
		if count == 1 {
			return
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for persisted Postgres event")
		case <-tick.C:
		}
	}
}

func TestEventJSONShapeMatchesWireConvention(t *testing.T) {
	event := Event{
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
	encoded, err := json.Marshal(event)
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
		if !strings.Contains(string(encoded), want) {
			t.Fatalf("JSON missing %q in: %s", want, encoded)
		}
	}
}

func TestEventOmitsZeroOptionalFields(t *testing.T) {
	event := Event{
		EventType:   "consume",
		Timestamp:   time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC),
		Product:     "rl",
		Environment: "prod",
		Action:      "send",
		Allowed:     false,
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, field := range []string{"decision_id", "unit", "cost", "limit_statuses", "metadata", "reservation", "lease"} {
		if strings.Contains(string(encoded), field) {
			t.Fatalf("zero optional field %q leaked into JSON: %s", field, encoded)
		}
	}
}

func TestStdoutSinkWritesEventJSON(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("close reader: %v", err)
		}
	})

	originalStdout := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() {
		os.Stdout = originalStdout
		_ = writer.Close()
	})

	lines := make(chan string, 1)
	readErrs := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(reader).ReadString('\n')
		if err != nil {
			readErrs <- err
			return
		}
		lines <- line
	}()

	sink := stdoutSink{logger: nullLogger()}
	sink.Emit(context.Background(), Event{
		EventType:   "quota.consumed",
		Timestamp:   time.Unix(100, 0).UTC(),
		RequestID:   "req-1",
		DecisionID:  "decision-1",
		Product:     "workspace",
		Environment: "test",
		Action:      "workspace.email.recipients",
		Unit:        "recipients",
		Cost:        25,
		Allowed:     true,
		Metadata:    map[string]string{"account": "acct_1"},
	})

	select {
	case err := <-readErrs:
		t.Fatalf("read stdout event: %v", err)
	case line := <-lines:
		var got Event
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("unmarshal stdout event %q: %v", line, err)
		}
		if got.EventType != "quota.consumed" || got.RequestID != "req-1" || got.DecisionID != "decision-1" {
			t.Fatalf("unexpected event identity: %#v", got)
		}
		if got.Product != "workspace" || got.Environment != "test" || got.Action != "workspace.email.recipients" {
			t.Fatalf("unexpected event labels: %#v", got)
		}
		if got.Metadata["account"] != "acct_1" {
			t.Fatalf("metadata missing from event: %#v", got.Metadata)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stdout event")
	}
}

func TestStdoutSinkLogsMarshalFailures(t *testing.T) {
	logs := &lockedBuffer{}
	sink := stdoutSink{logger: slog.New(slog.NewTextHandler(logs, nil))}

	sink.Emit(context.Background(), eventWithMarshalFailure())

	waitForLog(t, logs, "event marshal failed")
}

func TestPostgresSinkEmitInsertsEventAndCloses(t *testing.T) {
	execs := make(chan eventTestExec, 1)
	db := sql.OpenDB(eventTestConnector{execs: execs})
	sink := &postgresSink{
		db:     db,
		logger: nullLogger(),
	}
	event := Event{
		EventType:   "quota.consumed",
		Timestamp:   time.Unix(100, 0).UTC(),
		RequestID:   "req-postgres",
		DecisionID:  "decision-postgres",
		Product:     "workspace",
		Environment: "prod",
		Action:      "workspace.email.recipients",
		Unit:        "recipients",
		Cost:        3,
		Allowed:     true,
		Metadata:    map[string]string{"account": "acct_1"},
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	sink.Emit(requestCtx, event)

	select {
	case exec := <-execs:
		if !strings.Contains(exec.query, "INSERT INTO quota_usage_events") {
			t.Fatalf("unexpected insert query: %s", exec.query)
		}
		if len(exec.args) != 7 {
			t.Fatalf("insert args len = %d, want 7", len(exec.args))
		}
		assertNamedValue(t, exec.args[0], "quota.consumed")
		assertNamedValue(t, exec.args[2], "workspace")
		assertNamedValue(t, exec.args[3], "prod")
		assertNamedValue(t, exec.args[4], "workspace.email.recipients")
		assertNamedValue(t, exec.args[5], "req-postgres")
		payload, ok := exec.args[6].Value.([]byte)
		if !ok {
			t.Fatalf("payload arg type = %T, want []byte", exec.args[6].Value)
		}
		var stored Event
		if err := json.Unmarshal(payload, &stored); err != nil {
			t.Fatalf("unmarshal stored payload: %v", err)
		}
		if stored.DecisionID != "decision-postgres" || stored.Metadata["account"] != "acct_1" {
			t.Fatalf("unexpected stored event payload: %#v", stored)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for postgres event insert")
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPostgresSinkLogsMarshalFailures(t *testing.T) {
	logs := &lockedBuffer{}
	db := sql.OpenDB(eventTestConnector{execs: make(chan eventTestExec, 1)})
	sink := &postgresSink{
		db:     db,
		logger: slog.New(slog.NewTextHandler(logs, nil)),
	}
	t.Cleanup(func() {
		_ = sink.Close()
	})

	sink.Emit(context.Background(), eventWithMarshalFailure())

	waitForLog(t, logs, "event marshal failed")
}

func TestPostgresSinkLogsInsertFailures(t *testing.T) {
	logs := &lockedBuffer{}
	execs := make(chan eventTestExec, 1)
	db := sql.OpenDB(eventTestConnector{
		execs:   execs,
		execErr: errors.New("insert failed"),
	})
	sink := &postgresSink{
		db:     db,
		logger: slog.New(slog.NewTextHandler(logs, nil)),
	}
	t.Cleanup(func() {
		_ = sink.Close()
	})

	sink.Emit(context.Background(), Event{
		EventType:   "quota.consumed",
		Timestamp:   time.Unix(100, 0).UTC(),
		RequestID:   "req-postgres",
		Product:     "workspace",
		Environment: "prod",
		Action:      "workspace.email.recipients",
		Allowed:     true,
	})

	select {
	case <-execs:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for insert attempt")
	}
	waitForLog(t, logs, "event insert failed")
}

func assertNamedValue(t *testing.T, got driver.NamedValue, want string) {
	t.Helper()
	if got.Value != want {
		t.Fatalf("arg %d = %v, want %q", got.Ordinal, got.Value, want)
	}
}

type eventTestExec struct {
	query string
	args  []driver.NamedValue
}

type eventTestConnector struct {
	execs   chan eventTestExec
	execErr error
}

func (c eventTestConnector) Connect(context.Context) (driver.Conn, error) {
	return eventTestConn(c), nil
}

func (eventTestConnector) Driver() driver.Driver {
	return eventTestDriver{}
}

type eventTestDriver struct{}

func (eventTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("open is not used by sql.OpenDB")
}

type eventTestConn struct {
	execs   chan eventTestExec
	execErr error
}

func (eventTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not supported")
}

func (eventTestConn) Close() error { return nil }

func (eventTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c eventTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.execs <- eventTestExec{
		query: query,
		args:  append([]driver.NamedValue(nil), args...),
	}
	return driver.RowsAffected(1), c.execErr
}

func eventWithMarshalFailure() Event {
	return Event{
		EventType:   "quota.reserved",
		Timestamp:   time.Unix(100, 0).UTC(),
		RequestID:   "req-invalid-json",
		Product:     "workspace",
		Environment: "prod",
		Action:      "workspace.email.recipients",
		Allowed:     true,
		Reservation: &quotav1.Reservation{
			ReservationId: "res-invalid-json",
			Impacts: []*quotav1.ReservationImpact{{
				RefillRatePerSec: math.NaN(),
			}},
		},
	}
}

func waitForLog(t *testing.T, logs *lockedBuffer, want string) {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if strings.Contains(logs.String(), want) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("log missing %q: %s", want, logs.String())
		case <-tick.C:
		}
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
