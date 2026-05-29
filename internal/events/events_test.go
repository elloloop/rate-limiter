package events

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
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
	execs chan eventTestExec
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
	execs chan eventTestExec
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
	return driver.RowsAffected(1), nil
}
