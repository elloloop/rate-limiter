package ratelimiterserver

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elloloop/rate-limiter/internal/events"
	"github.com/elloloop/rate-limiter/ratelimiterserver/backend"
)

// EventSink is the contract for asynchronous quota-event delivery.
// Consumers that want their own sink (analytics queue, structured
// log pipeline, etc.) implement this interface and supply it on
// Options.EventSink. Leaving it nil installs a no-op sink that
// drops every event.
type EventSink = events.Sink

// Event is the payload an EventSink receives. The alias keeps
// consumers from having to import an internal package to satisfy
// EventSink.
type Event = events.Event

// Options configures a Server. Construct it directly and pass to
// [New]; the zero value is not usable — at minimum Backend must be
// set.
//
// The same Options shape is used both for embedded consumers and
// for cmd/quota-service. The container binary populates it from
// QUOTA_* environment variables; an embedded consumer typically
// builds it by hand.
type Options struct {
	// Product names the application that owns the quotas. It
	// becomes part of every Redis key and labels every emitted
	// metric / event. Required.
	Product string

	// Environment names the deployment slice (prod, staging, dev,
	// test). Like Product, it scopes Redis keys and labels metrics
	// / events. Required.
	Environment string

	// Backend persists counters, reservations, and leases. Today
	// the only supported implementation is
	// ratelimiterserver/backend/redis. Required.
	Backend backend.Backend

	// RedisMode is reported back through the GetRedisStatus RPC
	// for operator visibility. The only supported value is
	// "single_primary"; leaving it empty defaults to that.
	RedisMode string

	// EventSink receives quota-decision events emitted when a
	// request sets RequestOptions.emit_event=true. Leaving it nil
	// installs a no-op sink.
	EventSink EventSink

	// Logger receives the server's structured log output. nil
	// installs a no-op slog.Logger.
	Logger *slog.Logger

	// Metrics is the Prometheus registry the server records its
	// RED metrics into. nil creates a private isolated registry —
	// the embedded consumer typically wants to merge into its own
	// registry by constructing one and passing it here.
	Metrics prometheus.Registerer
}
