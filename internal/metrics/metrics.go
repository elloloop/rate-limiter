package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	requests        *prometheus.CounterVec
	duration        *prometheus.HistogramVec
	denials         *prometheus.CounterVec
	redisErrors     prometheus.Counter
	idempotencyHits prometheus.Counter
	reservations    prometheus.Gauge
	resExpired      prometheus.Counter
	leases          prometheus.Gauge
	overages        prometheus.Counter
	eventEmitErrors prometheus.Counter
	registry        *prometheus.Registry
}

// New registers the quota service's metrics. When reg is nil a
// private *prometheus.Registry is created and used as both the
// register-target and the scrape-source for Handler / Serve — that
// is the container path. When reg is supplied (the embedded path),
// the metrics are registered into the host registry and the
// Handler / Serve helpers are not available because the host owns
// scrape.
func New(reg prometheus.Registerer) *Metrics {
	var private *prometheus.Registry
	register := reg
	if register == nil {
		private = prometheus.NewRegistry()
		register = private
	}
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quota_requests_total",
			Help: "Total quota RPC requests.",
		}, []string{"rpc", "action", "product", "allowed", "reason"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "quota_request_duration_ms",
			Help:    "Quota RPC duration in milliseconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"rpc", "action", "product"}),
		denials: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quota_denials_total",
			Help: "Quota denials by action, product, and limit.",
		}, []string{"action", "product", "limit_id"}),
		redisErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quota_redis_errors_total",
			Help: "Redis operation errors.",
		}),
		idempotencyHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quota_idempotency_hits_total",
			Help: "Idempotent request replays served from Redis.",
		}),
		reservations: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "quota_reservations_active",
			Help: "Active reservations created by this process.",
		}),
		resExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quota_reservations_expired_total",
			Help: "Expired reservation count observed by this process.",
		}),
		leases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "quota_leases_active",
			Help: "Active leases created by this process.",
		}),
		overages: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quota_overages_total",
			Help: "Reservation finalization overages.",
		}),
		eventEmitErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "quota_event_emit_errors_total",
			Help: "Asynchronous event emission errors.",
		}),
		registry: private,
	}
	register.MustRegister(
		m.requests,
		m.duration,
		m.denials,
		m.redisErrors,
		m.idempotencyHits,
		m.reservations,
		m.resExpired,
		m.leases,
		m.overages,
		m.eventEmitErrors,
	)
	return m
}

func (m *Metrics) Observe(rpc, action, product string, allowed bool, reason string, start time.Time) {
	allowedLabel := "false"
	if allowed {
		allowedLabel = "true"
	}
	m.requests.WithLabelValues(rpc, action, product, allowedLabel, reason).Inc()
	m.duration.WithLabelValues(rpc, action, product).Observe(float64(time.Since(start).Milliseconds()))
}

func (m *Metrics) Denial(action, product, limitID string) {
	m.denials.WithLabelValues(action, product, limitID).Inc()
}

func (m *Metrics) RedisError()     { m.redisErrors.Inc() }
func (m *Metrics) IdempotencyHit() { m.idempotencyHits.Inc() }
func (m *Metrics) ReservationInc() { m.reservations.Inc() }
func (m *Metrics) ReservationDec() { m.reservations.Dec() }
func (m *Metrics) LeaseInc()       { m.leases.Inc() }
func (m *Metrics) LeaseDec()       { m.leases.Dec() }
func (m *Metrics) Overage()        { m.overages.Inc() }
func (m *Metrics) ReservationsExpired(count float64) {
	m.resExpired.Add(count)
}

// Handler exposes the private metrics registry over HTTP. It is
// only usable when New was called with a nil registerer (the
// container path); embedded consumers register into their own
// registry and serve scrape themselves.
func (m *Metrics) Handler() http.Handler {
	if m.registry == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics handler is unavailable: a host-supplied prometheus.Registerer is in use", http.StatusNotFound)
		})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) Serve(ctx context.Context, bindAddr string) error {
	server := &http.Server{
		Addr:              bindAddr,
		Handler:           m.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { //nolint:gosec // shutdown runs after ctx is canceled and must use a fresh context
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx) //nolint:contextcheck // deliberate: ctx is already canceled, shutdown needs a fresh context
	}()
	err := server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
