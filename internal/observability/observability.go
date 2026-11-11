// Package observability sets up structured logs (slog JSON), Prometheus metrics and OpenTelemetry traces.
package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the tracer every package uses.
var Tracer trace.Tracer = otel.Tracer("github.com/guzmanloeracode/relay")

// NewLogger returns a JSON logger at the given level ("debug", "info", "warn", "error").
func NewLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

// SetupTracing exports spans over OTLP/HTTP when endpoint is set; otherwise spans are dropped. It returns a
// shutdown function that flushes the exporter.
func SetupTracing(ctx context.Context, endpoint, service, version string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName(service), semconv.ServiceVersion(version)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))),
	)
	otel.SetTracerProvider(tp)
	Tracer = tp.Tracer("github.com/guzmanloeracode/relay")
	return tp.Shutdown, nil
}

// Metrics are every Prometheus series Relay exports (the Grafana dashboard reads these names).
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests     *prometheus.CounterVec
	HTTPDuration     *prometheus.HistogramVec
	EventsIngested   *prometheus.CounterVec
	IngestDuration   prometheus.Histogram
	RateLimited      *prometheus.CounterVec
	EventsRouted     prometheus.Counter
	DeliveriesRouted *prometheus.CounterVec
	RoutingSkipped   *prometheus.CounterVec
	Attempts         *prometheus.CounterVec
	AttemptDuration  *prometheus.HistogramVec
	DeliveryLag      *prometheus.HistogramVec
	Deferred         *prometheus.CounterVec
	DeadLettered     *prometheus.CounterVec
	EndpointDisabled *prometheus.CounterVec
	Requeued         *prometheus.CounterVec
	PushTokensPruned prometheus.Counter
	QueueDepth       *prometheus.GaugeVec
	InFlight         prometheus.Gauge
}

// NewMetrics registers every series on a fresh registry (with Go and process collectors).
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := factory{r}
	latency := []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	return &Metrics{
		Registry:       r,
		HTTPRequests:   f.counterVec("relay_http_requests_total", "API requests by route and status class.", "route", "method", "code"),
		HTTPDuration:   f.histogramVec("relay_http_request_duration_seconds", "API request duration by route.", latency, "route", "method"),
		EventsIngested: f.counterVec("relay_events_ingested_total", "Events accepted by POST /v1/events.", "result"),
		IngestDuration: f.histogram("relay_ingest_duration_seconds", "Time to accept one event (auth, rate limit, insert, enqueue).",
			[]float64{.001, .002, .005, .01, .02, .03, .05, .075, .1, .25, .5, 1}),
		RateLimited:      f.counterVec("relay_rate_limited_total", "Requests or deliveries held back by a limit.", "scope"),
		EventsRouted:     f.counter("relay_events_routed_total", "Events fanned out to their deliveries."),
		DeliveriesRouted: f.counterVec("relay_deliveries_created_total", "Deliveries created by routing or replay.", "channel", "source"),
		RoutingSkipped:   f.counterVec("relay_routing_skipped_total", "Matching endpoints that got no delivery.", "channel", "reason"),
		Attempts:         f.counterVec("relay_delivery_attempts_total", "Delivery attempts by channel and outcome.", "channel", "outcome"),
		AttemptDuration:  f.histogramVec("relay_delivery_attempt_duration_seconds", "Time of one delivery attempt.", latency, "channel"),
		DeliveryLag: f.histogramVec("relay_delivery_lag_seconds", "From an event's ingestion to its successful delivery.",
			[]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 3600, 21600, 86400}, "channel"),
		Deferred:         f.counterVec("relay_deliveries_deferred_total", "Deliveries parked or delayed by a limit before their attempt.", "reason"),
		DeadLettered:     f.counterVec("relay_dead_letters_total", "Deliveries moved to the dead-letter queue.", "reason"),
		EndpointDisabled: f.counterVec("relay_endpoints_disabled_total", "Endpoints disabled automatically.", "reason"),
		Requeued:         f.counterVec("relay_requeued_total", "Work found again by the sweeper.", "kind"),
		PushTokensPruned: f.counter("relay_push_tokens_pruned_total", "Expo push tokens deleted after DeviceNotRegistered."),
		QueueDepth:       f.gaugeVec("relay_queue_depth", "Work waiting in Redis.", "queue"),
		InFlight:         f.gauge("relay_deliveries_in_flight", "Delivery attempts running in this worker."),
	}
}

type factory struct{ r *prometheus.Registry }

func (f factory) counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	f.r.MustRegister(c)
	return c
}

func (f factory) counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	f.r.MustRegister(c)
	return c
}

func (f factory) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	f.r.MustRegister(g)
	return g
}

func (f factory) gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	f.r.MustRegister(g)
	return g
}

func (f factory) histogram(name, help string, buckets []float64) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
	f.r.MustRegister(h)
	return h
}

func (f factory) histogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	f.r.MustRegister(h)
	return h
}

// ServeMetrics serves /metrics on addr until ctx ends. It is a separate listener so the reverse proxy never
// exposes it.
func ServeMetrics(ctx context.Context, addr string, m *Metrics, log *slog.Logger) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server stopped", "err", err)
		}
	}()
}
