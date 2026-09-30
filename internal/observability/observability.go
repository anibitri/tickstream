// Package observability sets up what every service needs to be watched and
// run: JSON logs, Prometheus metrics, OpenTelemetry traces, health endpoints
// and a clean shutdown.
package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/anibitri/tickstream/internal/config"
)

// Obs bundles the observability handles of a running service.
type Obs struct {
	Log      *slog.Logger
	Registry *prometheus.Registry
	Tracer   trace.Tracer
	ready    atomic.Bool
	server   *http.Server
	shutdown []func(context.Context) error
}

// Setup initialises logging, metrics and tracing for service.
func Setup(ctx context.Context, service string, cfg config.Common) (*Obs, error) {
	if cfg.ServiceName != "" {
		service = cfg.ServiceName
	}
	o := &Obs{Registry: prometheus.NewRegistry()}
	o.Log = NewLogger(service, cfg.LogLevel)
	slog.SetDefault(o.Log)

	o.Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	Register(o.Registry)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if cfg.OTLPEndpoint != "" {
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(strings.TrimPrefix(strings.TrimPrefix(cfg.OTLPEndpoint, "http://"), "https://")),
			otlptracegrpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(service)))
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.TraceSampleRatio))),
		)
		otel.SetTracerProvider(tp)
		o.shutdown = append(o.shutdown, tp.Shutdown)
	}
	o.Tracer = otel.Tracer("github.com/anibitri/tickstream/" + service)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(o.Registry, promhttp.HandlerOpts{Registry: o.Registry}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !o.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
	o.server = &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return o, nil
}

// Serve starts the observability HTTP server in the background. Services that
// run their own HTTP server (api-gateway) mount Handler routes instead.
func (o *Obs) Serve() {
	go func() {
		if err := o.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			o.Log.Error("observability server failed", "err", err)
		}
	}()
}

// SetReady flips the /readyz response.
func (o *Obs) SetReady(ready bool) { o.ready.Store(ready) }

// Ready reports the current readiness state.
func (o *Obs) Ready() bool { return o.ready.Load() }

// Shutdown flushes traces and stops the HTTP server.
func (o *Obs) Shutdown(ctx context.Context) {
	if o.server != nil {
		_ = o.server.Shutdown(ctx)
	}
	for _, f := range o.shutdown {
		if err := f(ctx); err != nil {
			o.Log.Warn("observability shutdown", "err", err)
		}
	}
}

// NewLogger returns a JSON slog logger that adds trace_id/span_id from the context.
func NewLogger(service, level string) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(level))
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(traceHandler{h}).With("service", service)
}

type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(a)}
}
func (h traceHandler) WithGroup(n string) slog.Handler { return traceHandler{h.Handler.WithGroup(n)} }

// Run sets up observability, calls fn and stops on Ctrl-C or SIGTERM. If fn
// fails the process exits with status 1 so Docker restarts it; consumers then
// carry on from their last committed offset.
func Run(service string, common config.Common, fn func(ctx context.Context, o *Obs) error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	o, err := Setup(ctx, service, common)
	if err != nil {
		fmt.Fprintln(os.Stderr, "observability setup:", err)
		os.Exit(1)
	}
	o.Serve()
	o.Log.Info("starting")
	runErr := fn(ctx, o)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), common.ShutdownTimeout)
	defer cancel()
	o.Shutdown(shutdownCtx)
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		o.Log.Error("exiting with error", "err", runErr)
		os.Exit(1)
	}
	o.Log.Info("stopped")
}
