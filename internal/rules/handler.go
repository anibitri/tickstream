package rules

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
)

// Handler runs one rules Engine per assigned md.metrics partition.
type Handler struct {
	Set      *Set
	InTopic  string // md.metrics or replay.<run_id>.metrics
	OutTopic string // risk.alerts or replay.<run_id>.alerts
	DLQTopic string
	Tracer   trace.Tracer
	Log      *slog.Logger
	OnAlert  func(*domain.Alert) // optional hook (tests, backtests)
	engines  map[int32]*Engine
	mu       sync.Mutex
	nowFn    func() time.Time
}

func (h *Handler) engine(p int32) *Engine {
	if h.engines == nil {
		h.engines = make(map[int32]*Engine)
	}
	e := h.engines[p]
	if e == nil {
		e = NewEngine(h.Set)
		h.engines[p] = e
	}
	return e
}

func (h *Handler) now() time.Time {
	if h.nowFn != nil {
		return h.nowFn()
	}
	return time.Now()
}

// Assigned restores each partition's checkpoint into a fresh engine.
func (h *Handler) Assigned(topic string, parts map[int32]string) {
	if topic != h.InTopic {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.engines == nil {
		h.engines = make(map[int32]*Engine)
	}
	for p, meta := range parts {
		e := NewEngine(h.Set)
		if err := e.Restore(meta); err != nil {
			h.Log.Warn("ignoring unreadable checkpoint", "partition", p, "err", err)
		}
		h.engines[p] = e
	}
}

// Revoked drops state for partitions this instance no longer owns.
func (h *Handler) Revoked(topic string, parts []int32) {
	if topic != h.InTopic {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range parts {
		delete(h.engines, p)
	}
}

// Handle evaluates rules for each metrics record.
func (h *Handler) Handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*kgo.Record
	for _, r := range recs {
		var m domain.Metrics
		if err := proto.Unmarshal(r.Value, &m); err != nil {
			observability.DLQMessages.WithLabelValues("decode").Inc()
			out = append(out, kafkax.DeadLetterRecord(h.DLQTopic, "risk-engine", r, err, h.now().UnixNano()))
			continue
		}
		for _, a := range h.engine(r.Partition).Evaluate(&m, r.Offset) {
			rec, err := kafkax.ProtoRecord(h.OutTopic, a.Symbol, a)
			if err != nil {
				return nil, err
			}
			if h.Tracer != nil {
				parent := kafkax.Extract(ctx, r)
				if trace.SpanContextFromContext(parent).IsSampled() {
					spanCtx, span := h.Tracer.Start(parent, "risk alert "+a.RuleName,
						trace.WithAttributes(attribute.String("symbol", a.Symbol), attribute.String("alert_id", a.AlertId)))
					kafkax.Inject(spanCtx, rec)
					span.End()
				}
			}
			observability.AlertsFired.WithLabelValues(a.RuleName, a.Severity.String()).Inc()
			h.Log.Info("alert", "rule", a.RuleName, "symbol", a.Symbol, "exchange", a.Exchange,
				"severity", a.Severity.String(), "alert_id", a.AlertId, "message", a.Message)
			if h.OnAlert != nil {
				h.OnAlert(a)
			}
			out = append(out, rec)
		}
	}
	return out, nil
}

// CommitOffsets commits each partition's low-water offset plus checkpoint.
func (h *Handler) CommitOffsets() kafkax.Offsets {
	h.mu.Lock()
	defer h.mu.Unlock()
	offs := make(kafkax.Offsets)
	for p, e := range h.engines {
		if low := e.LowWater(); low >= 0 {
			offs.Set(h.InTopic, p, kafkax.Commit{Offset: low, Metadata: e.Checkpoint()})
		}
	}
	return offs
}
