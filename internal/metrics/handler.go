package metrics

import (
	"context"
	"log/slog"
	"strconv"
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

// Handler runs one Engine per assigned md.trades partition (kafkax.Handler,
// Committer, Rebalancer and Ticker).
type Handler struct {
	Cfg         Config
	InTopic     string // md.trades or replay.<run_id>.trades
	OutTopic    string // md.metrics or replay.<run_id>.metrics
	DLQTopic    string
	Live        bool          // wall-clock emitted_at, latency metrics, idle watermark advance
	IdleAdvance time.Duration // live only: advance an idle partition's watermark to now − IdleAdvance
	Tracer      trace.Tracer
	Log         *slog.Logger
	Now         func() time.Time

	mu      sync.Mutex
	engines map[int32]*partitionState
}

type partitionState struct {
	eng          *Engine
	lastActivity time.Time
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *Handler) newEngine() *Engine {
	e := NewEngine(h.Cfg)
	if h.Live {
		e.Clock = WallClock
	}
	return e
}

func (h *Handler) state(p int32) *partitionState {
	if h.engines == nil {
		h.engines = make(map[int32]*partitionState)
	}
	st := h.engines[p]
	if st == nil {
		st = &partitionState{eng: h.newEngine(), lastActivity: h.now()}
		h.engines[p] = st
	}
	return st
}

// Assigned creates a fresh engine per partition and restores its checkpoint.
func (h *Handler) Assigned(topic string, parts map[int32]string) {
	if topic != h.InTopic {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.engines == nil {
		h.engines = make(map[int32]*partitionState)
	}
	for p, meta := range parts {
		e := h.newEngine()
		if err := e.Restore(meta); err != nil {
			h.Log.Warn("ignoring unreadable checkpoint", "partition", p, "err", err)
		} else if meta != "" {
			h.Log.Info("restored checkpoint", "partition", p, "checkpoint", meta)
		}
		h.engines[p] = &partitionState{eng: e, lastActivity: h.now()}
	}
}

// Revoked drops in-memory state; the new owner rebuilds it from the checkpoint.
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

// Handle folds trades into the partition engines and returns closed windows.
func (h *Handler) Handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	var out []*kgo.Record
	for _, r := range recs {
		st := h.state(r.Partition)
		st.lastActivity = now
		var t domain.Trade
		if err := proto.Unmarshal(r.Value, &t); err != nil {
			out = append(out, kafkax.DeadLetterRecord(h.DLQTopic, "metrics-engine", r, err, now.UnixNano()))
			observability.DLQMessages.WithLabelValues("decode").Inc()
			continue
		}
		sc := trace.SpanContextFromContext(kafkax.Extract(ctx, r))
		em, outcome, err := st.eng.Add(&t, r.Offset, sc)
		if err != nil {
			out = append(out, kafkax.DeadLetterRecord(h.DLQTopic, "metrics-engine", r, err, now.UnixNano()))
			observability.DLQMessages.WithLabelValues("decimal").Inc()
			continue
		}
		switch outcome {
		case Late:
			observability.LateTrades.Inc()
		case Duplicate:
			observability.DuplicatesDropped.WithLabelValues("metrics-engine").Inc()
		}
		recv := kafkax.HeaderInt64(r, kafkax.HeaderRecvTimeNs)
		for _, e := range em {
			rec, err := h.record(ctx, e, recv)
			if err != nil {
				return nil, err
			}
			out = append(out, rec)
		}
	}
	return out, nil
}

// Tick advances the watermark of idle partitions in live mode so quiet
// symbols still emit windows. Replay mode never does this (it would make the
// output depend on processing speed).
func (h *Handler) Tick(ctx context.Context) ([]*kgo.Record, error) {
	if !h.Live || h.IdleAdvance <= 0 {
		return nil, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	var out []*kgo.Record
	for _, st := range h.engines {
		if now.Sub(st.lastActivity) < h.IdleAdvance {
			continue
		}
		for _, e := range st.eng.AdvanceWatermark(now.Add(-h.IdleAdvance).UnixNano()) {
			rec, err := h.record(ctx, e, 0)
			if err != nil {
				return nil, err
			}
			out = append(out, rec)
		}
	}
	return out, nil
}

func (h *Handler) record(ctx context.Context, e Emitted, triggerRecvNs int64) (*kgo.Record, error) {
	m := e.Metrics
	rec, err := kafkax.ProtoRecord(h.OutTopic, m.Symbol, m, m.WindowEndNs)
	if err != nil {
		return nil, err
	}
	if h.Tracer != nil && e.Trace.IsSampled() {
		spanCtx, span := h.Tracer.Start(trace.ContextWithRemoteSpanContext(ctx, e.Trace), "metrics window",
			trace.WithAttributes(attribute.String("symbol", m.Symbol), attribute.Int("window_secs", int(m.WindowSecs)),
				attribute.Int64("window_end_ns", m.WindowEndNs), attribute.Int64("trade_count", m.TradeCount)))
		kafkax.Inject(spanCtx, rec)
		span.End()
	}
	secs := strconv.Itoa(int(m.WindowSecs))
	observability.WindowsEmitted.WithLabelValues(secs).Inc()
	if v, err := strconv.ParseFloat(m.Vwap, 64); err == nil {
		observability.LastVWAP.WithLabelValues(m.Symbol, secs).Set(v)
	}
	if m.WindowSecs == 1 {
		observability.LastSpreadBps.WithLabelValues(m.Symbol).Set(m.XexSpreadBps)
	}
	if h.Live {
		now := h.now().UnixNano()
		if triggerRecvNs > 0 {
			observability.ProcessingLatency.Observe(float64(now-triggerRecvNs) / 1e9)
		}
		observability.EndToEndLatency.Observe(float64(now-m.WindowEndNs) / 1e9)
	}
	return rec, nil
}

// CommitOffsets returns each partition's low-water offset and checkpoint.
func (h *Handler) CommitOffsets() kafkax.Offsets {
	h.mu.Lock()
	defer h.mu.Unlock()
	offs := make(kafkax.Offsets)
	for p, st := range h.engines {
		if low := st.eng.LowWater(); low >= 0 {
			offs.Set(h.InTopic, p, kafkax.Commit{Offset: low, Metadata: st.eng.Checkpoint()})
		}
	}
	return offs
}
