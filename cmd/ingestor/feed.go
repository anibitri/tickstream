package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/exchange"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
)

// Publisher sends a record asynchronously.
type Publisher func(ctx context.Context, r *kgo.Record)

// Feed keeps one exchange websocket connected and publishes every trade
// message to Kafka as-is, adding only the time we received it.
type Feed struct {
	Adapter      exchange.Adapter
	Symbols      []string
	RawTopic     string
	AlertsTopic  string
	HealthTopic  string
	Publish      Publisher
	Backoff      Backoff
	ReadTimeout  time.Duration // no frame at all within this => reconnect
	GapThreshold time.Duration // disconnect gaps longer than this raise a feed_gap alert
	StaleAfter   time.Duration // symbol with no trade for this long reports STALE
	Tracer       trace.Tracer
	Log          *slog.Logger
	Now          func() time.Time
	Recorder     func(payload []byte) // optional: record raw frames to disk

	connected  atomic.Bool
	reconnects atomic.Int64
	lastMsgNs  atomic.Int64
	mu         sync.Mutex
	lastTrade  map[string]int64 // symbol -> receive time of last trade
}

func (f *Feed) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

// Run connects and reconnects until ctx is cancelled.
func (f *Feed) Run(ctx context.Context) error {
	name := f.Adapter.Name()
	f.mu.Lock()
	f.lastTrade = make(map[string]int64, len(f.Symbols))
	f.mu.Unlock()
	attempt := 0
	var disconnectedAt int64
	for ctx.Err() == nil {
		gotData, err := f.session(ctx, disconnectedAt)
		f.connected.Store(false)
		observability.FeedConnected.WithLabelValues(name).Set(0)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if gotData {
			attempt = 0
		}
		disconnectedAt = f.lastMsgNs.Load()
		if disconnectedAt == 0 {
			disconnectedAt = f.now().UnixNano()
		}
		delay := f.Backoff.Delay(attempt)
		attempt++
		f.reconnects.Add(1)
		observability.WSReconnects.WithLabelValues(name).Inc()
		f.Log.Warn("websocket disconnected; reconnecting", "exchange", name, "err", err,
			"attempt", attempt, "backoff", delay.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return ctx.Err()
}

// session runs a single connection. It reports whether any data frame arrived
// (used to reset the backoff) and the error that ended it.
func (f *Feed) session(ctx context.Context, disconnectedAtNs int64) (bool, error) {
	name := f.Adapter.Name()
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx, f.Adapter.URL(), nil) //nolint:bodyclose // the library closes it
	cancel()
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(4 << 20)

	msgs, err := f.Adapter.SubscribeMessages(f.Symbols)
	if err != nil {
		return false, err
	}
	for _, m := range msgs {
		if err := conn.Write(ctx, websocket.MessageText, m); err != nil {
			return false, fmt.Errorf("subscribe: %w", err)
		}
	}
	f.Log.Info("websocket connected", "exchange", name, "url", f.Adapter.URL(), "symbols", f.Symbols)

	gotData := false
	for {
		readCtx, cancel := context.WithTimeout(ctx, f.ReadTimeout)
		_, payload, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("no frame for %s: %w", f.ReadTimeout, err)
			}
			return gotData, err
		}
		recv := f.now().UnixNano()
		f.lastMsgNs.Store(recv)
		if f.Recorder != nil {
			f.Recorder(payload)
		}
		fr, err := f.Adapter.Inspect(payload)
		if err != nil {
			f.Log.Warn("uninspectable frame", "exchange", name, "err", err)
			continue
		}
		switch fr.Kind {
		case exchange.FrameError:
			return gotData, fmt.Errorf("exchange error: %s", fr.Err)
		case exchange.FrameHeartbeat, exchange.FrameControl:
			if !gotData && fr.Kind == exchange.FrameHeartbeat {
				gotData = true
				f.onConnected(ctx, disconnectedAtNs, recv)
			}
			continue
		case exchange.FrameTrade:
			if !gotData {
				gotData = true
				f.onConnected(ctx, disconnectedAtNs, recv)
			}
			f.publishRaw(ctx, fr.Symbol, payload, recv)
		default:
			f.Log.Debug("ignoring frame", "exchange", name, "kind", fr.Kind.String())
		}
	}
}

func (f *Feed) onConnected(ctx context.Context, disconnectedAtNs, nowNs int64) {
	name := f.Adapter.Name()
	f.connected.Store(true)
	observability.FeedConnected.WithLabelValues(name).Set(1)
	if disconnectedAtNs == 0 {
		return
	}
	gap := time.Duration(nowNs - disconnectedAtNs)
	observability.FeedGapSeconds.WithLabelValues(name).Observe(gap.Seconds())
	f.Log.Warn("feed gap after reconnect", "exchange", name, "gap", gap.String())
	if gap < f.GapThreshold || f.AlertsTopic == "" {
		return
	}
	for _, sym := range f.Symbols {
		a := &domain.Alert{
			AlertId:     domain.AlertID("feed_gap", sym, nowNs, name),
			RuleName:    "feed_gap",
			Symbol:      sym,
			Severity:    domain.SeverityWarn,
			WindowEndNs: nowNs,
			Exchange:    name,
			Message:     fmt.Sprintf("%s feed for %s was disconnected for %.1fs", name, sym, gap.Seconds()),
			Values:      map[string]float64{"gap_secs": gap.Seconds()},
		}
		if r, err := kafkax.ProtoRecord(f.AlertsTopic, sym, a); err == nil {
			f.Publish(ctx, r)
		}
	}
}

func (f *Feed) publishRaw(ctx context.Context, symbol string, payload []byte, recvNs int64) {
	name := f.Adapter.Name()
	r := &kgo.Record{Topic: f.RawTopic, Key: []byte(symbol), Value: payload}
	kafkax.SetHeader(r, kafkax.HeaderRecvTimeNs, strconv.FormatInt(recvNs, 10))
	kafkax.SetHeader(r, kafkax.HeaderExchange, name)
	if f.Tracer != nil {
		spanCtx, span := f.Tracer.Start(ctx, "ingest "+name, trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attribute.String("exchange", name), attribute.String("symbol", symbol)))
		kafkax.Inject(spanCtx, r)
		span.End()
	}
	f.Publish(ctx, r)
	f.mu.Lock()
	f.lastTrade[symbol] = recvNs
	f.mu.Unlock()
	observability.TradesReceived.WithLabelValues(name, symbol).Inc()
}

// Health returns a feed-health snapshot for every configured symbol.
func (f *Feed) Health() []*domain.FeedHealth {
	now := f.now().UnixNano()
	name := f.Adapter.Name()
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*domain.FeedHealth, 0, len(f.Symbols))
	for _, sym := range f.Symbols {
		last := f.lastTrade[sym]
		status := domain.FeedStatus_FEED_STATUS_CONNECTED
		switch {
		case !f.connected.Load():
			status = domain.FeedStatus_FEED_STATUS_RECONNECTING
		case last == 0 || time.Duration(now-last) > f.StaleAfter:
			status = domain.FeedStatus_FEED_STATUS_STALE
		}
		out = append(out, &domain.FeedHealth{
			Exchange: name, Symbol: sym, LastTradeNs: last,
			Reconnects: f.reconnects.Load(), Status: status, ReportedAtNs: now,
		})
	}
	if last := f.lastMsgNs.Load(); last > 0 {
		observability.FeedLastMessageAge.WithLabelValues(name).Set(time.Duration(now - last).Seconds())
	}
	return out
}

// ReportHealth publishes Health() every interval until ctx is done.
func (f *Feed) ReportHealth(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, h := range f.Health() {
				if r, err := kafkax.ProtoRecord(f.HealthTopic, h.Symbol, h); err == nil {
					f.Publish(ctx, r)
				}
			}
		}
	}
}

// Backoff is exponential backoff with "full jitter": wait a random time
// between 0 and min(Max, Base*2^attempt). The randomness stops many clients
// reconnecting at the same moment after an outage.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
	Rand func() float64 // for tests; defaults to math/rand
}

// Delay is how long to wait before reconnect attempt number attempt (0-based).
func (b Backoff) Delay(attempt int) time.Duration {
	r := b.Rand
	if r == nil {
		r = rand.Float64
	}
	return time.Duration(r() * float64(b.Ceiling(attempt)))
}

// Ceiling is the longest possible delay for attempt.
func (b Backoff) Ceiling(attempt int) time.Duration {
	attempt = min(max(attempt, 0), 30)
	c := b.Base << attempt
	if c <= 0 || c > b.Max {
		c = b.Max
	}
	return c
}

// FileRecorder appends raw messages to <dir>/<exchange>.jsonl, one per line.
// It is used to capture real payloads for test fixtures (-record flag).
type FileRecorder struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// NewFileRecorder opens the recording file for appending.
func NewFileRecorder(dir, exchange string) (*FileRecorder, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, exchange+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileRecorder{f: f, w: bufio.NewWriter(f)}, nil
}

func (r *FileRecorder) Write(payload []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.w.Write(payload)
	_ = r.w.WriteByte('\n')
}

func (r *FileRecorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.w.Flush()
	return r.f.Close()
}
