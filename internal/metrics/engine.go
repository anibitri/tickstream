package metrics

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/trace"

	"github.com/anibitri/tickstream/internal/domain"
)

// Config parameterises an Engine.
type Config struct {
	WindowSecs      []int32       // window sizes; the smallest defines lateness
	AllowedLateness time.Duration // watermark = max event time − lateness
}

// DefaultConfig matches the spec: 1s, 10s and 60s windows, 500 ms lateness.
func DefaultConfig() Config {
	return Config{WindowSecs: []int32{1, 10, 60}, AllowedLateness: 500 * time.Millisecond}
}

// Emitted is a closed window plus the trace context of a sampled trade in it.
type Emitted struct {
	Metrics *domain.Metrics
	Trace   trace.SpanContext
}

// Outcome describes what Add did with a trade.
type Outcome int

const (
	Accepted  Outcome = iota
	Late              // its smallest window had already closed
	Duplicate         // same (exchange, trade_id) already in an open window
	Replayed          // re-read after a restart; its windows were already emitted
)

// Engine computes metrics for the trades of one Kafka partition. It is a pure
// function of its input sequence: no wall clock is read unless Clock does so.
type Engine struct {
	sizes      []int32
	wm         Watermark
	windows    *windowSet
	seen       map[int64]map[string]struct{} // smallest-window start -> dedup keys
	lastOffset int64
	hasOffset  bool

	// Set by Restore: windows ending at or before suppressUpTo were emitted
	// before the restart, and offsets below resumeFrom are being re-read.
	suppressUpTo int64
	resumeFrom   int64
	// Suppressed counts windows rebuilt during recovery but not re-emitted.
	Suppressed int

	// Clock returns emitted_at_ns given the current watermark. Live mode uses
	// wall-clock time; replay/backtest mode uses the watermark itself so the
	// output is byte-identical across runs.
	Clock func(watermarkNs int64) int64
}

// NewEngine returns an engine for cfg.
func NewEngine(cfg Config) *Engine {
	sizes := append([]int32(nil), cfg.WindowSecs...)
	sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })
	return &Engine{
		sizes:        sizes,
		wm:           Watermark{LatenessNs: int64(cfg.AllowedLateness)},
		windows:      newWindowSet(),
		seen:         make(map[int64]map[string]struct{}),
		suppressUpTo: math.MinInt64,
		resumeFrom:   -1,
		Clock:        EventClock,
	}
}

// EventClock stamps emitted_at_ns with the watermark (deterministic).
func EventClock(wm int64) int64 { return wm }

// WallClock stamps emitted_at_ns with the current time.
func WallClock(int64) int64 { return time.Now().UnixNano() }

// Add folds a trade into every window size and returns the windows that the
// advancing watermark closed. offset is the Kafka offset (or sequence number).
func (e *Engine) Add(t *domain.Trade, offset int64, sc trace.SpanContext) ([]Emitted, Outcome, error) {
	price, err := decimal.NewFromString(t.Price)
	if err != nil {
		return nil, Accepted, fmt.Errorf("price %q: %w", t.Price, err)
	}
	size, err := decimal.NewFromString(t.Size)
	if err != nil {
		return nil, Accepted, fmt.Errorf("size %q: %w", t.Size, err)
	}
	e.lastOffset, e.hasOffset = offset, true

	smallest := e.sizes[0]
	start := Align(t.EventTimeNs, smallest)
	end := WindowKey{Secs: smallest, StartNs: start}.EndNs()
	if end <= e.wm.Current() {
		if offset < e.resumeFrom {
			return nil, Replayed, nil
		}
		return nil, Late, nil
	}
	bucket := e.seen[start]
	if bucket == nil {
		bucket = make(map[string]struct{})
		e.seen[start] = bucket
	}
	dk := domain.DedupKey(t.Exchange, t.TradeId)
	if _, dup := bucket[dk]; dup {
		return nil, Duplicate, nil
	}
	bucket[dk] = struct{}{}

	pf := price.InexactFloat64()
	wmBefore := e.wm.Current()
	for _, secs := range e.sizes {
		k := WindowKey{Symbol: t.Symbol, Secs: secs, StartNs: Align(t.EventTimeNs, secs)}
		acc := e.windows.getOrCreate(k, func() *Accumulator { return newAccumulator(offset, wmBefore) })
		acc.Add(t.Exchange, t.EventTimeNs, price, size, pf, offset, sc)
	}
	e.wm.Observe(t.EventTimeNs)
	return e.close(), Accepted, nil
}

// AdvanceWatermark raises the watermark (idle advancement in live mode) and
// returns any windows that closed as a result.
func (e *Engine) AdvanceWatermark(wmNs int64) []Emitted {
	e.wm.AdvanceTo(wmNs)
	return e.close()
}

// Watermark returns the current watermark.
func (e *Engine) Watermark() int64 { return e.wm.Current() }

// OpenWindows is the number of windows not yet emitted.
func (e *Engine) OpenWindows() int { return len(e.windows.open) }

func (e *Engine) close() []Emitted {
	wm := e.wm.Current()
	closed := e.windows.closeUpTo(wm)
	if len(closed) == 0 {
		return nil
	}
	now := e.Clock(wm)
	out := make([]Emitted, 0, len(closed))
	for _, c := range closed {
		if c.Key.EndNs() <= e.suppressUpTo {
			e.Suppressed++ // already emitted before the restart (possibly partial now)
			continue
		}
		ki := keyInfo{symbol: c.Key.Symbol, secs: c.Key.Secs, startNs: c.Key.StartNs, endNs: c.Key.EndNs()}
		out = append(out, Emitted{Metrics: c.Acc.Metrics(ki, now), Trace: c.Acc.trace})
	}
	smallest := int64(e.sizes[0]) * 1e9
	for start := range e.seen {
		if start+smallest <= wm {
			delete(e.seen, start)
		}
	}
	return out
}

// LowWater is the offset to commit: the oldest record still contributing to an
// open window, so a restart re-reads exactly the trades needed to rebuild
// open state. It returns -1 if nothing has been consumed.
func (e *Engine) LowWater() int64 {
	off, _ := e.lowWater()
	return off
}

// lowWater also returns the watermark as it was just before that record.
func (e *Engine) lowWater() (int64, int64) {
	if !e.hasOffset {
		return -1, e.wm.Current()
	}
	low, lowWM := e.lastOffset+1, e.wm.Current()
	for _, a := range e.windows.open {
		if a.minOffset < low {
			low, lowWM = a.minOffset, a.wmBefore
		}
	}
	return low, lowWM
}

type checkpoint struct {
	LowWatermark int64 `json:"wl"` // watermark just before the low-water record
	Watermark    int64 `json:"wm"` // watermark at commit time
	NextOffset   int64 `json:"hi"` // next offset at commit time
}

// Checkpoint serialises what is needed to resume exactly. It is stored in the
// metadata of the consumer-group offset commit, next to the low-water offset.
func (e *Engine) Checkpoint() string {
	if !e.hasOffset {
		return ""
	}
	_, wl := e.lowWater()
	b, _ := json.Marshal(checkpoint{LowWatermark: wl, Watermark: e.wm.Current(), NextOffset: e.lastOffset + 1})
	return string(b)
}

// Restore applies a checkpoint written by Checkpoint, before any record is
// added. Re-reading from the low-water offset with the watermark reset to its
// value at that offset reproduces the original watermark progression exactly,
// so every window still open at the checkpoint is rebuilt byte-for-byte.
// Windows that closed before the checkpoint were already emitted; they may be
// rebuilt only partially, so they are suppressed instead of re-emitted.
func (e *Engine) Restore(meta string) error {
	if meta == "" {
		return nil
	}
	var c checkpoint
	if err := json.Unmarshal([]byte(meta), &c); err != nil {
		return fmt.Errorf("restore checkpoint %q: %w", meta, err)
	}
	e.wm.AdvanceTo(c.LowWatermark)
	e.suppressUpTo = c.Watermark
	e.resumeFrom = c.NextOffset
	return nil
}

// exchangeAcc accumulates one exchange's trades inside a window.
type exchangeAcc struct {
	sumPQ, sumQ decimal.Decimal
	count       int64
	lastTradeNs int64
	prevPrice   float64 // previous trade price on this exchange, within this window
	hasPrev     bool
	sumSqRet    float64
	nRet        int
}

// Accumulator holds the running state of one symbol window.
type Accumulator struct {
	sumPQ, sumQ decimal.Decimal
	count       int64
	high, low   decimal.Decimal
	last        decimal.Decimal
	lastEventNs int64
	exchanges   map[string]*exchangeAcc
	minOffset   int64
	wmBefore    int64             // watermark just before the minOffset record was processed
	trace       trace.SpanContext // first sampled trade, for trace continuity
}

func newAccumulator(offset, wmBefore int64) *Accumulator {
	return &Accumulator{exchanges: make(map[string]*exchangeAcc, 2), minOffset: offset, wmBefore: wmBefore}
}

// Add folds one trade into the window. price and size are pre-parsed decimals.
func (a *Accumulator) Add(exch string, eventNs int64, price, size decimal.Decimal, pf float64, offset int64, sc trace.SpanContext) {
	pq := price.Mul(size)
	a.sumPQ = a.sumPQ.Add(pq)
	a.sumQ = a.sumQ.Add(size)
	if a.count == 0 || price.GreaterThan(a.high) {
		a.high = price
	}
	if a.count == 0 || price.LessThan(a.low) {
		a.low = price
	}
	// last price by event time; ties go to the later arrival.
	if a.count == 0 || eventNs >= a.lastEventNs {
		a.last = price
		a.lastEventNs = eventNs
	}
	a.count++
	if offset < a.minOffset {
		a.minOffset = offset
	}
	if !a.trace.IsValid() && sc.IsSampled() {
		a.trace = sc
	}

	e := a.exchanges[exch]
	if e == nil {
		e = &exchangeAcc{}
		a.exchanges[exch] = e
	}
	e.sumPQ = e.sumPQ.Add(pq)
	e.sumQ = e.sumQ.Add(size)
	e.count++
	if eventNs > e.lastTradeNs {
		e.lastTradeNs = eventNs
	}
	// Returns are taken between consecutive trades on the same exchange so the
	// cross-venue price difference does not masquerade as volatility.
	if e.hasPrev && e.prevPrice > 0 && pf > 0 {
		r := LogReturn(e.prevPrice, pf)
		e.sumSqRet += float64(r * r)
		e.nRet++
	}
	e.prevPrice = pf
	e.hasPrev = true
}

// Metrics renders the closed window. Realised volatility is the annualised
// square root of the mean per-exchange realised variance; the cross-exchange
// spread compares the first two exchanges in name order ("coinbase − kraken").
func (a *Accumulator) Metrics(k keyInfo, emittedAtNs int64) *domain.Metrics {
	names := make([]string, 0, len(a.exchanges))
	for n := range a.exchanges {
		names = append(names, n)
	}
	sort.Strings(names)

	m := &domain.Metrics{
		Symbol:        k.symbol,
		WindowStartNs: k.startNs,
		WindowEndNs:   k.endNs,
		WindowSecs:    k.secs,
		Vwap:          VWAP(a.sumPQ, a.sumQ).String(),
		Volume:        a.sumQ.String(),
		TradeCount:    a.count,
		High:          a.high.String(),
		Low:           a.low.String(),
		LastPrice:     a.last.String(),
		EmittedAtNs:   emittedAtNs,
		Exchanges:     make([]*domain.ExchangeStats, 0, len(names)),
	}
	var varSum float64
	var varN int
	vwaps := make([]decimal.Decimal, 0, len(names))
	for _, n := range names {
		e := a.exchanges[n]
		v := VWAP(e.sumPQ, e.sumQ)
		vwaps = append(vwaps, v)
		m.Exchanges = append(m.Exchanges, &domain.ExchangeStats{
			Exchange: n, Vwap: v.String(), Volume: e.sumQ.String(), TradeCount: e.count, LastTradeNs: e.lastTradeNs,
		})
		if e.nRet > 0 {
			varSum += e.sumSqRet
			varN++
		}
	}
	if varN > 0 {
		m.RealisedVol = AnnualisedVol(varSum/float64(varN), k.secs)
	}
	if len(vwaps) >= 2 {
		m.XexSpreadBps = SpreadBps(vwaps[0], vwaps[1])
	}
	return m
}

type keyInfo struct {
	symbol         string
	secs           int32
	startNs, endNs int64
}
