package rules

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/metrics"
)

// sample is one historical value plus the offset of the record it came from.
type sample struct {
	v      float64
	offset int64
}

// ring is a fixed-size history buffer.
type ring struct {
	buf  []sample
	next int
	full bool
}

func newRing(n int) *ring { return &ring{buf: make([]sample, n)} }

func (r *ring) push(s sample) {
	r.buf[r.next] = s
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
}

func (r *ring) values(dst []float64) []float64 {
	dst = dst[:0]
	if r.full {
		for _, s := range r.buf[r.next:] {
			dst = append(dst, s.v)
		}
	}
	for _, s := range r.buf[:r.next] {
		dst = append(dst, s.v)
	}
	return dst
}

func (r *ring) oldestOffset() (int64, bool) {
	if r.full {
		return r.buf[r.next].offset, true
	}
	if r.next > 0 {
		return r.buf[0].offset, true
	}
	return 0, false
}

type stateKey struct {
	rule   string
	symbol string
}

type ruleState struct {
	hist []*ring // one per stateful node
}

type seenAt struct {
	ns     int64
	offset int64
}

// Evaluation is reported to Observer for every rule evaluation (backtests use
// it to score thresholds without re-running the stream).
type Evaluation struct {
	Rule     *Rule
	Metrics  *domain.Metrics
	Exchange string
	Value    float64
	Defined  bool // false during warm-up or when the baseline is degenerate
	Fired    bool
}

// Engine evaluates a rule set against the metrics of one Kafka partition. Like
// the metrics engine it is a pure function of its input sequence.
type Engine struct {
	rules    []*Rule
	expected []string
	state    map[stateKey]*ruleState
	lastSeen map[string]map[string]seenAt // symbol -> exchange -> last trade
	cooldown map[string]int64             // rule|symbol|exchange -> window_end of last alert

	lastOffset  int64
	hasOffset   bool
	maxWindowNs int64

	suppressUpTo int64 // restored: alerts at or before this were already emitted
	scratch      []float64

	Observer func(Evaluation)
}

// NewEngine returns an engine for the given rule set.
func NewEngine(set *Set) *Engine {
	return &Engine{
		rules:        set.Rules,
		expected:     set.ExpectedExchanges,
		state:        make(map[stateKey]*ruleState),
		lastSeen:     make(map[string]map[string]seenAt),
		cooldown:     make(map[string]int64),
		suppressUpTo: math.MinInt64,
	}
}

// Evaluate runs every matching rule against one metrics record and returns the
// alerts that fired (after cooldown).
func (e *Engine) Evaluate(m *domain.Metrics, offset int64) []*domain.Alert {
	e.lastOffset, e.hasOffset = offset, true
	if m.WindowEndNs > e.maxWindowNs {
		e.maxWindowNs = m.WindowEndNs
	}
	e.updateLastSeen(m, offset)
	var out []*domain.Alert
	for _, r := range e.rules {
		if r.WindowSecs != m.WindowSecs {
			continue
		}
		st := e.stateFor(r, m.Symbol)
		if r.PerExch {
			for _, ex := range e.exchangesFor(m.Symbol) {
				v, info, ok := e.eval(r.Cond.Expr, st, m, ex)
				if a := e.decide(r, m, ex, v, info, ok); a != nil {
					out = append(out, a)
				}
			}
		} else {
			v, info, ok := e.eval(r.Cond.Expr, st, m, "")
			if a := e.decide(r, m, "", v, info, ok); a != nil {
				out = append(out, a)
			}
		}
		// History is updated only after evaluation, so a window's own value is
		// never part of its baseline (no lookahead).
		for _, n := range r.stateful {
			if v, ok := fieldValue(n.Field, m); ok {
				st.hist[n.slot].push(sample{v: v, offset: offset})
			}
		}
	}
	return out
}

func (e *Engine) decide(r *Rule, m *domain.Metrics, exch string, v float64, info map[string]float64, ok bool) *domain.Alert {
	fired := ok && r.Cond.Op.compare(v, r.Cond.Threshold)
	ck := r.Name + "|" + m.Symbol + "|" + exch
	if fired {
		if last, seen := e.cooldown[ck]; seen && m.WindowEndNs-last < int64(r.Cooldown) {
			fired = false
		}
	}
	if e.Observer != nil {
		e.Observer(Evaluation{Rule: r, Metrics: m, Exchange: exch, Value: v, Defined: ok, Fired: fired})
	}
	if !fired || m.WindowEndNs <= e.suppressUpTo {
		// Suppressed alerts were emitted before a restart; their cooldown
		// state was restored from the checkpoint rather than recomputed.
		return nil
	}
	e.cooldown[ck] = m.WindowEndNs
	values := map[string]float64{"value": v, "threshold": r.Cond.Threshold}
	for k, x := range info {
		values[k] = x
	}
	if lp, err := strconv.ParseFloat(m.LastPrice, 64); err == nil {
		values["last_price"] = lp
	}
	subject := m.Symbol
	if exch != "" {
		subject = exch + " " + m.Symbol
	}
	return &domain.Alert{
		AlertId:     domain.AlertID(r.Name, m.Symbol, m.WindowEndNs, exch),
		RuleName:    r.Name,
		Symbol:      m.Symbol,
		Severity:    r.Severity,
		WindowEndNs: m.WindowEndNs,
		Exchange:    exch,
		Values:      values,
		Message: fmt.Sprintf("%s %s: %s = %s %s %s (%ds window ending %s)", r.Name, subject, r.Cond.Expr,
			strconv.FormatFloat(v, 'f', 2, 64), r.Cond.Op, strconv.FormatFloat(r.Cond.Threshold, 'g', -1, 64),
			m.WindowSecs, time.Unix(0, m.WindowEndNs).UTC().Format(time.RFC3339)),
	}
}

func (e *Engine) stateFor(r *Rule, symbol string) *ruleState {
	k := stateKey{r.Name, symbol}
	st := e.state[k]
	if st == nil {
		st = &ruleState{hist: make([]*ring, len(r.stateful))}
		for i, n := range r.stateful {
			st.hist[i] = newRing(n.Lookback)
		}
		e.state[k] = st
	}
	return st
}

func (e *Engine) eval(x *Expr, st *ruleState, m *domain.Metrics, exch string) (float64, map[string]float64, bool) {
	switch x.Func {
	case "":
		if x.Field == FieldSecondsSinceLastTrade {
			seen, ok := e.lastSeen[m.Symbol][exch]
			if !ok {
				return 0, nil, false
			}
			return float64(m.WindowEndNs-seen.ns) / 1e9, nil, true
		}
		v, ok := fieldValue(x.Field, m)
		return v, nil, ok
	case "abs":
		v, info, ok := e.eval(x.Arg, st, m, exch)
		return math.Abs(v), info, ok
	case "zscore":
		cur, ok := fieldValue(x.Field, m)
		h := st.hist[x.slot]
		if !ok || !h.full {
			return 0, nil, false
		}
		e.scratch = h.values(e.scratch)
		z, mean, std, ok := metrics.ZScore(cur, e.scratch)
		return z, map[string]float64{"mean": mean, "std": std, "current": cur}, ok
	case "median_ratio":
		cur, ok := fieldValue(x.Field, m)
		h := st.hist[x.slot]
		if !ok || !h.full {
			return 0, nil, false
		}
		e.scratch = h.values(e.scratch)
		med := metrics.Median(e.scratch)
		if med <= 0 {
			return 0, nil, false
		}
		return cur / med, map[string]float64{"median": med, "current": cur}, true
	}
	return 0, nil, false
}

func fieldValue(field string, m *domain.Metrics) (float64, bool) {
	parse := func(s string) (float64, bool) {
		v, err := strconv.ParseFloat(s, 64)
		return v, err == nil
	}
	switch field {
	case "last_price":
		return parse(m.LastPrice)
	case "vwap":
		return parse(m.Vwap)
	case "volume":
		return parse(m.Volume)
	case "trade_count":
		return float64(m.TradeCount), true
	case "realised_vol":
		return m.RealisedVol, true
	case "xex_spread_bps":
		return m.XexSpreadBps, len(m.Exchanges) >= 2
	case "high":
		return parse(m.High)
	case "low":
		return parse(m.Low)
	case "range_bps":
		hi, ok1 := parse(m.High)
		lo, ok2 := parse(m.Low)
		v, ok3 := parse(m.Vwap)
		if !ok1 || !ok2 || !ok3 || v == 0 {
			return 0, false
		}
		return (hi - lo) / v * 1e4, true
	}
	return 0, false
}

func (e *Engine) updateLastSeen(m *domain.Metrics, offset int64) {
	ls := e.lastSeen[m.Symbol]
	if ls == nil {
		ls = make(map[string]seenAt)
		e.lastSeen[m.Symbol] = ls
		// Expected exchanges start their staleness clock at the first window.
		for _, ex := range e.expected {
			ls[ex] = seenAt{ns: m.WindowStartNs, offset: offset}
		}
	}
	for _, x := range m.Exchanges {
		if cur := ls[x.Exchange]; x.LastTradeNs > cur.ns {
			ls[x.Exchange] = seenAt{ns: x.LastTradeNs, offset: offset}
		}
	}
}

func (e *Engine) exchangesFor(symbol string) []string {
	ls := e.lastSeen[symbol]
	out := make([]string, 0, len(ls))
	for ex := range ls {
		out = append(out, ex)
	}
	sort.Strings(out)
	return out
}

// LowWater is the oldest offset that still contributes to rule state.
func (e *Engine) LowWater() int64 {
	if !e.hasOffset {
		return -1
	}
	low := e.lastOffset + 1
	for _, st := range e.state {
		for _, h := range st.hist {
			if o, ok := h.oldestOffset(); ok && o < low {
				low = o
			}
		}
	}
	for _, ls := range e.lastSeen {
		for _, s := range ls {
			if s.offset < low {
				low = s.offset
			}
		}
	}
	return low
}

type checkpoint struct {
	Emitted  int64            `json:"wm"`
	Cooldown map[string]int64 `json:"cd,omitempty"`
}

// Checkpoint stores the latest window evaluated and the cooldown clocks.
func (e *Engine) Checkpoint() string {
	if !e.hasOffset {
		return ""
	}
	b, _ := json.Marshal(checkpoint{Emitted: e.maxWindowNs, Cooldown: e.cooldown})
	return string(b)
}

// Restore applies a checkpoint before re-reading from the low-water offset:
// history buffers are rebuilt from the re-read records, alerts for windows
// already evaluated are suppressed, and cooldown clocks come from the checkpoint.
func (e *Engine) Restore(meta string) error {
	if meta == "" {
		return nil
	}
	var c checkpoint
	if err := json.Unmarshal([]byte(meta), &c); err != nil {
		return fmt.Errorf("restore checkpoint: %w", err)
	}
	e.suppressUpTo = c.Emitted
	for k, v := range c.Cooldown {
		e.cooldown[k] = v
	}
	return nil
}
