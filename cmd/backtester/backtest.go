package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"sort"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/metrics"
	"github.com/anibitri/tickstream/internal/rules"
)

// Partitions matches the md.trades / md.metrics topics. Trades are split the
// same way Kafka splits them (by symbol), so each engine sees exactly what it
// would see in production.
const Partitions = 6

// TradeSource yields trades in event-time order (replay.Reader).
type TradeSource interface {
	Next() (*domain.Trade, error)
}

// evalPoint is one rule evaluation captured from the risk engine.
type evalPoint struct {
	rule    string
	symbol  string
	endNs   int64
	value   float64
	defined bool
}

// Run is the output of pushing a trade stream through the engines.
type Run struct {
	Trades        int
	Metrics       []*domain.Metrics // in emission order
	Alerts        []*domain.Alert
	Evals         []evalPoint
	InputSHA256   string
	MetricsSHA256 string
}

// RunPipeline feeds trades through the metrics engine and the rules engine,
// one engine pair per partition, exactly like the live services. Windows that
// are still open when the input ends are not emitted (the live system would
// only emit them when later trades arrive).
func RunPipeline(src TradeSource, mcfg metrics.Config, set *rules.Set) (*Run, error) {
	part := kgo.StickyKeyPartitioner(nil).ForTopic("md.trades")
	var (
		mEngines [Partitions]*metrics.Engine
		rEngines [Partitions]*rules.Engine
		mOffsets [Partitions]int64
		rOffsets [Partitions]int64
	)
	run := &Run{}
	for p := range Partitions {
		mEngines[p] = metrics.NewEngine(mcfg) // event clock: emitted_at = watermark
		rEngines[p] = rules.NewEngine(set)
		rEngines[p].Observer = func(e rules.Evaluation) {
			if !e.Rule.PerExch {
				run.Evals = append(run.Evals, evalPoint{e.Rule.Name, e.Metrics.Symbol, e.Metrics.WindowEndNs, e.Value, e.Defined})
			}
		}
	}
	in, out := sha256.New(), sha256.New()
	det := proto.MarshalOptions{Deterministic: true}
	for {
		t, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		b, _ := det.Marshal(t)
		in.Write(b)
		run.Trades++
		p := part.Partition(&kgo.Record{Key: []byte(t.Symbol)}, Partitions)
		emitted, _, err := mEngines[p].Add(t, mOffsets[p], trace.SpanContext{})
		if err != nil {
			return nil, err
		}
		mOffsets[p]++
		for _, e := range emitted {
			m := e.Metrics
			b, _ := det.Marshal(m)
			out.Write(b)
			run.Metrics = append(run.Metrics, m)
			run.Alerts = append(run.Alerts, rEngines[p].Evaluate(m, rOffsets[p])...)
			rOffsets[p]++
		}
	}
	run.InputSHA256 = hex.EncodeToString(in.Sum(nil))
	run.MetricsSHA256 = hex.EncodeToString(out.Sum(nil))
	return run, nil
}

// priceSeries is the last price of every 1s window of one symbol.
type priceSeries struct {
	ends   []int64
	prices []float64
}

// at returns the price of the latest window ending at or before t.
func (s priceSeries) at(t int64) (float64, bool) {
	i := sort.Search(len(s.ends), func(i int) bool { return s.ends[i] > t }) - 1
	if i < 0 {
		return 0, false
	}
	return s.prices[i], true
}

func buildPrices(ms []*domain.Metrics) map[string]priceSeries {
	out := map[string]priceSeries{}
	for _, m := range ms {
		if m.WindowSecs != 1 {
			continue
		}
		p, err := strconv.ParseFloat(m.LastPrice, 64)
		if err != nil {
			continue
		}
		s := out[m.Symbol]
		s.ends, s.prices = append(s.ends, m.WindowEndNs), append(s.prices, p)
		out[m.Symbol] = s
	}
	return out
}

// futureMoveBps is |price(t+h) − price(t)| in basis points, or false when the
// data ends before t+h. It is only used to label outcomes, never as an input.
func futureMoveBps(s priceSeries, t, h int64) (float64, bool) {
	if len(s.ends) == 0 || t+h > s.ends[len(s.ends)-1] {
		return 0, false
	}
	p0, ok0 := s.at(t)
	p1, ok1 := s.at(t + h)
	if !ok0 || !ok1 || p0 == 0 {
		return 0, false
	}
	return math.Abs(p1-p0) / p0 * 1e4, true
}

// Confusion counts alert outcomes against "true events".
type Confusion struct {
	TP        int     `json:"true_positives"`
	FP        int     `json:"false_positives"`
	FN        int     `json:"false_negatives"`
	Precision float64 `json:"precision"`
	Recall    float64 `json:"recall"`
	F1        float64 `json:"f1"`
}

func (c *Confusion) add(o Confusion) { c.TP += o.TP; c.FP += o.FP; c.FN += o.FN }

func (c *Confusion) finish() {
	c.Precision = ratio(c.TP, c.TP+c.FP)
	c.Recall = ratio(c.TP, c.TP+c.FN)
	if c.Precision+c.Recall > 0 {
		c.F1 = round6(2 * c.Precision * c.Recall / (c.Precision + c.Recall))
	}
}

func ratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return round6(float64(a) / float64(b))
}

// labelled is an evaluation point with its outcome.
type labelled struct {
	symbol string
	endNs  int64
	value  float64
	event  bool
}

// scoreRule replays the rule's decisions for one threshold, applying the same
// cooldown as the risk engine, over points with endNs in [from, to).
func scoreRule(points []labelled, r *rules.Rule, threshold float64, from, to int64) Confusion {
	var c Confusion
	lastFire := map[string]int64{}
	for _, p := range points {
		if p.endNs < from || p.endNs >= to {
			continue
		}
		fired := r.Cond.Op.Compare(p.value, threshold)
		if fired {
			if last, ok := lastFire[p.symbol]; ok && p.endNs-last < int64(r.Cooldown) {
				fired = false
			}
		}
		if fired {
			lastFire[p.symbol] = p.endNs
		}
		switch {
		case fired && p.event:
			c.TP++
		case fired:
			c.FP++
		case p.event:
			c.FN++
		}
	}
	return c
}

// SignalTrade is one round trip of the mean-reversion signal.
type SignalTrade struct {
	Symbol    string
	EntryNs   int64
	ExitNs    int64
	Side      int // +1 long, -1 short
	NetReturn float64
}

// SignalParams configures the VWAP-deviation mean-reversion signal.
type SignalParams struct {
	Lookback int     // prior 10s windows in the VWAP baseline
	HoldNs   int64   // holding period
	CostBps  float64 // cost per fill (paid twice per round trip)
}

type bar struct {
	endNs      int64
	vwap, last float64
}

func buildBars(ms []*domain.Metrics) map[string][]bar {
	out := map[string][]bar{}
	for _, m := range ms {
		if m.WindowSecs != 10 {
			continue
		}
		v, err1 := strconv.ParseFloat(m.Vwap, 64)
		l, err2 := strconv.ParseFloat(m.LastPrice, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out[m.Symbol] = append(out[m.Symbol], bar{m.WindowEndNs, v, l})
	}
	return out
}

// simulateSignal trades when the last price is at least kBps away from the
// average VWAP of the previous Lookback windows: sell when above, buy when
// below, hold for HoldNs, then exit. Only entries in [from, to) count. The
// baseline uses past windows only, and the fill is the window's closing price.
func simulateSignal(bars map[string][]bar, p SignalParams, kBps float64, from, to int64) []SignalTrade {
	var out []SignalTrade
	for _, sym := range sortedSymbols(bars) {
		bs := bars[sym]
		busyUntil := int64(math.MinInt64)
		for i := p.Lookback; i < len(bs); i++ {
			b := bs[i]
			if b.endNs < from || b.endNs >= to || b.endNs <= busyUntil {
				continue
			}
			var base float64
			for _, x := range bs[i-p.Lookback : i] {
				base += x.vwap
			}
			base /= float64(p.Lookback)
			dev := (b.last - base) / base * 1e4
			if math.Abs(dev) < kBps {
				continue
			}
			j := sort.Search(len(bs), func(j int) bool { return bs[j].endNs >= b.endNs+p.HoldNs })
			if j >= len(bs) {
				break
			}
			side := -1
			if dev < 0 {
				side = 1
			}
			ret := float64(side)*(bs[j].last-b.last)/b.last - 2*p.CostBps/1e4
			out = append(out, SignalTrade{Symbol: sym, EntryNs: b.endNs, ExitNs: bs[j].endNs, Side: side, NetReturn: ret})
			busyUntil = bs[j].endNs
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ExitNs != out[j].ExitNs {
			return out[i].ExitNs < out[j].ExitNs
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}

// Stats summarises a list of signal trades.
type Stats struct {
	Trades           int     `json:"trades"`
	HitRate          float64 `json:"hit_rate"`
	MeanReturnBps    float64 `json:"mean_return_bps"`
	StdReturnBps     float64 `json:"std_return_bps"`
	SharpePerTrade   float64 `json:"sharpe_per_trade"`
	SharpeAnnualised float64 `json:"sharpe_annualised"` // 0 when the range is under a day
	MaxDrawdown      float64 `json:"max_drawdown"`
	TotalReturn      float64 `json:"total_return"`
	TradesPerDay     float64 `json:"trades_per_day"`
}

// EquityPoint is the compounded equity (starting at 1) after a trade exit.
type EquityPoint struct {
	TimeNs int64   `json:"time_ns"`
	Equity float64 `json:"equity"`
}

func computeStats(trades []SignalTrade, spanNs int64) (Stats, []EquityPoint) {
	s := Stats{Trades: len(trades)}
	if len(trades) == 0 {
		return s, nil
	}
	var sum, wins float64
	for _, t := range trades {
		sum += t.NetReturn
		if t.NetReturn > 0 {
			wins++
		}
	}
	mean := sum / float64(len(trades))
	var ss float64
	for _, t := range trades {
		ss += (t.NetReturn - mean) * (t.NetReturn - mean)
	}
	std := 0.0
	if len(trades) > 1 {
		std = math.Sqrt(ss / float64(len(trades)-1))
	}
	equity, peak, maxDD := 1.0, 1.0, 0.0
	curve := make([]EquityPoint, 0, len(trades))
	for _, t := range trades {
		equity *= 1 + t.NetReturn
		peak = math.Max(peak, equity)
		maxDD = math.Max(maxDD, 1-equity/peak)
		curve = append(curve, EquityPoint{t.ExitNs, round6(equity)})
	}
	days := float64(spanNs) / 86400e9
	s.HitRate = round6(wins / float64(len(trades)))
	s.MeanReturnBps = round6(mean * 1e4)
	s.StdReturnBps = round6(std * 1e4)
	if std > 0 {
		s.SharpePerTrade = round6(mean / std)
		// Annualising from less than a day of data gives meaningless numbers.
		if days >= 1 {
			s.SharpeAnnualised = round6(mean / std * math.Sqrt(float64(len(trades))/days*365))
		}
	}
	s.MaxDrawdown = round6(maxDD)
	s.TotalReturn = round6(equity - 1)
	if days > 0 {
		s.TradesPerDay = round6(float64(len(trades)) / days)
	}
	return s, downsample(curve, 500)
}

func downsample(c []EquityPoint, n int) []EquityPoint {
	if len(c) <= n {
		return c
	}
	out := make([]EquityPoint, 0, n)
	for i := range n {
		out = append(out, c[i*(len(c)-1)/(n-1)])
	}
	return out
}

func round6(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	return math.Round(x*1e6) / 1e6
}

func sortedSymbols[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
