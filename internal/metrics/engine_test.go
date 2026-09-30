package metrics

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	"github.com/anibitri/tickstream/internal/domain"
)

const sec = int64(time.Second)

var t0 = time.Date(2026, 10, 4, 13, 0, 0, 0, time.UTC).UnixNano()

func trade(exch, sym, id string, atNs int64, price, size string) *domain.Trade {
	return &domain.Trade{Exchange: exch, Symbol: sym, TradeId: id, EventTimeNs: atNs, Price: price, Size: size, Side: domain.SideBuy}
}

func feed(t *testing.T, e *Engine, trades []*domain.Trade) []*domain.Metrics {
	t.Helper()
	var out []*domain.Metrics
	for i, tr := range trades {
		em, _, err := e.Add(tr, int64(i), trace.SpanContext{})
		require.NoError(t, err)
		for _, x := range em {
			out = append(out, x.Metrics)
		}
	}
	return out
}

func find(ms []*domain.Metrics, sym string, secs int32, startNs int64) *domain.Metrics {
	for _, m := range ms {
		if m.Symbol == sym && m.WindowSecs == secs && m.WindowStartNs == startNs {
			return m
		}
	}
	return nil
}

func TestEngineComputesWindowMetrics(t *testing.T) {
	e := NewEngine(Config{WindowSecs: []int32{1, 10}, AllowedLateness: 500 * time.Millisecond})
	trades := []*domain.Trade{
		trade("coinbase", "BTC-USD", "1", t0+100e6, "100.00", "1"),
		trade("kraken", "BTC-USD", "a", t0+200e6, "100.20", "2"),
		trade("coinbase", "BTC-USD", "2", t0+300e6, "101.00", "1"),
		trade("kraken", "BTC-USD", "b", t0+400e6, "99.80", "1"),
		// advance the watermark past t0+1s: 1.6s - 0.5s = 1.1s
		trade("coinbase", "BTC-USD", "3", t0+1600e6, "100.50", "1"),
	}
	out := feed(t, e, trades)
	require.Len(t, out, 1, "only the first 1s window has closed")
	m := out[0]
	assert.Equal(t, int32(1), m.WindowSecs)
	assert.Equal(t, t0, m.WindowStartNs)
	assert.Equal(t, t0+sec, m.WindowEndNs)
	assert.Equal(t, int64(4), m.TradeCount)
	assert.Equal(t, "5", m.Volume)
	// Σpq = 100 + 200.4 + 101 + 99.8 = 501.2 ; / 5
	assert.Equal(t, "100.24", m.Vwap)
	assert.Equal(t, "101", m.High)
	assert.Equal(t, "99.8", m.Low)
	assert.Equal(t, "99.8", m.LastPrice)
	require.Len(t, m.Exchanges, 2)
	assert.Equal(t, "coinbase", m.Exchanges[0].Exchange)
	assert.Equal(t, "100.5", m.Exchanges[0].Vwap)
	assert.Equal(t, "100.0666666667", m.Exchanges[1].Vwap) // (200.4 + 99.8) / 3
	assert.InDelta(t, SpreadBps(d("100.5"), d("100.0666666667")), m.XexSpreadBps, 1e-9)

	// realised vol: per-exchange returns, averaged variance
	rc := math.Log(101.0 / 100.0)
	rk := math.Log(99.8 / 100.2)
	wantVol := math.Sqrt((rc*rc+rk*rk)/2) * math.Sqrt(365*86400/1.0)
	assert.InDelta(t, wantVol, m.RealisedVol, 1e-9)
	assert.Equal(t, t0+1100e6, m.EmittedAtNs, "event clock: emitted_at = watermark")
}

func TestLateAndDuplicateTrades(t *testing.T) {
	e := NewEngine(Config{WindowSecs: []int32{1, 60}, AllowedLateness: 500 * time.Millisecond})
	_, o, _ := e.Add(trade("coinbase", "ETH-USD", "1", t0+100e6, "10", "1"), 0, trace.SpanContext{})
	assert.Equal(t, Accepted, o)
	_, o, _ = e.Add(trade("coinbase", "ETH-USD", "1", t0+100e6, "10", "1"), 1, trace.SpanContext{})
	assert.Equal(t, Duplicate, o)
	em, _, _ := e.Add(trade("coinbase", "ETH-USD", "2", t0+2*sec, "10", "1"), 2, trace.SpanContext{})
	require.Len(t, em, 1)
	// t0+0.9s belongs to the closed [t0, t0+1s) window: late, and dropped from
	// every window size so that windows of all sizes see the same trades.
	_, o, _ = e.Add(trade("coinbase", "ETH-USD", "3", t0+900e6, "999", "1"), 3, trace.SpanContext{})
	assert.Equal(t, Late, o)
	// within allowed lateness: [t0+1s, t0+2s) is still open (watermark t0+1.5s)
	_, o, _ = e.Add(trade("coinbase", "ETH-USD", "4", t0+1200e6, "11", "1"), 4, trace.SpanContext{})
	assert.Equal(t, Accepted, o)

	em = e.AdvanceWatermark(t0 + 61*sec)
	m60 := find(ms(em), "ETH-USD", 60, t0)
	require.NotNil(t, m60)
	assert.Equal(t, int64(3), m60.TradeCount, "late trade excluded, duplicate excluded")
	assert.Equal(t, "11", m60.High)
}

func ms(em []Emitted) []*domain.Metrics {
	out := make([]*domain.Metrics, len(em))
	for i := range em {
		out[i] = em[i].Metrics
	}
	return out
}

func TestSingleExchangeWindowHasNoSpread(t *testing.T) {
	e := NewEngine(Config{WindowSecs: []int32{1}, AllowedLateness: 0})
	out := feed(t, e, []*domain.Trade{
		trade("kraken", "SOL-USD", "1", t0, "150", "1"),
		trade("kraken", "SOL-USD", "2", t0+2*sec, "150", "1"),
	})
	require.Len(t, out, 1)
	assert.Zero(t, out[0].XexSpreadBps)
	assert.Zero(t, out[0].RealisedVol, "one trade has no returns")
}

func TestLowWaterAndCheckpoint(t *testing.T) {
	e := NewEngine(Config{WindowSecs: []int32{1, 10}, AllowedLateness: 0})
	assert.Equal(t, int64(-1), e.LowWater())
	assert.Empty(t, e.Checkpoint())
	feed(t, e, []*domain.Trade{
		trade("coinbase", "BTC-USD", "1", t0, "1", "1"),
		trade("coinbase", "BTC-USD", "2", t0+1500e6, "1", "1"),
	})
	// [t0,t0+1s) closed; the 10s window still holds offset 0.
	assert.Equal(t, int64(0), e.LowWater())
	assert.JSONEq(t, fmt.Sprintf(`{"wl":%d,"wm":%d,"hi":2}`, int64(math.MinInt64), t0+1500e6), e.Checkpoint())
}

// genTrades draws a messy but realistic stream: two exchanges, three symbols,
// mostly increasing event times with jitter, out-of-order and late arrivals,
// and redelivered duplicates.
func genTrades(t *rapid.T) []*domain.Trade {
	n := rapid.IntRange(1, 400).Draw(t, "n")
	syms := []string{"BTC-USD", "ETH-USD", "SOL-USD"}
	exchs := []string{"coinbase", "kraken"}
	out := make([]*domain.Trade, 0, n)
	clock := t0
	for i := 0; i < n; i++ {
		if len(out) > 0 && rapid.IntRange(0, 19).Draw(t, "dupRoll") == 0 {
			dup := proto.Clone(out[rapid.IntRange(0, len(out)-1).Draw(t, "dupIdx")]).(*domain.Trade)
			out = append(out, dup)
			continue
		}
		clock += rapid.Int64Range(0, 700e6).Draw(t, "gap")
		ts := clock - rapid.Int64Range(0, 1500e6).Draw(t, "jitter") // some arrive late
		price := decimal.NewFromInt(rapid.Int64Range(9_000, 11_000).Draw(t, "p")).Shift(-2)
		size := decimal.NewFromInt(rapid.Int64Range(1, 5_000).Draw(t, "q")).Shift(-3)
		out = append(out, trade(exchs[rapid.IntRange(0, 1).Draw(t, "ex")], syms[rapid.IntRange(0, 2).Draw(t, "sym")],
			fmt.Sprint(i), ts, price.String(), size.String()))
	}
	return out
}

func run(e *Engine, trades []*domain.Trade, from int) []*domain.Metrics {
	var out []*domain.Metrics
	for i := from; i < len(trades); i++ {
		em, _, err := e.Add(trades[i], int64(i), trace.SpanContext{})
		if err != nil {
			panic(err)
		}
		out = append(out, ms(em)...)
	}
	return append(out, ms(e.AdvanceWatermark(math.MaxInt64-1))...)
}

func cfg() Config {
	return Config{WindowSecs: []int32{1, 10, 60}, AllowedLateness: 500 * time.Millisecond}
}

// Property: VWAP always lies within [low, high].
func TestPropertyVWAPWithinRange(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		for _, m := range run(NewEngine(cfg()), genTrades(t), 0) {
			v, lo, hi := d(m.Vwap), d(m.Low), d(m.High)
			if v.LessThan(lo) || v.GreaterThan(hi) {
				t.Fatalf("vwap %s outside [%s,%s]", v, lo, hi)
			}
		}
	})
}

// Property: windows partition accepted trades exactly once — per symbol, the
// trade counts and volumes of each window size sum to the same totals.
func TestPropertyWindowsPartitionTrades(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		out := run(NewEngine(cfg()), genTrades(t), 0)
		count := map[string]map[int32]int64{}
		vol := map[string]map[int32]decimal.Decimal{}
		for _, m := range out {
			if count[m.Symbol] == nil {
				count[m.Symbol], vol[m.Symbol] = map[int32]int64{}, map[int32]decimal.Decimal{}
			}
			count[m.Symbol][m.WindowSecs] += m.TradeCount
			vol[m.Symbol][m.WindowSecs] = vol[m.Symbol][m.WindowSecs].Add(d(m.Volume))
		}
		for sym, c := range count {
			if c[1] != c[10] || c[1] != c[60] {
				t.Fatalf("%s trade counts differ across window sizes: %v", sym, c)
			}
			if !vol[sym][1].Equal(vol[sym][10]) || !vol[sym][1].Equal(vol[sym][60]) {
				t.Fatalf("%s volumes differ across window sizes: %v", sym, vol[sym])
			}
		}
	})
}

// Property: the engine is deterministic — two runs over the same input produce
// byte-identical protobuf output.
func TestPropertyDeterministic(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		trades := genTrades(t)
		a, b := run(NewEngine(cfg()), trades, 0), run(NewEngine(cfg()), trades, 0)
		if len(a) != len(b) {
			t.Fatalf("len %d != %d", len(a), len(b))
		}
		for i := range a {
			ba, _ := proto.MarshalOptions{Deterministic: true}.Marshal(a[i])
			bb, _ := proto.MarshalOptions{Deterministic: true}.Marshal(b[i])
			if string(ba) != string(bb) {
				t.Fatalf("output %d differs", i)
			}
		}
	})
}

// Property: crash recovery is exact. Process a prefix, "commit" (low-water
// offset + checkpoint), throw the engine away, restore a fresh one from the
// checkpoint and re-read from the low-water offset. The restored engine must
// emit exactly the windows the uninterrupted run emitted after the commit
// point — no gaps, no partial windows, no duplicates.
func TestPropertyRestartFromCheckpointIsExact(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		trades := genTrades(t)
		full := NewEngine(cfg())
		commitAt := rapid.IntRange(0, len(trades)).Draw(t, "commitAt")
		var before, after []*domain.Metrics
		var low int64
		var meta string
		for i, tr := range trades {
			if i == commitAt {
				low, meta = full.LowWater(), full.Checkpoint()
			}
			em, _, _ := full.Add(tr, int64(i), trace.SpanContext{})
			if i < commitAt {
				before = append(before, ms(em)...)
			} else {
				after = append(after, ms(em)...)
			}
		}
		if commitAt == len(trades) {
			low, meta = full.LowWater(), full.Checkpoint()
		}
		after = append(after, ms(full.AdvanceWatermark(math.MaxInt64-1))...)

		restored := NewEngine(cfg())
		if err := restored.Restore(meta); err != nil {
			t.Fatal(err)
		}
		if low < 0 {
			low = 0
		}
		got := run(restored, trades, int(low))
		if len(got) != len(after) {
			t.Fatalf("restored run emitted %d windows, want %d (before=%d, low=%d, commitAt=%d)",
				len(got), len(after), len(before), low, commitAt)
		}
		for i := range got {
			if !proto.Equal(got[i], after[i]) {
				t.Fatalf("window %d differs after restore:\n got %v\nwant %v", i, got[i], after[i])
			}
		}
	})
}
