package rules

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	"github.com/anibitri/tickstream/internal/domain"
)

const specRules = `
expected_exchanges: [coinbase, kraken]
rules:
  - name: price_jump
    window_secs: 10
    condition: abs(zscore(last_price, lookback=5)) > 4
    severity: WARN
    cooldown_secs: 60
  - name: volume_spike
    window_secs: 10
    condition: median_ratio(volume, lookback=5) > 5
    severity: WARN
    cooldown_secs: 0
  - name: cross_exchange_divergence
    window_secs: 1
    condition: abs(xex_spread_bps) > 25
    severity: CRITICAL
    cooldown_secs: 30
  - name: stale_feed
    condition: seconds_since_last_trade > 15
    severity: CRITICAL
    cooldown_secs: 60
`

func TestParseCondition(t *testing.T) {
	ok := map[string]string{
		"abs(zscore(last_price, lookback=30)) > 4": "abs(zscore(last_price, lookback=30)) > 4",
		"median_ratio(volume,lookback=60)>=5":      "median_ratio(volume, lookback=60) >= 5",
		"abs(xex_spread_bps) > 25":                 "abs(xex_spread_bps) > 25",
		"seconds_since_last_trade > 15":            "seconds_since_last_trade > 15",
		"zscore(vwap) < -3.5":                      "zscore(vwap, lookback=30) < -3.5",
	}
	for in, want := range ok {
		c, err := ParseCondition(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, c.String())
	}
	bad := []string{
		"", "last_price", "last_price > ", "foo > 1", "zscore(foo) > 1", "zscore(last_price, window=3) > 1",
		"zscore(last_price, lookback=1) > 1", "abs(last_price > 1", "last_price > 1 extra", "sqrt(vwap) > 1",
		"zscore(seconds_since_last_trade) > 1", "last_price == 1",
	}
	for _, in := range bad {
		_, err := ParseCondition(in)
		assert.Error(t, err, in)
	}
}

func TestParseRuleFile(t *testing.T) {
	set, err := Parse([]byte(specRules))
	require.NoError(t, err)
	require.Len(t, set.Rules, 4)
	assert.Equal(t, int32(1), set.Rules[3].WindowSecs, "window defaults to 1s")
	assert.True(t, set.Rules[3].PerExch)
	assert.Equal(t, domain.SeverityCritical, set.Rules[2].Severity)
	assert.Equal(t, 60*time.Second, set.Rules[0].Cooldown)

	for _, doc := range []string{
		"rules: []",
		"rules:\n  - name: a\n    condition: vwap > 1\n    severity: LOUD",
		"rules:\n  - name: a\n    condition: vwap > 1\n    severity: INFO\n  - name: a\n    condition: vwap > 1\n    severity: INFO",
		"rules:\n  - name: a\n    condition: vwap > 1\n    severity: INFO\n    typo_field: 1",
	} {
		_, err := Parse([]byte(doc))
		assert.Error(t, err, doc)
	}
}

var t0 = time.Date(2026, 10, 4, 13, 0, 0, 0, time.UTC).UnixNano()

func m10(i int, price, volume string) *domain.Metrics {
	end := t0 + int64(i+1)*10e9
	return &domain.Metrics{Symbol: "BTC-USD", WindowSecs: 10, WindowStartNs: end - 10e9, WindowEndNs: end,
		LastPrice: price, Vwap: price, Volume: volume, High: price, Low: price, TradeCount: 5,
		Exchanges: []*domain.ExchangeStats{{Exchange: "coinbase", LastTradeNs: end - 1}, {Exchange: "kraken", LastTradeNs: end - 1}}}
}

func TestPriceJumpUsesPriorWindowsOnlyAndRespectsCooldown(t *testing.T) {
	set, _ := Parse([]byte(specRules))
	e := NewEngine(set)
	prices := []string{"100", "101", "99", "100", "101"} // baseline mean 100.2, std ~0.84
	for i, p := range prices {
		assert.Empty(t, e.Evaluate(m10(i, p, "1"), int64(i)), "warm-up: baseline not full")
	}
	alerts := e.Evaluate(m10(5, "110", "1"), 5)
	require.Len(t, alerts, 1)
	a := alerts[0]
	assert.Equal(t, "price_jump", a.RuleName)
	assert.Equal(t, domain.AlertID("price_jump", "BTC-USD", a.WindowEndNs, ""), a.AlertId)
	assert.InDelta(t, 100.2, a.Values["mean"], 1e-9, "mean excludes the jump window itself")
	assert.Greater(t, a.Values["value"], 4.0)
	assert.Contains(t, a.Message, "price_jump BTC-USD")

	// Still anomalous 10s later, but inside the 60s cooldown.
	assert.Empty(t, e.Evaluate(m10(6, "130", "1"), 6))
}

func TestVolumeSpike(t *testing.T) {
	set, _ := Parse([]byte(specRules))
	e := NewEngine(set)
	for i, v := range []string{"1", "2", "1", "3", "1"} {
		e.Evaluate(m10(i, "100", v), int64(i))
	}
	alerts := e.Evaluate(m10(5, "100", "6"), 5) // median 1 -> ratio 6 > 5
	require.Len(t, alerts, 1)
	assert.Equal(t, "volume_spike", alerts[0].RuleName)
	assert.Equal(t, 1.0, alerts[0].Values["median"])
}

func TestDivergenceAndStaleFeed(t *testing.T) {
	set, _ := Parse([]byte(specRules))
	e := NewEngine(set)
	m1 := func(i int, spread float64, exchanges ...string) *domain.Metrics {
		end := t0 + int64(i+1)*1e9
		m := &domain.Metrics{Symbol: "ETH-USD", WindowSecs: 1, WindowStartNs: end - 1e9, WindowEndNs: end,
			LastPrice: "2000", Vwap: "2000", Volume: "1", High: "2000", Low: "2000", XexSpreadBps: spread}
		for _, ex := range exchanges {
			m.Exchanges = append(m.Exchanges, &domain.ExchangeStats{Exchange: ex, LastTradeNs: end - 1})
		}
		return m
	}
	alerts := e.Evaluate(m1(0, -30, "coinbase", "kraken"), 0)
	require.Len(t, alerts, 1)
	assert.Equal(t, "cross_exchange_divergence", alerts[0].RuleName)
	assert.Empty(t, e.Evaluate(m1(1, 40, "coinbase", "kraken"), 1), "cooldown 30s")

	// Kraken goes quiet; only coinbase trades keep windows closing.
	var stale []*domain.Alert
	for i := 2; i < 25; i++ {
		stale = append(stale, e.Evaluate(m1(i, 0, "coinbase"), int64(i))...)
	}
	require.Len(t, stale, 1, "one stale alert, then cooldown")
	assert.Equal(t, "stale_feed", stale[0].RuleName)
	assert.Equal(t, "kraken", stale[0].Exchange)
	assert.Greater(t, stale[0].Values["value"], 15.0)
}

func TestSpreadRuleNeedsTwoExchanges(t *testing.T) {
	set, _ := Parse([]byte(`rules: [{name: d, window_secs: 1, condition: "abs(xex_spread_bps) > 1", severity: INFO}]`))
	e := NewEngine(set)
	m := &domain.Metrics{Symbol: "X-USD", WindowSecs: 1, WindowEndNs: t0, XexSpreadBps: 99,
		Exchanges: []*domain.ExchangeStats{{Exchange: "kraken"}}}
	assert.Empty(t, e.Evaluate(m, 0))
}

func genMetrics(t *rapid.T) []*domain.Metrics {
	n := rapid.IntRange(1, 300).Draw(t, "n")
	out := make([]*domain.Metrics, 0, n)
	for i := 0; i < n; i++ {
		sym := []string{"BTC-USD", "ETH-USD"}[rapid.IntRange(0, 1).Draw(t, "sym")]
		secs := []int32{1, 10}[rapid.IntRange(0, 1).Draw(t, "secs")]
		end := t0 + int64(i+1)*1e9
		price := 100 + rapid.Float64Range(-5, 5).Draw(t, "p")
		if rapid.IntRange(0, 15).Draw(t, "jump") == 0 {
			price += 50
		}
		m := &domain.Metrics{Symbol: sym, WindowSecs: secs, WindowStartNs: end - int64(secs)*1e9, WindowEndNs: end,
			LastPrice: fmt.Sprintf("%.2f", price), Vwap: fmt.Sprintf("%.2f", price), High: "200", Low: "1",
			Volume:       fmt.Sprintf("%.3f", rapid.Float64Range(0.001, 20).Draw(t, "v")),
			XexSpreadBps: rapid.Float64Range(-40, 40).Draw(t, "spread")}
		for _, ex := range []string{"coinbase", "kraken"} {
			if rapid.IntRange(0, 9).Draw(t, "present") > 0 {
				m.Exchanges = append(m.Exchanges, &domain.ExchangeStats{Exchange: ex, LastTradeNs: end - 1})
			}
		}
		out = append(out, m)
	}
	return out
}

// Property: restoring from a checkpoint and re-reading from the low-water
// offset emits exactly the alerts the uninterrupted run emitted afterwards.
func TestPropertyRestartFromCheckpointIsExact(t *testing.T) {
	set, err := Parse([]byte(specRules))
	require.NoError(t, err)
	rapid.Check(t, func(t *rapid.T) {
		ms := genMetrics(t)
		commitAt := rapid.IntRange(0, len(ms)).Draw(t, "commitAt")
		full := NewEngine(set)
		var after []*domain.Alert
		low, meta := int64(-1), ""
		for i, m := range ms {
			if i == commitAt {
				low, meta = full.LowWater(), full.Checkpoint()
			}
			a := full.Evaluate(m, int64(i))
			if i >= commitAt {
				after = append(after, a...)
			}
		}
		if commitAt == len(ms) {
			low, meta = full.LowWater(), full.Checkpoint()
		}
		restored := NewEngine(set)
		if err := restored.Restore(meta); err != nil {
			t.Fatal(err)
		}
		var got []*domain.Alert
		for i := int(math.Max(0, float64(low))); i < len(ms); i++ {
			got = append(got, restored.Evaluate(ms[i], int64(i))...)
		}
		if len(got) != len(after) {
			t.Fatalf("got %d alerts after restore, want %d", len(got), len(after))
		}
		for i := range got {
			if !proto.Equal(got[i], after[i]) {
				t.Fatalf("alert %d differs:\n got %v\nwant %v", i, got[i], after[i])
			}
		}
	})
}

// The shipped rules file must always load.
func TestShippedRulesFileLoads(t *testing.T) {
	set, err := Load("../../deploy/rules.yaml")
	require.NoError(t, err)
	require.Len(t, set.Rules, 4)
}
