package main

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anibitri/tickstream/internal/rules"
)

func TestScoreRuleAppliesCooldown(t *testing.T) {
	set, err := rules.Parse([]byte(`rules: [{name: j, window_secs: 10, condition: "abs(vwap) > 5", severity: INFO, cooldown_secs: 30}]`))
	require.NoError(t, err)
	r := set.Rules[0]
	s := int64(time.Second)
	points := []labelled{
		{"X", 0, 10, true}, {"X", 10 * s, 10, true}, // second fire suppressed by cooldown -> FN
		{"X", 40 * s, 10, false}, // fires again after 30s -> FP
		{"X", 50 * s, 1, true},   // below threshold -> FN
		{"Y", 10 * s, 10, true},  // other symbol has its own cooldown -> TP
	}
	c := scoreRule(points, r, 5, 0, 100*s)
	c.finish()
	assert.Equal(t, Confusion{TP: 2, FP: 1, FN: 2, Precision: 0.666667, Recall: 0.5, F1: 0.571429}, c)
	assert.Equal(t, 1, scoreRule(points, r, 5, 0, 10*s).TP, "range [from, to) respected")
}

func TestSignalAndStats(t *testing.T) {
	s := int64(time.Second)
	// Flat at 100, then a spike to 101 (100 bps above the baseline): the signal
	// sells at 101 and buys back 20s later at 100.
	bars := map[string][]bar{"X": {
		{10 * s, 100, 100}, {20 * s, 100, 100}, {30 * s, 100, 101}, {40 * s, 100, 100.5}, {50 * s, 100, 100},
	}}
	p := SignalParams{Lookback: 2, HoldNs: 20 * s, CostBps: 5}
	trades := simulateSignal(bars, p, 50, 0, 100*s)
	require.Len(t, trades, 1)
	assert.Equal(t, -1, trades[0].Side)
	assert.InDelta(t, (101.0-100)/101-10e-4, trades[0].NetReturn, 1e-12)
	assert.Empty(t, simulateSignal(bars, p, 150, 0, 100*s), "deviation below k")

	st, curve := computeStats([]SignalTrade{{NetReturn: 0.01, ExitNs: 1}, {NetReturn: -0.02, ExitNs: 2}, {NetReturn: 0.01, ExitNs: 3}}, 2*86400e9)
	assert.Equal(t, 3, st.Trades)
	assert.InDelta(t, 2.0/3, st.HitRate, 1e-6)
	assert.InDelta(t, 0, st.MeanReturnBps, 1e-6)
	assert.InDelta(t, 0.02, st.MaxDrawdown, 1e-6)
	assert.InDelta(t, 1.01*0.98*1.01-1, st.TotalReturn, 1e-6)
	assert.InDelta(t, 1.5, st.TradesPerDay, 1e-9)
	require.Len(t, curve, 3)
	assert.Equal(t, 1.01, curve[0].Equity)
	assert.Zero(t, round6(math.NaN()))
}

func TestFutureMoveNeedsFullHorizon(t *testing.T) {
	s := priceSeries{ends: []int64{1, 2, 3}, prices: []float64{100, 101, 102}}
	m, ok := futureMoveBps(s, 1, 2)
	require.True(t, ok)
	assert.InDelta(t, 200, m, 1e-9)
	_, ok = futureMoveBps(s, 2, 2)
	assert.False(t, ok, "data ends before t+h")
}

const (
	fixtureFrom = "2026-10-04T13:50:00Z"
	fixtureTo   = "2026-10-04T14:10:00Z"
)

func TestParseFlagsValidation(t *testing.T) {
	for _, bad := range [][]string{
		{"-from", fixtureFrom, "-to", fixtureTo},                                               // no run id
		{"-from", fixtureTo, "-to", fixtureFrom, "-run-id", "x"},                               // reversed
		{"-from", fixtureFrom, "-to", fixtureTo, "-run-id", "../x"},                            // unsafe id
		{"-from", fixtureFrom, "-to", fixtureTo, "-run-id", "x", "-folds", "2", "-train", "2"}, // no test fold
	} {
		_, err := parseFlags(bad)
		assert.Error(t, err, bad)
	}
}
