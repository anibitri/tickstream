package metrics

import (
	"math"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestVWAPHandComputed(t *testing.T) {
	// trades: 100 x 1, 101 x 3, 99 x 0.5 -> (100 + 303 + 49.5) / 4.5 = 452.5 / 4.5 = 100.5555555556
	sumPQ := d("100").Mul(d("1")).Add(d("101").Mul(d("3"))).Add(d("99").Mul(d("0.5")))
	sumQ := d("4.5")
	assert.Equal(t, "100.5555555556", VWAP(sumPQ, sumQ).String())
	assert.True(t, VWAP(d("1"), decimal.Zero).IsZero())
}

// Floats drift when summing many small products; decimals stay exact.
func TestDecimalVWAPIsExactWhereFloatDrifts(t *testing.T) {
	var fPQ, fQ float64
	pq, q := decimal.Zero, decimal.Zero
	for i := 0; i < 100_000; i++ {
		fPQ += 0.1 * 0.3
		fQ += 0.3
		pq = pq.Add(d("0.1").Mul(d("0.3")))
		q = q.Add(d("0.3"))
	}
	assert.Equal(t, "0.1", VWAP(pq, q).String())
	assert.NotEqual(t, 0.1, fPQ/fQ, "float VWAP accumulates rounding error")
}

func TestAnnualisedVolHandComputed(t *testing.T) {
	// returns ln(101/100), ln(100/101): Σr² = 2·ln(1.01)²
	r := math.Log(101.0 / 100.0)
	rv := 2 * r * r
	want := math.Sqrt(rv) * math.Sqrt(365*86400/10.0)
	assert.InDelta(t, want, AnnualisedVol(rv, 10), 1e-12)
	// a 1 bp move in a 1s window annualises to 1e-4 * sqrt(31,536,000) ≈ 0.5616
	assert.InDelta(t, 0.56157, AnnualisedVol(1e-4*1e-4, 1), 1e-5)
	assert.Zero(t, AnnualisedVol(0, 1))
	assert.Zero(t, AnnualisedVol(1, 0))
}

func TestSpreadBps(t *testing.T) {
	// (100.10 - 100.00) / 100.05 * 10000 = 9.995002...
	assert.InDelta(t, 9.995002, SpreadBps(d("100.10"), d("100.00")), 1e-6)
	assert.InDelta(t, -9.995002, SpreadBps(d("100.00"), d("100.10")), 1e-6)
	assert.Zero(t, SpreadBps(d("100"), d("100")))
}

func TestZScoreExcludesCurrentValue(t *testing.T) {
	baseline := []float64{10, 12, 11, 13, 9}
	z, mean, std, ok := ZScore(20, baseline)
	require.True(t, ok)
	assert.InDelta(t, 11.0, mean, 1e-12)
	assert.InDelta(t, math.Sqrt(2.5), std, 1e-12) // sample std of the baseline
	assert.InDelta(t, 9/math.Sqrt(2.5), z, 1e-12)

	_, _, _, ok = ZScore(1, []float64{5})
	assert.False(t, ok, "need at least two baseline points")
	_, _, _, ok = ZScore(1, []float64{5, 5, 5})
	assert.False(t, ok, "zero variance baseline")
}

func TestMedian(t *testing.T) {
	assert.Equal(t, 3.0, Median([]float64{5, 1, 3}))
	assert.Equal(t, 2.5, Median([]float64{4, 1, 2, 3}))
	in := []float64{3, 1, 2}
	Median(in)
	assert.Equal(t, []float64{3, 1, 2}, in, "input not mutated")
	assert.Zero(t, Median(nil))
}
