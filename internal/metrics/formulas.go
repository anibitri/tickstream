// Package metrics computes per-symbol rolling market metrics over event-time
// windows: VWAP, volume, trade count, realised volatility, price range and
// cross-exchange spread.
package metrics

import (
	"math"

	"github.com/shopspring/decimal"
)

// SecondsPerYear annualises volatility. Crypto trades 24/7, so a year is
// 365 × 86,400 seconds rather than the 252 × 6.5h equity convention.
const SecondsPerYear = 365 * 86_400

// VWAPScale is the number of decimal places kept when dividing Σpq by Σq.
const VWAPScale = 10

var (
	tenThousand = decimal.NewFromInt(10_000)
	two         = decimal.NewFromInt(2)
)

// VWAP = Σ(pᵢ·qᵢ) / Σqᵢ, computed in exact decimal arithmetic. Summing float64
// products accumulates rounding error across thousands of trades; decimals do not.
func VWAP(sumPQ, sumQ decimal.Decimal) decimal.Decimal {
	if sumQ.IsZero() {
		return decimal.Zero
	}
	return sumPQ.DivRound(sumQ, VWAPScale)
}

// LogReturn rᵢ = ln(pᵢ / pᵢ₋₁).
func LogReturn(prev, cur float64) float64 {
	return math.Log(cur / prev)
}

// AnnualisedVol converts a window's realised variance Σrᵢ² into annualised
// volatility: √(Σrᵢ²) × √(SecondsPerYear / windowSecs).
func AnnualisedVol(realisedVariance float64, windowSecs int32) float64 {
	if realisedVariance <= 0 || windowSecs <= 0 {
		return 0
	}
	return float64(math.Sqrt(realisedVariance)) * float64(math.Sqrt(float64(SecondsPerYear)/float64(windowSecs)))
}

// SpreadBps = 10,000 × (a − b) / mid(a, b), in exact decimal arithmetic and
// rounded to 6 decimal places of a basis point.
func SpreadBps(a, b decimal.Decimal) float64 {
	mid := a.Add(b).Div(two)
	if mid.IsZero() {
		return 0
	}
	return a.Sub(b).Mul(tenThousand).DivRound(mid, 6).InexactFloat64()
}

// ZScore = (x − mean) / std over a baseline that must NOT include x itself
// (including the current window in its own baseline is lookahead bias).
// It returns ok=false when the baseline is too short or has zero variance.
func ZScore(x float64, baseline []float64) (z float64, mean float64, std float64, ok bool) {
	n := len(baseline)
	if n < 2 {
		return 0, 0, 0, false
	}
	var sum float64
	for _, v := range baseline {
		sum += v
	}
	mean = sum / float64(n)
	var ss float64
	for _, v := range baseline {
		d := v - mean
		ss += float64(d * d)
	}
	std = math.Sqrt(ss / float64(n-1)) // sample standard deviation
	if std == 0 || math.IsNaN(std) {
		return 0, mean, std, false
	}
	return (x - mean) / std, mean, std, true
}

// Median of values (does not modify the input).
func Median(values []float64) float64 {
	n := len(values)
	if n == 0 {
		return 0
	}
	c := make([]float64, n)
	copy(c, values)
	insertionSort(c)
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}

func insertionSort(a []float64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}
