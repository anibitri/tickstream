package metrics

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestAlign(t *testing.T) {
	assert.Equal(t, int64(10e9), Align(10_500_000_000, 1))
	assert.Equal(t, int64(10e9), Align(19_999_999_999, 10))
	assert.Equal(t, int64(60e9), Align(60e9, 60), "boundary belongs to the next window")
	assert.Equal(t, int64(-10e9), Align(-1, 10), "negative times align downwards")
	k := WindowKey{Symbol: "BTC-USD", Secs: 10, StartNs: 20e9}
	assert.Equal(t, int64(30e9), k.EndNs())
}

func TestWatermark(t *testing.T) {
	w := Watermark{LatenessNs: 500e6}
	assert.Equal(t, int64(math.MinInt64), w.Current())
	w.Observe(10e9)
	assert.Equal(t, int64(9_500_000_000), w.Current())
	w.Observe(9e9) // out of order: max unchanged
	assert.Equal(t, int64(9_500_000_000), w.Current())
	w.AdvanceTo(20e9)
	assert.Equal(t, int64(20e9), w.Current(), "floor from checkpoint/idle advance")
	w.Observe(30e9)
	assert.Equal(t, int64(29_500_000_000), w.Current())
	w.AdvanceTo(1) // never moves backwards
	assert.Equal(t, int64(29_500_000_000), w.Current())
}

func TestCloseUpToOrdersDeterministically(t *testing.T) {
	s := newWindowSet()
	keys := []WindowKey{
		{"ETH-USD", 1, 0}, {"BTC-USD", 1, 0}, {"BTC-USD", 10, 0}, {"BTC-USD", 1, 1e9}, {"BTC-USD", 60, 0},
	}
	for _, k := range keys {
		s.getOrCreate(k, func() *Accumulator { return newAccumulator(0, 0) })
	}
	assert.Nil(t, s.closeUpTo(999_999_999), "nothing ends before 1s")
	var order []WindowKey
	for _, c := range s.closeUpTo(10e9) {
		order = append(order, c.Key)
	}
	assert.Equal(t, []WindowKey{{"BTC-USD", 1, 0}, {"ETH-USD", 1, 0}, {"BTC-USD", 1, 1e9}, {"BTC-USD", 10, 0}}, order)
	assert.Len(t, s.open, 1)
	assert.Len(t, s.closeUpTo(math.MaxInt64), 1)
}

// Property: every event lands in exactly one window per size, and windows of
// each size tile the time line without overlap.
func TestAlignmentPartitionsTime(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ts := rapid.Int64Range(-1e15, 1e15).Draw(t, "t")
		for _, secs := range []int32{1, 10, 60} {
			start := Align(ts, secs)
			end := WindowKey{Secs: secs, StartNs: start}.EndNs()
			if ts < start || ts >= end {
				t.Fatalf("t=%d not in [%d,%d) for %ds", ts, start, end, secs)
			}
			if start%(int64(secs)*1e9) != 0 {
				t.Fatalf("start %d not aligned to %ds", start, secs)
			}
		}
	})
}
