package metrics

import (
	"math"
	"sort"
)

// Windows are cut by event time (the exchange's timestamp), not by the clock
// of the machine processing them. A 10s window covers [start, start+10s) and
// starts on a multiple of 10s since the Unix epoch.
//
// The watermark answers "how far has time got?". It is the latest event time
// seen minus an allowed lateness (e.g. 500 ms), so a slightly late trade still
// makes it into its window. A window is closed and emitted once the watermark
// passes its end. Nothing here reads the wall clock, so the same trades always
// produce the same windows, whatever speed they are replayed at.

// WindowKey identifies one window of one size for one symbol.
type WindowKey struct {
	Symbol  string
	Secs    int32
	StartNs int64
}

// EndNs is the (exclusive) end of the window.
func (k WindowKey) EndNs() int64 { return k.StartNs + int64(k.Secs)*1e9 }

// Align returns the start of the secs-long window that contains tNs.
func Align(tNs int64, secs int32) int64 {
	size := int64(secs) * 1e9
	start := tNs - tNs%size
	if tNs < 0 && tNs%size != 0 {
		start -= size
	}
	return start
}

// Watermark tracks the latest event time and the watermark derived from it.
type Watermark struct {
	LatenessNs int64
	maxEvent   int64
	seen       bool
	floor      int64 // minimum value, set when restoring or when a partition is idle
	hasFloor   bool
}

// Observe records an event time.
func (w *Watermark) Observe(tNs int64) {
	if !w.seen || tNs > w.maxEvent {
		w.maxEvent, w.seen = tNs, true
	}
}

// Current returns the watermark (math.MinInt64 before any event).
func (w *Watermark) Current() int64 {
	wm := int64(math.MinInt64)
	if w.seen {
		wm = w.maxEvent - w.LatenessNs
	}
	if w.hasFloor && w.floor > wm {
		wm = w.floor
	}
	return wm
}

// AdvanceTo moves the watermark forward to at least wm. It never goes back.
func (w *Watermark) AdvanceTo(wm int64) {
	if !w.hasFloor || wm > w.floor {
		w.floor, w.hasFloor = wm, true
	}
}

// windowSet holds the open (not yet emitted) windows.
type windowSet struct {
	open      map[WindowKey]*Accumulator
	nextClose int64 // earliest end of any open window
}

func newWindowSet() *windowSet {
	return &windowSet{open: make(map[WindowKey]*Accumulator), nextClose: math.MaxInt64}
}

func (s *windowSet) getOrCreate(k WindowKey, mk func() *Accumulator) *Accumulator {
	if a, ok := s.open[k]; ok {
		return a
	}
	a := mk()
	s.open[k] = a
	s.nextClose = min(s.nextClose, k.EndNs())
	return a
}

type closedWindow struct {
	Key WindowKey
	Acc *Accumulator
}

// closeUpTo removes every window ending at or before wm. They are returned
// sorted by (end, size, symbol) so the output order is always the same.
func (s *windowSet) closeUpTo(wm int64) []closedWindow {
	if wm < s.nextClose {
		return nil
	}
	var out []closedWindow
	next := int64(math.MaxInt64)
	for k, a := range s.open {
		if e := k.EndNs(); e <= wm {
			out = append(out, closedWindow{k, a})
			delete(s.open, k)
		} else {
			next = min(next, e)
		}
	}
	s.nextClose = next
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Key, out[j].Key
		if a.EndNs() != b.EndNs() {
			return a.EndNs() < b.EndNs()
		}
		if a.Secs != b.Secs {
			return a.Secs < b.Secs
		}
		return a.Symbol < b.Symbol
	})
	return out
}
