// Package replay reads archived trades back from the Parquet archive in exact
// event-time order, so past days can be pushed through the same processing
// code as live data. The replayer and the backtester both use it.
package replay

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/storage"
)

// ListFiles returns the archive files for the given symbols whose hour
// overlaps [from, to), sorted by hour.
func ListFiles(ctx context.Context, store storage.ObjectStore, symbols []string, from, to time.Time) ([]storage.ArchiveFile, error) {
	want := map[string]bool{}
	for _, s := range symbols {
		want[s] = true
	}
	keys, err := store.List(ctx, "trades/")
	if err != nil {
		return nil, err
	}
	var files []storage.ArchiveFile
	for _, k := range keys {
		f, ok := storage.ParseArchiveKey(k)
		if !ok || (len(want) > 0 && !want[f.Symbol]) {
			continue
		}
		if f.Hour.Add(time.Hour).After(from) && f.Hour.Before(to) {
			files = append(files, f)
		}
	}
	sort.Slice(files, func(i, j int) bool {
		if !files[i].Hour.Equal(files[j].Hour) {
			return files[i].Hour.Before(files[j].Hour)
		}
		return files[i].Key < files[j].Key
	})
	return files, nil
}

// Reader merges many archive files into one stream ordered by
// (event time, exchange, trade ID), dropping duplicates.
//
// Each file is already sorted (the archiver sorts before writing), so this is
// a k-way merge: keep the next row of every file in a min-heap and always take
// the smallest. Files are opened one hour at a time, so memory stays at about
// one hour of data however long the replay is.
type Reader struct {
	store    storage.ObjectStore
	fromNs   int64
	toNs     int64
	hours    [][]storage.ArchiveFile // files grouped by hour, in order
	heap     rowHeap
	last     storage.TradeRow
	hasLast  bool
	ctx      context.Context
	Dupes    int // duplicate rows skipped (e.g. a batch archived twice)
	Filtered int // rows outside [from, to)
}

// Open prepares a Reader over [from, to).
func Open(ctx context.Context, store storage.ObjectStore, symbols []string, from, to time.Time) (*Reader, error) {
	files, err := ListFiles(ctx, store, symbols, from, to)
	if err != nil {
		return nil, err
	}
	r := &Reader{store: store, fromNs: from.UnixNano(), toNs: to.UnixNano(), ctx: ctx}
	for i, f := range files {
		if i == 0 || !f.Hour.Equal(files[i-1].Hour) {
			r.hours = append(r.hours, nil)
		}
		r.hours[len(r.hours)-1] = append(r.hours[len(r.hours)-1], f)
	}
	return r, nil
}

// Next returns the next trade, or io.EOF when done.
func (r *Reader) Next() (*domain.Trade, error) {
	for {
		if len(r.heap) == 0 {
			if len(r.hours) == 0 {
				return nil, io.EOF
			}
			if err := r.loadHour(r.hours[0]); err != nil {
				return nil, err
			}
			r.hours = r.hours[1:]
			continue
		}
		top := r.heap[0]
		row := top.row
		next, err := top.reader.Next()
		switch {
		case errors.Is(err, io.EOF):
			_ = top.reader.Close()
			heap.Pop(&r.heap)
		case err != nil:
			return nil, fmt.Errorf("read %s: %w", top.key, err)
		default:
			top.row = next
			heap.Fix(&r.heap, 0)
		}
		if r.hasLast && row.EventTimeNs == r.last.EventTimeNs && row.Exchange == r.last.Exchange && row.TradeID == r.last.TradeID {
			r.Dupes++
			continue
		}
		r.last, r.hasLast = row, true
		if row.EventTimeNs < r.fromNs || row.EventTimeNs >= r.toNs {
			r.Filtered++
			continue
		}
		return row.Trade(), nil
	}
}

func (r *Reader) loadHour(files []storage.ArchiveFile) error {
	for _, f := range files {
		b, err := r.store.Get(r.ctx, f.Key)
		if err != nil {
			return err
		}
		tr := storage.NewTradeReader(b)
		row, err := tr.Next()
		if errors.Is(err, io.EOF) {
			_ = tr.Close()
			continue
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", f.Key, err)
		}
		heap.Push(&r.heap, &cursor{key: f.Key, reader: tr, row: row})
	}
	return nil
}

type cursor struct {
	key    string
	reader *storage.TradeReader
	row    storage.TradeRow
}

type rowHeap []*cursor

func (h rowHeap) Len() int           { return len(h) }
func (h rowHeap) Less(i, j int) bool { return storage.LessRow(&h[i].row, &h[j].row) }
func (h rowHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rowHeap) Push(x any)        { *h = append(*h, x.(*cursor)) }
func (h *rowHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}

// Pacer slows a replay down to a multiple of real time. Speed 0 means "as
// fast as possible". It sleeps so that event-time gaps are divided by Speed.
type Pacer struct {
	Speed   float64
	startNs int64
	startAt time.Time
	started bool
}

// Wait blocks until the trade at eventNs is due.
func (p *Pacer) Wait(ctx context.Context, eventNs int64) error {
	if p.Speed <= 0 {
		return nil
	}
	if !p.started {
		p.startNs, p.startAt, p.started = eventNs, time.Now(), true
		return nil
	}
	due := p.startAt.Add(time.Duration(float64(eventNs-p.startNs) / p.Speed))
	if d := time.Until(due); d > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
	return nil
}
