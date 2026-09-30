package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/storage"
)

// Archiver saves every trade from md.trades to S3 as Parquet files.
//
// Trades are buffered per (exchange, symbol, hour). A buffer is written when it
// is full, when its hour is over, or when it gets old. Each file is named after
// the range of Kafka offsets it holds, so writing the same batch again after a
// crash overwrites the same file instead of creating a duplicate. Offsets are
// only committed once the file is safely in S3.
type Archiver struct {
	Store     storage.ObjectStore
	InTopic   string
	MaxRows   int           // flush a buffer at this many rows (deterministic batch boundaries)
	MaxAge    time.Duration // flush a buffer this long after its first row (processing time)
	HourGrace time.Duration // flush an hour this long (event time) after it ends
	RetryBase time.Duration // first retry delay for failed writes (doubles each time)
	Log       *slog.Logger
	Now       func() time.Time

	mu    sync.Mutex
	parts map[int32]*archivePartition
}

type bufferKey struct {
	exchange, symbol string
	hourStart        int64
}

type buffer struct {
	rows    []storage.TradeRow
	first   int64
	last    int64
	created time.Time
}

type archivePartition struct {
	buffers    map[bufferKey]*buffer
	maxEventNs int64
	next       int64 // last consumed offset + 1
	hasNext    bool
}

func (a *Archiver) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func (a *Archiver) part(p int32) *archivePartition {
	if a.parts == nil {
		a.parts = make(map[int32]*archivePartition)
	}
	ap := a.parts[p]
	if ap == nil {
		ap = &archivePartition{buffers: make(map[bufferKey]*buffer)}
		a.parts[p] = ap
	}
	return ap
}

// Handle buffers trades and writes any buffer that is ready. Writes happen
// before Handle returns, so the loop only commits after they succeed.
func (a *Archiver) Handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, r := range recs {
		ap := a.part(r.Partition)
		ap.next, ap.hasNext = r.Offset+1, true
		var t domain.Trade
		if err := proto.Unmarshal(r.Value, &t); err != nil {
			a.Log.Warn("skipping undecodable trade", "partition", r.Partition, "offset", r.Offset, "err", err)
			continue
		}
		hour := time.Unix(0, t.EventTimeNs).UTC().Truncate(time.Hour).UnixNano()
		k := bufferKey{t.Exchange, t.Symbol, hour}
		b := ap.buffers[k]
		if b == nil {
			b = &buffer{first: r.Offset, created: a.now()}
			ap.buffers[k] = b
		}
		b.rows = append(b.rows, storage.RowFromTrade(&t, r.Offset))
		b.last = r.Offset
		if t.EventTimeNs > ap.maxEventNs {
			ap.maxEventNs = t.EventTimeNs
		}
		if len(b.rows) >= a.MaxRows {
			if err := a.flush(ctx, ap, k); err != nil {
				return nil, err
			}
		}
	}
	return nil, a.flushDue(ctx)
}

// Tick writes buffers that are old or whose hour has passed.
func (a *Archiver) Tick(ctx context.Context) ([]*kgo.Record, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return nil, a.flushDue(ctx)
}

func (a *Archiver) flushDue(ctx context.Context) error {
	now := a.now()
	for _, ap := range a.parts {
		for k, b := range ap.buffers {
			hourDone := ap.maxEventNs >= k.hourStart+int64(time.Hour)+int64(a.HourGrace)
			if hourDone || now.Sub(b.created) >= a.MaxAge {
				if err := a.flush(ctx, ap, k); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (a *Archiver) flush(ctx context.Context, ap *archivePartition, k bufferKey) error {
	b := ap.buffers[k]
	body, err := storage.EncodeTrades(b.rows)
	if err != nil {
		return err
	}
	key := storage.ArchiveKey(k.exchange, k.symbol, k.hourStart, b.first, b.last)
	// Retry a few times. If S3 is still failing, return the error: the service
	// exits without committing and rewrites the same file after it restarts.
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if lastErr = a.Store.Put(ctx, key, body, "application/vnd.apache.parquet"); lastErr == nil {
			break
		}
		a.Log.Warn("archive write failed; retrying", "key", key, "attempt", attempt+1, "err", lastErr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(a.RetryBase << attempt):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("archive %s: %w", key, lastErr)
	}
	a.Log.Info("archived", "key", key, "rows", len(b.rows), "bytes", len(body))
	delete(ap.buffers, k)
	return nil
}

// CommitOffsets commits, per partition, the first offset that is not yet in
// S3: the start of the oldest unwritten buffer, or the next offset if all
// buffers are written.
func (a *Archiver) CommitOffsets() kafkax.Offsets {
	a.mu.Lock()
	defer a.mu.Unlock()
	offs := make(kafkax.Offsets)
	for p, ap := range a.parts {
		if !ap.hasNext {
			continue
		}
		low := ap.next
		for _, b := range ap.buffers {
			if b.first < low {
				low = b.first
			}
		}
		offs.Set(a.InTopic, p, kafkax.Commit{Offset: low})
	}
	return offs
}

// Assigned needs no work: unwritten trades are simply re-read from the commit.
func (a *Archiver) Assigned(string, map[int32]string) {}

// Revoked drops unwritten buffers. They were never committed, so whichever
// instance gets the partition next re-reads and writes them.
func (a *Archiver) Revoked(topic string, parts []int32) {
	if topic != a.InTopic {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range parts {
		delete(a.parts, p)
	}
}
