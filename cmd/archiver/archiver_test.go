package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/storage"
)

var hour13 = time.Date(2026, 10, 4, 13, 0, 0, 0, time.UTC).UnixNano()

func rec(t *testing.T, partition int32, offset int64, exch, sym, id string, atNs int64) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(&domain.Trade{Exchange: exch, Symbol: sym, TradeId: id, EventTimeNs: atNs,
		Price: "100", Size: "1", Side: domain.SideBuy})
	require.NoError(t, err)
	return &kgo.Record{Topic: "md.trades", Partition: partition, Offset: offset, Value: b}
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newArchiver(store storage.ObjectStore, c *clock) *Archiver {
	return &Archiver{Store: store, InTopic: "md.trades", MaxRows: 3, MaxAge: time.Minute, HourGrace: time.Minute,
		RetryBase: time.Millisecond, Log: slog.New(slog.DiscardHandler), Now: c.now}
}

func TestArchiverWritesFullBuffersAndCommitsLowWater(t *testing.T) {
	ctx := context.Background()
	store := &storage.DirStore{Root: t.TempDir()}
	a := newArchiver(store, &clock{time.Now()})

	_, err := a.Handle(ctx, []*kgo.Record{
		rec(t, 0, 10, "coinbase", "BTC-USD", "1", hour13+1e9),
		rec(t, 0, 11, "kraken", "BTC-USD", "a", hour13+2e9),
		rec(t, 0, 12, "coinbase", "BTC-USD", "2", hour13+3e9),
		rec(t, 0, 13, "coinbase", "BTC-USD", "3", hour13+4e9), // 3rd coinbase row: buffer full
	})
	require.NoError(t, err)

	keys, _ := store.List(ctx, "trades/")
	require.Equal(t, []string{storage.ArchiveKey("coinbase", "BTC-USD", hour13, 10, 13)}, keys)
	b, _ := store.Get(ctx, keys[0])
	rows, err := storage.DecodeTrades(b)
	require.NoError(t, err)
	assert.Len(t, rows, 3)

	// The kraken buffer (offset 11) is not written yet, so the commit stays at 11.
	assert.Equal(t, int64(11), a.CommitOffsets()["md.trades"][0].Offset)
}

func TestArchiverFlushesFinishedHoursAndOldBuffers(t *testing.T) {
	ctx := context.Background()
	store := &storage.DirStore{Root: t.TempDir()}
	c := &clock{time.Now()}
	a := newArchiver(store, c)

	_, err := a.Handle(ctx, []*kgo.Record{
		rec(t, 0, 0, "coinbase", "ETH-USD", "1", hour13+10e9),
		rec(t, 1, 0, "kraken", "SOL-USD", "x", hour13+10e9),
	})
	require.NoError(t, err)
	// A trade 1h01m later on partition 0 closes hour 13 for that partition only.
	_, err = a.Handle(ctx, []*kgo.Record{rec(t, 0, 1, "coinbase", "ETH-USD", "2", hour13+int64(61*time.Minute))})
	require.NoError(t, err)
	keys, _ := store.List(ctx, "trades/")
	assert.Equal(t, []string{storage.ArchiveKey("coinbase", "ETH-USD", hour13, 0, 0)}, keys)

	// After MaxAge, Tick writes the remaining buffers.
	c.t = c.t.Add(2 * time.Minute)
	_, err = a.Tick(ctx)
	require.NoError(t, err)
	keys, _ = store.List(ctx, "trades/")
	assert.Len(t, keys, 3)
	offs := a.CommitOffsets()["md.trades"]
	assert.Equal(t, kafkax.Commit{Offset: 2}, offs[0])
	assert.Equal(t, kafkax.Commit{Offset: 1}, offs[1])
}

type failingStore struct {
	storage.ObjectStore
	calls int
}

func (f *failingStore) Put(context.Context, string, []byte, string) error {
	f.calls++
	return errors.New("s3 unavailable")
}

func TestArchiverReturnsErrorWhenS3KeepsFailing(t *testing.T) {
	fs := &failingStore{}
	a := newArchiver(fs, &clock{time.Now()})
	a.MaxRows = 1
	_, err := a.Handle(context.Background(), []*kgo.Record{rec(t, 0, 5, "coinbase", "BTC-USD", "1", hour13)})
	require.Error(t, err)
	assert.Equal(t, 5, fs.calls, "retried before giving up")
	assert.Equal(t, int64(5), a.CommitOffsets()["md.trades"][0].Offset, "nothing past the failed batch is committed")
}

func TestArchiverRevokedDropsState(t *testing.T) {
	a := newArchiver(&storage.DirStore{Root: t.TempDir()}, &clock{time.Now()})
	_, err := a.Handle(context.Background(), []*kgo.Record{rec(t, 2, 7, "coinbase", "BTC-USD", "1", hour13)})
	require.NoError(t, err)
	a.Revoked("md.trades", []int32{2})
	assert.Empty(t, a.CommitOffsets())
}
