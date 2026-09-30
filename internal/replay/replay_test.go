package replay

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/storage"
)

func smallConfig() GenConfig {
	c := DefaultGenConfig()
	c.Duration = 3 * time.Minute
	c.Start = time.Date(2026, 10, 4, 13, 58, 30, 0, time.UTC) // crosses an hour boundary
	return c
}

func readAll(t *testing.T, r *Reader) []*domain.Trade {
	t.Helper()
	var out []*domain.Trade
	for {
		tr, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		out = append(out, tr)
	}
}

func TestGenerateAndReplayInEventOrder(t *testing.T) {
	ctx := context.Background()
	store := &storage.DirStore{Root: t.TempDir()}
	cfg := smallConfig()
	n, err := Generate(ctx, store, cfg)
	require.NoError(t, err)
	require.Greater(t, n, 1000)

	keys, _ := store.List(ctx, "trades/")
	assert.Contains(t, keys[0], "hour=13")
	assert.Contains(t, keys[len(keys)-1], "hour=14", "files split at the hour")

	r, err := Open(ctx, store, nil, cfg.Start, cfg.Start.Add(cfg.Duration))
	require.NoError(t, err)
	trades := readAll(t, r)
	require.Len(t, trades, n)
	for i := 1; i < len(trades); i++ {
		a, b := storage.RowFromTrade(trades[i-1], 0), storage.RowFromTrade(trades[i], 0)
		require.True(t, storage.LessRow(&a, &b), "strictly ordered at %d", i)
	}
	exchanges := map[string]bool{}
	for _, tr := range trades {
		exchanges[tr.Exchange] = true
		_, err := domain.ParsePositiveDecimal(tr.Price)
		require.NoError(t, err)
	}
	assert.Len(t, exchanges, 2)
}

func TestGenerateIsDeterministic(t *testing.T) {
	ctx := context.Background()
	a, b := &storage.DirStore{Root: t.TempDir()}, &storage.DirStore{Root: t.TempDir()}
	_, err := Generate(ctx, a, smallConfig())
	require.NoError(t, err)
	_, err = Generate(ctx, b, smallConfig())
	require.NoError(t, err)
	ka, _ := a.List(ctx, "")
	kb, _ := b.List(ctx, "")
	require.Equal(t, ka, kb)
	for _, k := range ka {
		x, _ := a.Get(ctx, k)
		y, _ := b.Get(ctx, k)
		require.Equal(t, x, y, k)
	}
}

func TestReaderFiltersSymbolsRangeAndDuplicates(t *testing.T) {
	ctx := context.Background()
	store := &storage.DirStore{Root: t.TempDir()}
	cfg := smallConfig()
	_, err := Generate(ctx, store, cfg)
	require.NoError(t, err)

	// Archive one file twice under another name, as a crash-and-retry with
	// different batch boundaries would.
	keys, _ := store.List(ctx, "trades/exchange=kraken/symbol=ETH-USD/")
	body, _ := store.Get(ctx, keys[0])
	f, _ := storage.ParseArchiveKey(keys[0])
	require.NoError(t, store.Put(ctx, storage.ArchiveKey("kraken", "ETH-USD", f.Hour.UnixNano(), 0, 1), body, ""))

	from, to := cfg.Start.Add(30*time.Second), cfg.Start.Add(90*time.Second)
	r, err := Open(ctx, store, []string{"ETH-USD"}, from, to)
	require.NoError(t, err)
	trades := readAll(t, r)
	require.NotEmpty(t, trades)
	assert.Greater(t, r.Dupes, 0)
	for _, tr := range trades {
		assert.Equal(t, "ETH-USD", tr.Symbol)
		assert.GreaterOrEqual(t, tr.EventTimeNs, from.UnixNano())
		assert.Less(t, tr.EventTimeNs, to.UnixNano())
	}
	seen := map[string]bool{}
	for _, tr := range trades {
		k := domain.DedupKey(tr.Exchange, tr.TradeId)
		require.False(t, seen[k], "duplicate %s", k)
		seen[k] = true
	}
}

func TestPacer(t *testing.T) {
	ctx := context.Background()
	p := &Pacer{Speed: 10}
	start := time.Now()
	require.NoError(t, p.Wait(ctx, 0))
	require.NoError(t, p.Wait(ctx, int64(500*time.Millisecond))) // 0.5s of event time at 10x = 50ms
	el := time.Since(start)
	assert.GreaterOrEqual(t, el, 45*time.Millisecond)
	assert.Less(t, el, 400*time.Millisecond)

	fast := &Pacer{}
	start = time.Now()
	require.NoError(t, fast.Wait(ctx, 0))
	require.NoError(t, fast.Wait(ctx, int64(time.Hour)))
	assert.Less(t, time.Since(start), 10*time.Millisecond, "speed 0 = no waiting")
}
