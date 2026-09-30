package storage

import (
	"context"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anibitri/tickstream/internal/domain"
)

func TestParquetRoundTripSortsIntoReplayOrder(t *testing.T) {
	base := time.Date(2026, 10, 4, 13, 5, 0, 0, time.UTC).UnixNano()
	rows := []TradeRow{
		RowFromTrade(&domain.Trade{Exchange: "kraken", Symbol: "BTC-USD", TradeId: "9", EventTimeNs: base + 2, Price: "100.5", Size: "0.1", Side: domain.SideSell}, 12),
		RowFromTrade(&domain.Trade{Exchange: "coinbase", Symbol: "BTC-USD", TradeId: "7", EventTimeNs: base + 2, Price: "100.4", Size: "0.2", Side: domain.SideBuy}, 11),
		RowFromTrade(&domain.Trade{Exchange: "kraken", Symbol: "BTC-USD", TradeId: "8", EventTimeNs: base, Price: "100.3", Size: "1", Side: domain.SideBuy}, 10),
	}
	b, err := EncodeTrades(rows)
	require.NoError(t, err)
	got, err := DecodeTrades(b)
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, []string{"8", "7", "9"}, []string{got[0].TradeID, got[1].TradeID, got[2].TradeID},
		"ordered by event time, then exchange, then trade id")
	tr := got[1].Trade()
	assert.Equal(t, "coinbase", tr.Exchange)
	assert.Equal(t, "100.4", tr.Price)
	assert.Equal(t, domain.SideBuy, tr.Side)
	assert.Equal(t, int64(11), got[1].KafkaOffset)
}

func TestArchiveKeys(t *testing.T) {
	ts := time.Date(2026, 10, 4, 13, 59, 59, 0, time.UTC).UnixNano()
	key := ArchiveKey("coinbase", "BTC-USD", ts, 100, 250)
	assert.Equal(t, "trades/exchange=coinbase/symbol=BTC-USD/date=2026-10-04/hour=13/part-00000000000000000100-00000000000000000250.parquet", key)
	f, ok := ParseArchiveKey(key)
	require.True(t, ok)
	assert.Equal(t, "coinbase", f.Exchange)
	assert.Equal(t, "BTC-USD", f.Symbol)
	assert.Equal(t, time.Date(2026, 10, 4, 13, 0, 0, 0, time.UTC), f.Hour)
	for _, bad := range []string{"trades/x.parquet", "backtests/r1/report.json",
		"trades/exchange=a/symbol=b/date=2026-13-01/hour=01/p.parquet", "trades/exchange=a/symbol=b/date=2026-10-01/hour=24/p.parquet"} {
		_, ok := ParseArchiveKey(bad)
		assert.False(t, ok, bad)
	}
}

func TestDirStore(t *testing.T) {
	ctx := context.Background()
	s := &DirStore{Root: t.TempDir()}
	require.NoError(t, s.Put(ctx, "trades/a/1.parquet", []byte("one"), ""))
	require.NoError(t, s.Put(ctx, "trades/a/1.parquet", []byte("uno"), ""), "overwrite is idempotent")
	require.NoError(t, s.Put(ctx, "backtests/r/report.json", []byte("{}"), ""))
	b, err := s.Get(ctx, "trades/a/1.parquet")
	require.NoError(t, err)
	assert.Equal(t, "uno", string(b))
	_, err = s.Get(ctx, "nope")
	assert.ErrorIs(t, err, ErrNotFound)
	keys, err := s.List(ctx, "trades/")
	require.NoError(t, err)
	assert.Equal(t, []string{"trades/a/1.parquet"}, keys)
	assert.Error(t, s.Put(ctx, "../escape", nil, ""), "keys cannot escape the root")
}

func TestItemConversions(t *testing.T) {
	a := &domain.Alert{AlertId: "abc", RuleName: "price_jump", Symbol: "BTC-USD", Severity: domain.SeverityWarn,
		WindowEndNs: time.Date(2026, 10, 4, 13, 0, 10, 0, time.UTC).UnixNano(), Values: map[string]float64{"value": 5}}
	it := AlertItemFrom(a)
	assert.Equal(t, "WARN", it.Severity)
	assert.Equal(t, "01791118810000000000#abc", it.SK)
	assert.Equal(t, time.Date(2026, 11, 3, 13, 0, 10, 0, time.UTC).Unix(), it.ExpiresAt, "30-day TTL")

	c := encodeCursor(map[string]ddbAV{"symbol": sAV("BTC-USD"), "sk": sAV("0001#x")})
	key, err := decodeCursor(c)
	require.NoError(t, err)
	assert.Equal(t, sAV("0001#x"), key["sk"])
	_, err = decodeCursor("!!!")
	assert.Error(t, err)
	assert.Equal(t, "reconnecting", StatusName("FEED_STATUS_RECONNECTING"))
}

type ddbAV = ddbtypes.AttributeValue

func sAV(s string) ddbAV { return &ddbtypes.AttributeValueMemberS{Value: s} }
