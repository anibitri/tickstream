package main

import (
	"context"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/kafkax"
)

func raw(topic, payload string, recv int64, offset int64) *kgo.Record {
	r := &kgo.Record{Topic: topic, Value: []byte(payload), Offset: offset}
	kafkax.SetHeader(r, kafkax.HeaderRecvTimeNs, strconv.FormatInt(recv, 10))
	return r
}

func newHandler() *Handler {
	return &Handler{
		RawPrefix: "raw.trades.", TradesTopic: "md.trades", DLQTopic: "dlq.normaliser",
		Allowed: map[string]bool{"BTC-USD": true, "ETH-USD": true},
		Dedup:   NewDedupSet(1000), Log: slog.New(slog.DiscardHandler),
		Now: func() time.Time { return time.Date(2026, 9, 30, 1, 35, 0, 0, time.UTC) },
	}
}

func TestHandleNormalisesDedupsAndDeadLetters(t *testing.T) {
	recv := time.Date(2026, 9, 30, 1, 34, 20, 0, time.UTC).UnixNano()
	cb := `{"type":"match","trade_id":7,"side":"sell","size":"0.50","price":"100.10","product_id":"BTC-USD","time":"2026-09-30T01:34:19.5Z"}`
	kr := `{"channel":"trade","type":"update","data":[{"symbol":"ETH/USD","side":"buy","price":2669.17,"qty":0.013,"ord_type":"market","trade_id":5,"timestamp":"2026-09-30T01:34:19.6Z"},{"symbol":"ETH/USD","side":"sell","price":2669.10,"qty":1,"ord_type":"limit","trade_id":6,"timestamp":"2026-09-30T01:34:19.7Z"}]}`
	recs := []*kgo.Record{
		raw("raw.trades.coinbase", cb, recv, 0),
		raw("raw.trades.coinbase", cb, recv, 1), // redelivered duplicate
		raw("raw.trades.kraken", kr, recv, 0),
		raw("raw.trades.coinbase", `{"type":"match","trade_id":8`, recv, 2), // truncated
		raw("raw.trades.coinbase", `{"type":"match","trade_id":9,"side":"sell","size":"1","price":"-3","product_id":"BTC-USD","time":"2026-09-30T01:34:19.5Z"}`, recv, 3),
		raw("raw.trades.kraken", `{"channel":"trade","data":[{"symbol":"DOGE/USD","side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"2026-09-30T01:34:19.6Z"}]}`, recv, 1),
	}
	out, err := newHandler().Handle(context.Background(), recs)
	require.NoError(t, err)

	var trades []*domain.Trade
	var dlq []*domain.DeadLetter
	for _, r := range out {
		switch r.Topic {
		case "md.trades":
			var tr domain.Trade
			require.NoError(t, proto.Unmarshal(r.Value, &tr))
			assert.Equal(t, tr.Symbol, string(r.Key), "keyed by symbol")
			assert.Equal(t, tr.EventTimeNs/1e6, r.Timestamp.UnixMilli(), "record timestamp is event time")
			trades = append(trades, &tr)
		case "dlq.normaliser":
			var d domain.DeadLetter
			require.NoError(t, proto.Unmarshal(r.Value, &d))
			dlq = append(dlq, &d)
		}
	}
	require.Len(t, trades, 3, "1 coinbase (duplicate dropped) + 2 kraken")
	assert.Equal(t, "coinbase", trades[0].Exchange)
	assert.Equal(t, "100.1", trades[0].Price)
	assert.Equal(t, "0.5", trades[0].Size)
	assert.Equal(t, domain.SideBuy, trades[0].Side, "coinbase maker sell => taker buy")
	assert.Equal(t, recv, trades[0].RecvTimeNs)
	assert.Equal(t, "ETH-USD", trades[1].Symbol)
	assert.Equal(t, "2669.1", trades[2].Price)

	require.Len(t, dlq, 3)
	assert.Contains(t, dlq[0].Error, "parse")
	assert.Contains(t, dlq[1].Error, "validation")
	assert.Contains(t, dlq[2].Error, "not configured")
	assert.Equal(t, int64(2), dlq[0].SourceOffset)
	assert.Equal(t, `{"type":"match","trade_id":8`, string(dlq[0].Payload), "original payload preserved")
}

func TestUnknownExchangeIsDeadLettered(t *testing.T) {
	out, err := newHandler().Handle(context.Background(), []*kgo.Record{raw("raw.trades.bitmex", `{}`, 1, 0)})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "dlq.normaliser", out[0].Topic)
}

func TestDedupSeenAndEviction(t *testing.T) {
	s := NewDedupSet(3)
	assert.False(t, s.Seen("a"))
	assert.True(t, s.Seen("a"))
	assert.False(t, s.Seen("b"))
	assert.False(t, s.Seen("c"))
	assert.False(t, s.Seen("d")) // evicts "a"
	assert.Equal(t, 3, s.Len())
	assert.False(t, s.Seen("a"), "oldest key was evicted")
	assert.True(t, s.Seen("d"))
}

// Property: within the capacity window the set behaves exactly like an
// unbounded set, and it never exceeds its capacity.
func TestDedupMatchesUnboundedWithinWindow(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(1, 50).Draw(t, "capacity")
		keys := rapid.SliceOf(rapid.IntRange(0, 30)).Draw(t, "keys")
		s := NewDedupSet(capacity)
		var inserted []string // insertion order of distinct-at-the-time keys
		for _, k := range keys {
			key := strconv.Itoa(k)
			// reference: seen iff key is among the last `capacity` inserted keys
			want := false
			start := max(0, len(inserted)-capacity)
			for _, x := range inserted[start:] {
				if x == key {
					want = true
				}
			}
			if got := s.Seen(key); got != want {
				t.Fatalf("Seen(%q) = %v, want %v", key, got, want)
			}
			if !want {
				inserted = append(inserted, key)
			}
			if s.Len() > capacity {
				t.Fatalf("len %d > capacity %d", s.Len(), capacity)
			}
		}
	})
}
