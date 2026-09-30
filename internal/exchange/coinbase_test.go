package exchange

import (
	"bufio"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anibitri/tickstream/internal/domain"
)

func TestCoinbaseParseMatchInvertsMakerSide(t *testing.T) {
	a := &Coinbase{}
	payload := []byte(`{"type":"match","trade_id":1100236008,"maker_order_id":"m","taker_order_id":"t","side":"buy","size":"0.00000011","price":"83332.25","product_id":"BTC-USD","sequence":136998241387,"time":"2026-09-30T01:34:18.832879Z"}`)

	f, err := a.Inspect(payload)
	require.NoError(t, err)
	assert.Equal(t, FrameTrade, f.Kind)
	assert.Equal(t, "BTC-USD", f.Symbol)

	trades, err := a.Parse(payload)
	require.NoError(t, err)
	require.Len(t, trades, 1)
	tr := trades[0]
	assert.Equal(t, "1100236008", tr.TradeID)
	assert.Equal(t, "83332.25", tr.Price)
	assert.Equal(t, "0.00000011", tr.Size)
	assert.Equal(t, time.Date(2026, 9, 30, 1, 34, 18, 832879000, time.UTC).UnixNano(), tr.EventTimeNs)
	// maker side "buy" => the taker (aggressor) sold.
	assert.Equal(t, domain.SideSell, tr.Side)
}

func TestCoinbaseInspectFrameKinds(t *testing.T) {
	a := &Coinbase{}
	cases := map[string]FrameKind{
		`{"type":"heartbeat","last_trade_id":1,"product_id":"BTC-USD","sequence":1,"time":"2026-09-30T01:34:19.000000Z"}`: FrameHeartbeat,
		`{"type":"subscriptions","channels":[]}`:                        FrameControl,
		`{"type":"error","message":"Failed to subscribe","reason":"x"}`: FrameError,
		`{"type":"ticker"}`: FrameUnknown,
	}
	for payload, want := range cases {
		f, err := a.Inspect([]byte(payload))
		require.NoError(t, err)
		assert.Equal(t, want, f.Kind, payload)
	}
	_, err := a.Inspect([]byte(`{not json`))
	assert.Error(t, err)
}

func TestCoinbaseParseRejectsMalformed(t *testing.T) {
	a := &Coinbase{}
	bad := []string{
		`{"type":"match","product_id":"BTC-USD","price":"1","size":"1","side":"buy","time":"2026-09-30T01:34:18Z"}`,               // no trade_id
		`{"type":"match","trade_id":1,"product_id":"BTC-USD","price":"1","size":"1","side":"buy","time":"yesterday"}`,             // bad time
		`{"type":"match","trade_id":1,"product_id":"BTC-USD","price":"1","size":"1","side":"hold","time":"2026-09-30T01:34:18Z"}`, // bad side
		`{"type":"match"`, // truncated
	}
	for _, p := range bad {
		_, err := a.Parse([]byte(p))
		assert.Error(t, err, p)
	}
	_, err := a.Parse([]byte(`{"type":"heartbeat"}`))
	assert.ErrorIs(t, err, ErrNotTrade)
}

func TestCoinbaseSubscribeMessage(t *testing.T) {
	msgs, err := (&Coinbase{}).SubscribeMessages([]string{"BTC-USD", "ETH-USD"})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.JSONEq(t, `{"type":"subscribe","product_ids":["BTC-USD","ETH-USD"],"channels":["matches","heartbeat"]}`, string(msgs[0]))
}

// TestRecordedPayloads runs every frame recorded from the live feed through
// Inspect and Parse; every trade frame must parse and validate.
func TestCoinbaseRecordedPayloads(t *testing.T) {
	f, err := os.Open("../../testdata/payloads/coinbase.jsonl")
	require.NoError(t, err)
	defer f.Close()
	a := &Coinbase{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var trades int
	for sc.Scan() {
		fr, err := a.Inspect(sc.Bytes())
		require.NoError(t, err)
		if fr.Kind != FrameTrade {
			continue
		}
		ts, err := a.Parse(sc.Bytes())
		require.NoError(t, err, sc.Text())
		for _, rt := range ts {
			_, err := ToCanonical("coinbase", rt, rt.EventTimeNs+1_000_000, nil)
			require.NoError(t, err)
			trades++
		}
	}
	assert.Greater(t, trades, 10)
}
