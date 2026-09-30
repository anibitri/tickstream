package exchange

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anibitri/tickstream/internal/domain"
)

func TestKrakenParsePreservesDecimalLiterals(t *testing.T) {
	a := &Kraken{}
	// qty 0.01300000 and a price that is not exactly representable as float64.
	payload := []byte(`{"channel":"trade","type":"update","data":[
		{"symbol":"ETH/USD","side":"sell","price":2669.17,"qty":0.01300000,"ord_type":"limit","trade_id":66993561,"timestamp":"2026-09-30T01:31:46.656885Z"},
		{"symbol":"ETH/USD","side":"buy","price":0.1000000000000000055511151231257827,"qty":5e-05,"ord_type":"market","trade_id":66993562,"timestamp":"2026-09-30T01:31:46.656885Z"}]}`)

	f, err := a.Inspect(payload)
	require.NoError(t, err)
	assert.Equal(t, FrameTrade, f.Kind)
	assert.Equal(t, "ETH-USD", f.Symbol)

	trades, err := a.Parse(payload)
	require.NoError(t, err)
	require.Len(t, trades, 2)
	assert.Equal(t, "2669.17", trades[0].Price)
	assert.Equal(t, "0.01300000", trades[0].Size)
	assert.Equal(t, domain.SideSell, trades[0].Side)
	assert.Equal(t, "66993561", trades[0].TradeID)
	assert.Equal(t, "0.1000000000000000055511151231257827", trades[1].Price)
	assert.Equal(t, domain.SideBuy, trades[1].Side)

	c, err := ToCanonical("kraken", trades[1], trades[1].EventTimeNs, nil)
	require.NoError(t, err)
	assert.Equal(t, "0.00005", c.Size, "scientific notation is canonicalised exactly")
	c, err = ToCanonical("kraken", trades[0], trades[0].EventTimeNs, nil)
	require.NoError(t, err)
	assert.Equal(t, "0.013", c.Size, "trailing zeros are stripped")
}

func TestKrakenInspectFrameKinds(t *testing.T) {
	a := &Kraken{}
	cases := map[string]FrameKind{
		`{"channel":"heartbeat"}`:                                                      FrameHeartbeat,
		`{"channel":"status","type":"update","data":[]}`:                               FrameControl,
		`{"method":"subscribe","success":true}`:                                        FrameControl,
		`{"method":"subscribe","success":false,"error":"Currency pair not supported"}`: FrameError,
		`{"channel":"book"}`:                                                           FrameUnknown,
	}
	for payload, want := range cases {
		f, err := a.Inspect([]byte(payload))
		require.NoError(t, err)
		assert.Equal(t, want, f.Kind, payload)
	}
}

func TestKrakenParseRejectsMalformed(t *testing.T) {
	a := &Kraken{}
	bad := []string{
		`{"channel":"trade","data":[{"symbol":"BTC/USD","side":"buy","qty":1,"trade_id":1,"timestamp":"2026-09-30T01:31:46Z"}]}`,          // no price
		`{"channel":"trade","data":[{"symbol":"BTC/USD","side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"not-a-time"}]}`,          // bad ts
		`{"channel":"trade","data":[{"symbol":"BTC/USD","side":"up","price":1,"qty":1,"trade_id":1,"timestamp":"2026-09-30T01:31:46Z"}]}`, // bad side
	}
	for _, p := range bad {
		_, err := a.Parse([]byte(p))
		assert.Error(t, err, p)
	}
	_, err := a.Parse([]byte(`{"channel":"heartbeat"}`))
	assert.ErrorIs(t, err, ErrNotTrade)
}

func TestKrakenSymbolMapping(t *testing.T) {
	assert.Equal(t, "BTC/USD", toKrakenSymbol("BTC-USD"))
	assert.Equal(t, "SOL-USD", fromKrakenSymbol("SOL/USD"))
	msgs, err := (&Kraken{}).SubscribeMessages([]string{"BTC-USD"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD"],"snapshot":true}}`, string(msgs[0]))
}

func TestKrakenRecordedPayloads(t *testing.T) {
	f, err := os.Open("../../testdata/payloads/kraken.jsonl")
	require.NoError(t, err)
	defer f.Close()
	a := &Kraken{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<22), 1<<22)
	var trades int
	for sc.Scan() {
		fr, err := a.Inspect(sc.Bytes())
		require.NoError(t, err)
		if fr.Kind != FrameTrade {
			continue
		}
		ts, err := a.Parse(sc.Bytes())
		require.NoError(t, err)
		for _, rt := range ts {
			_, err := ToCanonical("kraken", rt, rt.EventTimeNs+1_000_000, nil)
			require.NoError(t, err)
			trades++
		}
	}
	assert.Greater(t, trades, 10)
}
