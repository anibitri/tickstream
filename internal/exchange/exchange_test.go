package exchange

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anibitri/tickstream/internal/domain"
)

func TestToCanonical(t *testing.T) {
	now := time.Date(2026, 10, 4, 13, 0, 0, 0, time.UTC).UnixNano()
	good := RawTrade{Symbol: "BTC-USD", TradeID: "42", EventTimeNs: now - 1e6, Price: "60000.100", Size: "0.5", Side: domain.SideBuy}

	tr, err := ToCanonical("coinbase", good, now, map[string]bool{"BTC-USD": true})
	require.NoError(t, err)
	assert.Equal(t, "60000.1", tr.Price)
	assert.Equal(t, domain.SideBuy, tr.Side)
	assert.Equal(t, now, tr.RecvTimeNs)

	cases := map[string]func(*RawTrade){
		"bad symbol":        func(r *RawTrade) { r.Symbol = "btcusd" },
		"not configured":    func(r *RawTrade) { r.Symbol = "DOGE-USD" },
		"empty id":          func(r *RawTrade) { r.TradeID = "" },
		"zero price":        func(r *RawTrade) { r.Price = "0" },
		"negative size":     func(r *RawTrade) { r.Size = "-1" },
		"nan price":         func(r *RawTrade) { r.Price = "NaN" },
		"future timestamp":  func(r *RawTrade) { r.EventTimeNs = now + int64(time.Minute) },
		"ancient timestamp": func(r *RawTrade) { r.EventTimeNs = now - int64(48*time.Hour) },
		"no side":           func(r *RawTrade) { r.Side = domain.Side_SIDE_UNSPECIFIED },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			rt := good
			mutate(&rt)
			_, err := ToCanonical("coinbase", rt, now, map[string]bool{"BTC-USD": true})
			assert.Error(t, err)
		})
	}
}
