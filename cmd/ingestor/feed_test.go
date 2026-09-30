package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/exchange"
	"github.com/anibitri/tickstream/internal/kafkax"
)

func TestBackoffFullJitter(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 2 * time.Second, Rand: func() float64 { return 0.999 }}
	assert.Equal(t, 100*time.Millisecond, b.Ceiling(0))
	assert.Equal(t, 800*time.Millisecond, b.Ceiling(3))
	assert.Equal(t, 2*time.Second, b.Ceiling(10), "capped at Max")
	assert.Equal(t, 2*time.Second, b.Ceiling(1000), "no overflow on huge attempts")
	assert.Less(t, b.Delay(3), 800*time.Millisecond)
	b.Rand = func() float64 { return 0 }
	assert.Equal(t, time.Duration(0), b.Delay(5))
}

// fakeExchange serves the Coinbase protocol; each connection sends the given
// frames and then either holds the socket open or drops it.
type fakeExchange struct {
	conns  atomic.Int32
	frames func(conn int) (frames []string, drop bool)
}

func (fx *fakeExchange) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	n := int(fx.conns.Add(1))
	ctx := r.Context()
	if _, _, err := c.Read(ctx); err != nil { // subscribe message
		return
	}
	frames, drop := fx.frames(n)
	for _, f := range frames {
		if err := c.Write(ctx, websocket.MessageText, []byte(f)); err != nil {
			return
		}
	}
	if drop {
		return
	}
	<-ctx.Done()
}

type capture struct {
	mu   sync.Mutex
	recs []*kgo.Record
}

func (c *capture) publish(_ context.Context, r *kgo.Record) {
	c.mu.Lock()
	c.recs = append(c.recs, r)
	c.mu.Unlock()
}

func (c *capture) byTopic(topic string) []*kgo.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*kgo.Record
	for _, r := range c.recs {
		if r.Topic == topic {
			out = append(out, r)
		}
	}
	return out
}

const matchFrame = `{"type":"match","trade_id":%ID%,"side":"buy","size":"0.1","price":"100.5","product_id":"BTC-USD","time":"2026-09-30T01:34:18.832879Z"}`

func TestFeedPublishesTradesAndReconnectsWithGapAlert(t *testing.T) {
	fx := &fakeExchange{frames: func(n int) ([]string, bool) {
		id := "1"
		if n > 1 {
			id = "2"
		}
		frames := []string{`{"type":"subscriptions","channels":[]}`, strings.Replace(matchFrame, "%ID%", id, 1)}
		return frames, n == 1 // first connection drops after sending
	}}
	srv := httptest.NewServer(fx)
	defer srv.Close()

	var clock atomic.Int64
	clock.Store(time.Date(2026, 9, 30, 1, 34, 19, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Add(int64(3*time.Second))) } // each call advances 3s

	cp := &capture{}
	f := &Feed{
		Adapter:      &exchange.Coinbase{Endpoint: "ws" + strings.TrimPrefix(srv.URL, "http")},
		Symbols:      []string{"BTC-USD"},
		RawTopic:     "raw.trades.coinbase",
		AlertsTopic:  "risk.alerts",
		HealthTopic:  "md.feed_health",
		Publish:      cp.publish,
		Backoff:      Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond},
		ReadTimeout:  2 * time.Second,
		GapThreshold: time.Second,
		StaleAfter:   time.Hour,
		Log:          slog.New(slog.DiscardHandler),
		Now:          now,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()

	require.Eventually(t, func() bool { return len(cp.byTopic("raw.trades.coinbase")) == 2 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	<-done

	raws := cp.byTopic("raw.trades.coinbase")
	assert.Equal(t, "BTC-USD", string(raws[0].Key))
	assert.Contains(t, string(raws[0].Value), `"trade_id":1`, "payload forwarded untouched")
	assert.NotZero(t, kafkax.HeaderInt64(raws[0], kafkax.HeaderRecvTimeNs))
	assert.Equal(t, "coinbase", kafkax.Header(raws[0], kafkax.HeaderExchange))
	assert.GreaterOrEqual(t, f.reconnects.Load(), int64(1))

	alerts := cp.byTopic("risk.alerts")
	require.Len(t, alerts, 1, "one feed_gap alert per symbol after the reconnect")
	var a domain.Alert
	require.NoError(t, proto.Unmarshal(alerts[0].Value, &a))
	assert.Equal(t, "feed_gap", a.RuleName)
	assert.Equal(t, "coinbase", a.Exchange)
	assert.Equal(t, domain.AlertID("feed_gap", "BTC-USD", a.WindowEndNs, "coinbase"), a.AlertId)
	assert.Greater(t, a.Values["gap_secs"], 1.0)

	h := f.Health()
	require.Len(t, h, 1)
	assert.Equal(t, int64(f.reconnects.Load()), h[0].Reconnects)
	assert.NotZero(t, h[0].LastTradeNs)
}

func TestFeedReconnectsOnExchangeError(t *testing.T) {
	fx := &fakeExchange{frames: func(n int) ([]string, bool) {
		if n == 1 {
			return []string{`{"type":"error","message":"Failed to subscribe","reason":"bad product"}`}, false
		}
		return []string{strings.Replace(matchFrame, "%ID%", "9", 1)}, false
	}}
	srv := httptest.NewServer(fx)
	defer srv.Close()
	cp := &capture{}
	f := &Feed{
		Adapter: &exchange.Coinbase{Endpoint: "ws" + strings.TrimPrefix(srv.URL, "http")},
		Symbols: []string{"BTC-USD"}, RawTopic: "raw", Publish: cp.publish,
		Backoff: Backoff{Base: time.Millisecond, Max: time.Millisecond}, ReadTimeout: time.Second,
		GapThreshold: time.Hour, StaleAfter: time.Hour, Log: slog.New(slog.DiscardHandler),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.Run(ctx) }()
	require.Eventually(t, func() bool { return len(cp.byTopic("raw")) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.GreaterOrEqual(t, fx.conns.Load(), int32(2))
}
