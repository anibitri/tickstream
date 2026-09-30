package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/anibitri/tickstream/internal/storage"
)

type fakeMetrics map[string]*storage.MetricsItem

func (f fakeMetrics) GetLatest(_ context.Context, sym string, secs int32) (*storage.MetricsItem, error) {
	if m, ok := f[historyKey(sym, secs)]; ok {
		return m, nil
	}
	return nil, storage.ErrNotFound
}

type fakeAlerts map[string][]storage.AlertItem

func (f fakeAlerts) Query(_ context.Context, sym string, since int64, limit int32, _ string) (*storage.AlertPage, error) {
	var out []storage.AlertItem
	for _, a := range f[sym] {
		if a.WindowEndNs >= since && len(out) < int(limit) {
			out = append(out, a)
		}
	}
	return &storage.AlertPage{Items: out}, nil
}

type fakeHealth []storage.FeedHealthItem

func (f fakeHealth) List(context.Context) ([]storage.FeedHealthItem, error) { return f, nil }

func newTestAPI(t *testing.T) (*API, *httptest.Server) {
	store := &storage.DirStore{Root: t.TempDir()}
	require.NoError(t, store.Put(context.Background(), "backtests/run-1/report.json", []byte(`{"run_id":"run-1"}`), ""))
	a := &API{
		Symbols: []string{"BTC-USD", "ETH-USD"},
		Metrics: fakeMetrics{"BTC-USD/10": {Symbol: "BTC-USD", WindowSecs: 10, VWAP: "60000.5"}},
		Alerts: fakeAlerts{
			"BTC-USD": {{Symbol: "BTC-USD", AlertID: "b", WindowEndNs: 300}, {Symbol: "BTC-USD", AlertID: "a", WindowEndNs: 100}},
			"ETH-USD": {{Symbol: "ETH-USD", AlertID: "e", WindowEndNs: 200}},
		},
		Health:  fakeHealth{{Exchange: "kraken", Symbol: "BTC-USD", Status: "connected"}, {Exchange: "coinbase", Symbol: "BTC-USD", Status: "stale"}},
		Archive: store,
		History: &History{},
		Hub:     NewHub(),
		Log:     slog.New(slog.DiscardHandler),
	}
	srv := httptest.NewServer(a.Router())
	t.Cleanup(srv.Close)
	return a, srv
}

func getJSON(t *testing.T, url string, v any) int {
	t.Helper()
	res, err := http.Get(url)
	require.NoError(t, err)
	defer res.Body.Close()
	if v != nil {
		require.NoError(t, json.NewDecoder(res.Body).Decode(v))
	}
	return res.StatusCode
}

func TestRESTEndpoints(t *testing.T) {
	a, srv := newTestAPI(t)

	var syms []symbolInfo
	assert.Equal(t, 200, getJSON(t, srv.URL+"/api/v1/symbols", &syms))
	require.Len(t, syms, 2)
	assert.Equal(t, "coinbase", syms[0].Feeds[0].Exchange, "feeds sorted by exchange")
	assert.NotNil(t, syms[1].Feeds, "empty list, not null")

	var m storage.MetricsItem
	assert.Equal(t, 200, getJSON(t, srv.URL+"/api/v1/metrics/BTC-USD?window=10", &m))
	assert.Equal(t, "60000.5", m.VWAP)
	assert.Equal(t, 404, getJSON(t, srv.URL+"/api/v1/metrics/ETH-USD", nil))
	assert.Equal(t, 400, getJSON(t, srv.URL+"/api/v1/metrics/BTC-USD?window=5", nil))
	assert.Equal(t, 400, getJSON(t, srv.URL+"/api/v1/metrics/DOGE-USD", nil))

	var page storage.AlertPage
	assert.Equal(t, 200, getJSON(t, srv.URL+"/api/v1/alerts?limit=2", &page))
	require.Len(t, page.Items, 2)
	assert.Equal(t, []string{"b", "e"}, []string{page.Items[0].AlertID, page.Items[1].AlertID}, "merged newest first")
	assert.Equal(t, 200, getJSON(t, srv.URL+"/api/v1/alerts?symbol=BTC-USD&since=200", &page))
	assert.Len(t, page.Items, 1)

	var runs map[string][]string
	assert.Equal(t, 200, getJSON(t, srv.URL+"/api/v1/backtests", &runs))
	assert.Equal(t, []string{"run-1"}, runs["runs"])
	var report map[string]any
	assert.Equal(t, 200, getJSON(t, srv.URL+"/api/v1/backtests/run-1", &report))
	assert.Equal(t, 404, getJSON(t, srv.URL+"/api/v1/backtests/nope", nil))
	assert.Equal(t, 400, getJSON(t, srv.URL+"/api/v1/backtests/..", nil))

	for i := range 5 {
		a.History.Add(storage.MetricsItem{Symbol: "BTC-USD", WindowSecs: 1, WindowEndNs: int64(i)})
	}
	a.History.Add(storage.MetricsItem{Symbol: "BTC-USD", WindowSecs: 1, WindowEndNs: 2}) // out of order: ignored
	var hist []storage.MetricsItem
	assert.Equal(t, 200, getJSON(t, srv.URL+"/api/v1/metrics/BTC-USD/history?window=1&limit=3", &hist))
	require.Len(t, hist, 3)
	assert.Equal(t, int64(2), hist[0].WindowEndNs)
	assert.Equal(t, int64(4), hist[2].WindowEndNs)
}

func TestOpenAPISpecIsValidYAML(t *testing.T) {
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(openAPISpec, &doc))
	assert.Equal(t, "3.1.0", doc["openapi"])
	paths := doc["paths"].(map[string]any)
	for _, p := range []string{"/api/v1/symbols", "/api/v1/metrics/{symbol}", "/api/v1/alerts", "/api/v1/backtests/{run_id}"} {
		assert.Contains(t, paths, p)
	}
}

func TestWebsocketFiltersBySymbol(t *testing.T) {
	a, srv := newTestAPI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws?symbols=ETH-USD", nil) //nolint:bodyclose // the library closes it
	require.NoError(t, err)
	defer conn.CloseNow()

	require.Eventually(t, func() bool { a.Hub.mu.Lock(); defer a.Hub.mu.Unlock(); return len(a.Hub.clients) == 1 },
		time.Second, 5*time.Millisecond)
	a.Hub.Broadcast(KindMetrics, "BTC-USD", []byte(`{"type":"metrics","data":{"symbol":"BTC-USD"}}`))
	a.Hub.Broadcast(KindAlert, "ETH-USD", []byte(`{"type":"alert","data":{"symbol":"ETH-USD"}}`))

	_, msg, err := conn.Read(ctx)
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"alert","data":{"symbol":"ETH-USD"}}`, string(msg), "BTC-USD was filtered out")
}

func TestSlowClientDropsOldestMetricsButKeepsAlerts(t *testing.T) {
	c := &client{wake: make(chan struct{}, 1), gone: make(chan struct{})}
	for i := range 5 {
		c.enqueue(KindMetrics, []byte{byte('0' + i)}, 3, 10)
	}
	c.enqueue(KindAlert, []byte("A"), 3, 10)
	got := c.next()
	assert.Equal(t, [][]byte{[]byte("A"), []byte("2"), []byte("3"), []byte("4")}, got, "alert first, 2 oldest metrics dropped")

	for range 3 {
		c.enqueue(KindAlert, []byte("A"), 3, 2)
	}
	select {
	case <-c.gone:
	default:
		t.Fatal("client that falls too far behind on alerts is disconnected")
	}
}
