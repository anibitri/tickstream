package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/anibitri/tickstream/internal/storage"
)

//go:embed openapi.yaml
var openAPISpec []byte

// API serves the REST endpoints and the websocket for the dashboard.
type API struct {
	Symbols []string
	Metrics interface {
		GetLatest(ctx context.Context, symbol string, windowSecs int32) (*storage.MetricsItem, error)
	}
	Alerts interface {
		Query(ctx context.Context, symbol string, sinceNs int64, limit int32, cursor string) (*storage.AlertPage, error)
	}
	Health interface {
		List(ctx context.Context) ([]storage.FeedHealthItem, error)
	}
	Archive storage.ObjectStore // backtest reports live under backtests/<run_id>/
	History *History
	Hub     *Hub
	Ops     http.Handler // /metrics, /healthz, /readyz
	Log     *slog.Logger
}

// Router builds the HTTP routes.
func (a *API) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/api/v1/symbols", a.symbols)
	r.Get("/api/v1/metrics/{symbol}", a.latestMetrics)
	r.Get("/api/v1/metrics/{symbol}/history", a.history)
	r.Get("/api/v1/alerts", a.alerts)
	r.Get("/api/v1/backtests", a.listBacktests)
	r.Get("/api/v1/backtests/{runID}", a.backtest)
	r.Get("/api/openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openAPISpec)
	})
	r.Get("/ws", a.websocket)
	if a.Ops != nil {
		r.Handle("/metrics", a.Ops)
		r.Handle("/healthz", a.Ops)
		r.Handle("/readyz", a.Ops)
	}
	return r
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (a *API) serverError(w http.ResponseWriter, err error) {
	a.Log.Error("request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func (a *API) knownSymbol(s string) bool {
	for _, x := range a.Symbols {
		if x == s {
			return true
		}
	}
	return false
}

// windowParam reads ?window= (default 10).
func windowParam(r *http.Request) (int32, bool) {
	v := r.URL.Query().Get("window")
	if v == "" {
		return 10, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || (n != 1 && n != 10 && n != 60) {
		return 0, false
	}
	return int32(n), true
}

type symbolInfo struct {
	Symbol string                   `json:"symbol"`
	Feeds  []storage.FeedHealthItem `json:"feeds"`
}

func (a *API) symbols(w http.ResponseWriter, r *http.Request) {
	items, err := a.Health.List(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	bySymbol := map[string][]storage.FeedHealthItem{}
	for _, it := range items {
		bySymbol[it.Symbol] = append(bySymbol[it.Symbol], it)
	}
	out := make([]symbolInfo, 0, len(a.Symbols))
	for _, s := range a.Symbols {
		feeds := bySymbol[s]
		sort.Slice(feeds, func(i, j int) bool { return feeds[i].Exchange < feeds[j].Exchange })
		out = append(out, symbolInfo{Symbol: s, Feeds: append([]storage.FeedHealthItem{}, feeds...)})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) latestMetrics(w http.ResponseWriter, r *http.Request) {
	sym := chi.URLParam(r, "symbol")
	win, ok := windowParam(r)
	if !a.knownSymbol(sym) || !ok {
		writeError(w, http.StatusBadRequest, "unknown symbol or window (use 1, 10 or 60)")
		return
	}
	m, err := a.Metrics.GetLatest(r.Context(), sym, win)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no metrics yet")
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (a *API) history(w http.ResponseWriter, r *http.Request) {
	sym := chi.URLParam(r, "symbol")
	win, ok := windowParam(r)
	if !a.knownSymbol(sym) || !ok {
		writeError(w, http.StatusBadRequest, "unknown symbol or window (use 1, 10 or 60)")
		return
	}
	limit := intParam(r, "limit", 300, 1, HistorySize)
	writeJSON(w, http.StatusOK, a.History.Last(sym, win, limit))
}

func intParam(r *http.Request, name string, def, lo, hi int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(name))
	if err != nil {
		return def
	}
	return min(max(n, lo), hi)
}

// alerts returns alert history, newest first. With ?symbol= it pages through
// that symbol using next_cursor; without it, it merges the newest alerts of
// every symbol (no paging).
func (a *API) alerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := int32(intParam(r, "limit", 50, 1, 500))
	since, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	if sym := q.Get("symbol"); sym != "" {
		if !a.knownSymbol(sym) {
			writeError(w, http.StatusBadRequest, "unknown symbol")
			return
		}
		page, err := a.Alerts.Query(r.Context(), sym, since, limit, q.Get("cursor"))
		if err != nil {
			a.serverError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	var all []storage.AlertItem
	for _, sym := range a.Symbols {
		page, err := a.Alerts.Query(r.Context(), sym, since, limit, "")
		if err != nil {
			a.serverError(w, err)
			return
		}
		all = append(all, page.Items...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].WindowEndNs > all[j].WindowEndNs })
	if len(all) > int(limit) {
		all = all[:limit]
	}
	writeJSON(w, http.StatusOK, storage.AlertPage{Items: append([]storage.AlertItem{}, all...)})
}

func (a *API) listBacktests(w http.ResponseWriter, r *http.Request) {
	keys, err := a.Archive.List(r.Context(), "backtests/")
	if err != nil {
		a.serverError(w, err)
		return
	}
	runs := []string{}
	for _, k := range keys {
		if run, ok := strings.CutSuffix(strings.TrimPrefix(k, "backtests/"), "/report.json"); ok {
			runs = append(runs, run)
		}
	}
	writeJSON(w, http.StatusOK, map[string][]string{"runs": runs})
}

func (a *API) backtest(w http.ResponseWriter, r *http.Request) {
	run := chi.URLParam(r, "runID")
	if run == "" || strings.ContainsAny(run, "/.") {
		writeError(w, http.StatusBadRequest, "invalid run id")
		return
	}
	b, err := a.Archive.Get(r.Context(), "backtests/"+run+"/report.json")
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such backtest")
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (a *API) websocket(w http.ResponseWriter, r *http.Request) {
	var symbols []string
	if s := r.URL.Query().Get("symbols"); s != "" {
		symbols = strings.Split(s, ",")
	}
	// The dashboard is served from another origin in development (Vite).
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	a.Hub.Serve(r.Context(), conn, symbols)
}

// HistorySize is how many recent windows are kept per symbol and window size.
const HistorySize = 600

// History keeps recent metrics in memory so a freshly opened chart is not empty.
type History struct {
	mu   sync.Mutex
	data map[string][]storage.MetricsItem
}

func historyKey(symbol string, secs int32) string { return symbol + "/" + strconv.Itoa(int(secs)) }

// Add appends a window, keeping at most HistorySize per key.
func (h *History) Add(m storage.MetricsItem) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.data == nil {
		h.data = map[string][]storage.MetricsItem{}
	}
	k := historyKey(m.Symbol, m.WindowSecs)
	s := h.data[k]
	if n := len(s); n > 0 && s[n-1].WindowEndNs >= m.WindowEndNs {
		return // duplicate or out of order
	}
	s = append(s, m)
	if len(s) > HistorySize {
		s = s[len(s)-HistorySize:]
	}
	h.data[k] = s
}

// Last returns up to n most recent windows, oldest first.
func (h *History) Last(symbol string, secs int32, n int) []storage.MetricsItem {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.data[historyKey(symbol, secs)]
	if len(s) > n {
		s = s[len(s)-n:]
	}
	return append([]storage.MetricsItem{}, s...)
}
