package main

// Tests against the committed fixture: a 20-minute synthetic dataset in
// testdata/archive, generated with
//
//	go run ./cmd/replayer -generate -dir testdata/archive -minutes 20 -tps 10

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/metrics"
	"github.com/anibitri/tickstream/internal/replay"
	"github.com/anibitri/tickstream/internal/rules"
	"github.com/anibitri/tickstream/internal/storage"
)

var update = flag.Bool("update", false, "rewrite testdata/metrics.golden.jsonl")

const (
	fixtureDir    = "../../testdata/archive"
	goldenPath    = "../../testdata/metrics.golden.jsonl"
	shippedRules  = "../../deploy/rules.yaml"
	fixtureTrades = 13857
)

func fixtureFlags(t *testing.T) flags {
	t.Helper()
	f, err := parseFlags([]string{"-from", fixtureFrom, "-to", fixtureTo, "-dir", fixtureDir, "-run-id", "fixture",
		"-rules", shippedRules})
	require.NoError(t, err)
	return f
}

// goldenLine renders one window in a stable text form. Floats are rounded so
// the file is identical on amd64 and arm64 (the last bits of math.Log differ).
func goldenLine(m *domain.Metrics) string {
	var ex []string
	for _, x := range m.Exchanges {
		ex = append(ex, fmt.Sprintf("%s:%s/%s/%d", x.Exchange, x.Vwap, x.Volume, x.TradeCount))
	}
	return fmt.Sprintf(`{"symbol":%q,"window_secs":%d,"start_ns":%d,"vwap":%q,"volume":%q,"trades":%d,"high":%q,"low":%q,"last":%q,"vol":%q,"spread_bps":%q,"exchanges":%q}`,
		m.Symbol, m.WindowSecs, m.WindowStartNs, m.Vwap, m.Volume, m.TradeCount, m.High, m.Low, m.LastPrice,
		fmt.Sprintf("%.9g", m.RealisedVol), fmt.Sprintf("%.6f", m.XexSpreadBps), strings.Join(ex, " "))
}

func runFixture(t *testing.T) *replay.Run {
	t.Helper()
	f := fixtureFlags(t)
	set, err := rules.Load(shippedRules)
	require.NoError(t, err)
	r, err := replay.Open(context.Background(), &storage.DirStore{Root: fixtureDir}, nil, f.from, f.to)
	require.NoError(t, err)
	run, err := replay.RunPipeline(r, metrics.DefaultConfig(), set)
	require.NoError(t, err)
	return run
}

// TestGoldenMetrics pins the metrics engine output for the fixture: the 10s
// and 60s windows in readable form, plus a hash over every window (including
// the 1s ones, which would make the file too long to review). If a change to
// the engine is intended, regenerate with:
//
//	go test ./cmd/backtester -run TestGoldenMetrics -update
func TestGoldenMetrics(t *testing.T) {
	run := runFixture(t)
	require.Equal(t, fixtureTrades, run.Trades)
	var buf, all bytes.Buffer
	for _, m := range run.Metrics {
		all.WriteString(goldenLine(m))
		if m.WindowSecs != 1 {
			buf.WriteString(goldenLine(m))
			buf.WriteByte('\n')
		}
	}
	sum := sha256.Sum256(all.Bytes())
	fmt.Fprintf(&buf, "{\"all_windows\":%d,\"sha256\":%q}\n", len(run.Metrics), hex.EncodeToString(sum[:]))
	if *update {
		require.NoError(t, os.WriteFile(goldenPath, buf.Bytes(), 0o644))
	}
	want, err := os.ReadFile(goldenPath)
	require.NoError(t, err)
	require.Equal(t, string(want), buf.String(), "metrics differ from %s", goldenPath)
}

// TestBacktestIsDeterministic runs the full backtest twice and requires
// byte-identical reports (this is the CI determinism check).
func TestBacktestIsDeterministic(t *testing.T) {
	f := fixtureFlags(t)
	store := &storage.DirStore{Root: fixtureDir}
	a, err := Backtest(context.Background(), store, f, nil)
	require.NoError(t, err)
	b, err := Backtest(context.Background(), store, f, nil)
	require.NoError(t, err)
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	require.Equal(t, string(ja), string(jb))

	assert.Equal(t, fixtureTrades, a.Data.Trades)
	assert.Len(t, a.Rules, 3, "stale_feed is not scored against price moves")
	for _, r := range a.Rules {
		assert.Len(t, r.Folds, 3, "5 blocks, 2 for training -> 3 test folds")
	}
	assert.Contains(t, a.Markdown(), "| price_jump |")
}

// Evaluating metrics that "came from Kafka" (shuffled, with repeats, as after
// a consumer restart) must give exactly the same report as computing them.
func TestKafkaMetricsGiveTheSameReport(t *testing.T) {
	f := fixtureFlags(t)
	store := &storage.DirStore{Root: fixtureDir}
	want, err := Backtest(context.Background(), store, f, nil)
	require.NoError(t, err)

	ms := runFixture(t).Metrics
	shuffled := make([]*domain.Metrics, 0, len(ms)+100)
	for i := len(ms) - 1; i >= 0; i-- { // reverse order: partitions interleave differently
		shuffled = append(shuffled, ms[i])
	}
	shuffled = append(shuffled, ms[:100]...) // repeats
	got, err := Backtest(context.Background(), store, f, shuffled)
	require.NoError(t, err)

	jw, _ := json.Marshal(want)
	jg, _ := json.Marshal(got)
	require.Equal(t, string(jw), string(jg))
}
