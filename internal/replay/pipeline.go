package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/metrics"
	"github.com/anibitri/tickstream/internal/rules"
)

// Partitions matches the md.trades / md.metrics topics. Trades are split the
// same way Kafka splits them (by symbol), so each engine sees exactly what it
// would see in production.
const Partitions = 6

// TradeSource yields trades in event-time order (replay.Reader).
type TradeSource interface {
	Next() (*domain.Trade, error)
}

// EvalPoint is one rule evaluation captured from the risk engine.
type EvalPoint struct {
	Rule    string
	Symbol  string
	EndNs   int64
	Value   float64
	Defined bool
}

// Run is the output of pushing a trade stream through the engines.
type Run struct {
	Trades        int
	Metrics       []*domain.Metrics // in emission order
	Alerts        []*domain.Alert
	Evals         []EvalPoint
	InputSHA256   string
	MetricsSHA256 string
}

// RunPipeline feeds trades through the metrics engine and the rules engine,
// one engine pair per partition, exactly like the live services. Windows that
// are still open when the input ends are not emitted (the live system would
// only emit them when later trades arrive).
func RunPipeline(src TradeSource, mcfg metrics.Config, set *rules.Set) (*Run, error) {
	part := kgo.StickyKeyPartitioner(nil).ForTopic("md.trades")
	var (
		mEngines [Partitions]*metrics.Engine
		rEngines [Partitions]*rules.Engine
		mOffsets [Partitions]int64
		rOffsets [Partitions]int64
	)
	run := &Run{}
	for p := range Partitions {
		mEngines[p] = metrics.NewEngine(mcfg) // event clock: emitted_at = watermark
		rEngines[p] = rules.NewEngine(set)
		rEngines[p].Observer = func(e rules.Evaluation) {
			if !e.Rule.PerExch {
				run.Evals = append(run.Evals, EvalPoint{e.Rule.Name, e.Metrics.Symbol, e.Metrics.WindowEndNs, e.Value, e.Defined})
			}
		}
	}
	in := sha256.New()
	det := proto.MarshalOptions{Deterministic: true}
	for {
		t, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		b, _ := det.Marshal(t)
		in.Write(b)
		run.Trades++
		p := part.Partition(&kgo.Record{Key: []byte(t.Symbol)}, Partitions)
		emitted, _, err := mEngines[p].Add(t, mOffsets[p], trace.SpanContext{})
		if err != nil {
			return nil, err
		}
		mOffsets[p]++
		for _, e := range emitted {
			m := e.Metrics
			run.Metrics = append(run.Metrics, m)
			run.Alerts = append(run.Alerts, rEngines[p].Evaluate(m, rOffsets[p])...)
			rOffsets[p]++
		}
	}
	run.InputSHA256 = hex.EncodeToString(in.Sum(nil))
	run.MetricsSHA256 = MetricsHash(run.Metrics)
	return run, nil
}

// MetricsHash fingerprints a set of windows independent of arrival order:
// windows are sorted by (end, size, symbol) and hashed. Two runs that produced
// the same windows — in-process or through Kafka — get the same hash.
func MetricsHash(ms []*domain.Metrics) string {
	sorted := append([]*domain.Metrics(nil), ms...)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.WindowEndNs != b.WindowEndNs {
			return a.WindowEndNs < b.WindowEndNs
		}
		if a.WindowSecs != b.WindowSecs {
			return a.WindowSecs < b.WindowSecs
		}
		return a.Symbol < b.Symbol
	})
	h := sha256.New()
	det := proto.MarshalOptions{Deterministic: true}
	for _, m := range sorted {
		b, _ := det.Marshal(m)
		h.Write(b)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RunFromMetrics evaluates metrics that were computed elsewhere (read from a
// Kafka replay topic). Repeated windows — expected after a consumer restart —
// are dropped, keeping the first copy.
func RunFromMetrics(ms []*domain.Metrics, set *rules.Set) *Run {
	seen := map[[3]int64]map[string]bool{}
	run := &Run{}
	for _, m := range ms {
		k := [3]int64{m.WindowEndNs, int64(m.WindowSecs)}
		if seen[k] == nil {
			seen[k] = map[string]bool{}
		}
		if seen[k][m.Symbol] {
			continue
		}
		seen[k][m.Symbol] = true
		run.Metrics = append(run.Metrics, m)
	}
	// Evaluate rules per symbol in window order, as the risk engine does.
	sort.SliceStable(run.Metrics, func(i, j int) bool { return run.Metrics[i].WindowEndNs < run.Metrics[j].WindowEndNs })
	part := kgo.StickyKeyPartitioner(nil).ForTopic("md.metrics")
	var engines [Partitions]*rules.Engine
	for p := range Partitions {
		engines[p] = rules.NewEngine(set)
		engines[p].Observer = func(e rules.Evaluation) {
			if !e.Rule.PerExch {
				run.Evals = append(run.Evals, EvalPoint{e.Rule.Name, e.Metrics.Symbol, e.Metrics.WindowEndNs, e.Value, e.Defined})
			}
		}
	}
	var offs [Partitions]int64
	for _, m := range run.Metrics {
		p := part.Partition(&kgo.Record{Key: []byte(m.Symbol)}, Partitions)
		run.Alerts = append(run.Alerts, engines[p].Evaluate(m, offs[p])...)
		offs[p]++
	}
	run.MetricsSHA256 = MetricsHash(run.Metrics)
	return run
}
