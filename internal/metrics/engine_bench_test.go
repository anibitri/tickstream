package metrics

import (
	"strconv"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/anibitri/tickstream/internal/domain"
)

// BenchmarkEngineAdd measures the cost of folding one trade into the 1s, 10s
// and 60s windows (including window closes), with 3 symbols on 2 exchanges.
func BenchmarkEngineAdd(b *testing.B) {
	syms := []string{"BTC-USD", "ETH-USD", "SOL-USD"}
	exch := []string{"coinbase", "kraken"}
	trades := make([]*domain.Trade, 100_000)
	for i := range trades {
		trades[i] = &domain.Trade{Exchange: exch[i%2], Symbol: syms[i%3], TradeId: strconv.Itoa(i),
			EventTimeNs: t0 + int64(i)*10e6, Price: "60000." + strconv.Itoa(i%100), Size: "0.0" + strconv.Itoa(1+i%9)}
	}
	e := NewEngine(DefaultConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		t := trades[i%len(trades)]
		if i >= len(trades) { // keep event time moving forward on later laps
			t = &domain.Trade{Exchange: t.Exchange, Symbol: t.Symbol, TradeId: strconv.Itoa(i), EventTimeNs: t0 + int64(i)*10e6,
				Price: t.Price, Size: t.Size}
		}
		if _, _, err := e.Add(t, int64(i), trace.SpanContext{}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "trades/s")
}
