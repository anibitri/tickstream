package replay

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/shopspring/decimal"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/storage"
)

// GenConfig describes a synthetic dataset. The same config always produces
// byte-identical files (fixed random seed).
//
// The data is a random walk with occasional price jumps, volume bursts and
// short divergences between exchanges. It has no built-in trading edge, so it
// is for testing and benchmarking the pipeline, not for judging a strategy.
type GenConfig struct {
	Seed         uint64
	Start        time.Time
	Duration     time.Duration
	TradesPerSec float64            // across all symbols and exchanges
	Symbols      map[string]float64 // symbol -> starting price
	Exchanges    []string
	MaxRows      int // rows per file before starting a new part (bounds memory)
}

// DefaultGenConfig is a small dataset: 20 minutes, ~10 trades/s, 3 symbols.
func DefaultGenConfig() GenConfig {
	return GenConfig{
		Seed:         1,
		Start:        time.Date(2026, 10, 4, 13, 50, 0, 0, time.UTC),
		Duration:     20 * time.Minute,
		TradesPerSec: 10,
		Symbols:      map[string]float64{"BTC-USD": 60000, "ETH-USD": 2500, "SOL-USD": 150},
		Exchanges:    []string{"coinbase", "kraken"},
		MaxRows:      500_000,
	}
}

type genSymbol struct {
	name     string
	mid      float64
	offset   []float64 // per-exchange price offset (fraction), mean-reverting noise
	burstEnd int64     // volume burst active until this time
	divEnd   int64     // exchange divergence active until this time
	divSize  float64
}

// Generate writes the dataset to store in the archive layout and returns the
// number of trades written.
func Generate(ctx context.Context, store storage.ObjectStore, cfg GenConfig) (int, error) {
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	names := sortedKeys(cfg.Symbols)
	syms := make([]*genSymbol, len(names))
	for i, n := range names {
		syms[i] = &genSymbol{name: n, mid: cfg.Symbols[n], offset: make([]float64, len(cfg.Exchanges))}
	}
	w := &genWriter{store: store, maxRows: cfg.MaxRows, buffers: map[string]*genBuffer{}, nextID: map[string]int64{}}

	const annualVol = 0.6
	stepNs := int64(10 * time.Millisecond) // simulation step
	perStep := cfg.TradesPerSec * float64(stepNs) / 1e9 / float64(len(syms)*len(cfg.Exchanges))
	stepVol := annualVol * math.Sqrt(float64(stepNs)/1e9/(365*86400))
	startNs, endNs := cfg.Start.UnixNano(), cfg.Start.Add(cfg.Duration).UnixNano()

	for t := startNs; t < endNs; t += stepNs {
		if err := ctx.Err(); err != nil {
			return w.count, err
		}
		for _, s := range syms {
			s.mid *= math.Exp(stepVol * rng.NormFloat64())
			if rng.Float64() < 0.00002 { // rare jump of 0.5–1.5%
				s.mid *= 1 + (0.005+0.01*rng.Float64())*sign(rng)
			}
			if t > s.burstEnd && rng.Float64() < 0.00002 {
				s.burstEnd = t + int64(20*time.Second)
			}
			if t > s.divEnd && rng.Float64() < 0.00001 {
				s.divEnd, s.divSize = t+int64(3*time.Second), 0.003*sign(rng)
			}
			rate := perStep
			if t < s.burstEnd {
				rate *= 8
			}
			for e, exch := range cfg.Exchanges {
				s.offset[e] = 0.98*s.offset[e] + 0.00002*rng.NormFloat64()
				for n := poisson(rng, rate); n > 0; n-- {
					off := s.offset[e]
					if t < s.divEnd && e == len(cfg.Exchanges)-1 {
						off += s.divSize
					}
					side := domain.SideBuy
					spread := 0.00005 // half-spread: 0.5 bp
					if rng.Float64() < 0.5 {
						side, spread = domain.SideSell, -spread
					}
					eventNs := t + rng.Int64N(stepNs)
					price := decimal.NewFromFloat(s.mid * (1 + off + spread)).Round(2)
					size := decimal.NewFromFloat(math.Exp(rng.NormFloat64()-3) * 10000 / s.mid).Round(8)
					if !size.IsPositive() {
						size = decimal.New(1, -8)
					}
					if err := w.add(ctx, &domain.Trade{Exchange: exch, Symbol: s.name, EventTimeNs: eventNs,
						RecvTimeNs: eventNs + int64(20*time.Millisecond) + rng.Int64N(int64(60*time.Millisecond)),
						Price:      price.String(), Size: size.String(), Side: side}); err != nil {
						return w.count, err
					}
				}
			}
		}
	}
	return w.count, w.flushAll(ctx)
}

func sign(rng *rand.Rand) float64 {
	if rng.Float64() < 0.5 {
		return -1
	}
	return 1
}

// poisson draws from a Poisson distribution with small mean lambda.
func poisson(rng *rand.Rand, lambda float64) int {
	l, k, p := math.Exp(-lambda), 0, 1.0
	for {
		p *= rng.Float64()
		if p <= l {
			return k
		}
		k++
	}
}

type genBuffer struct {
	hour  int64
	rows  []storage.TradeRow
	first int64
}

type genWriter struct {
	store   storage.ObjectStore
	maxRows int
	buffers map[string]*genBuffer // exchange|symbol
	nextID  map[string]int64      // per-exchange trade id / offset
	count   int
}

func (w *genWriter) add(ctx context.Context, t *domain.Trade) error {
	id := w.nextID[t.Exchange] + 1
	w.nextID[t.Exchange] = id
	t.TradeId = strconv.FormatInt(id, 10)
	key := t.Exchange + "|" + t.Symbol
	hour := time.Unix(0, t.EventTimeNs).UTC().Truncate(time.Hour).UnixNano()
	b := w.buffers[key]
	if b != nil && (b.hour != hour || len(b.rows) >= w.maxRows) {
		if err := w.flush(ctx, b); err != nil {
			return err
		}
		b = nil
	}
	if b == nil {
		b = &genBuffer{hour: hour, first: id}
		w.buffers[key] = b
	}
	b.rows = append(b.rows, storage.RowFromTrade(t, id))
	w.count++
	return nil
}

func (w *genWriter) flush(ctx context.Context, b *genBuffer) error {
	if len(b.rows) == 0 {
		return nil
	}
	body, err := storage.EncodeTrades(b.rows)
	if err != nil {
		return err
	}
	r := b.rows[0]
	key := storage.ArchiveKey(r.Exchange, r.Symbol, b.hour, b.first, b.rows[len(b.rows)-1].KafkaOffset)
	if err := w.store.Put(ctx, key, body, "application/vnd.apache.parquet"); err != nil {
		return fmt.Errorf("write %s: %w", key, err)
	}
	b.rows = nil
	return nil
}

func (w *genWriter) flushAll(ctx context.Context) error {
	for _, k := range sortedKeys(w.buffers) {
		if err := w.flush(ctx, w.buffers[k]); err != nil {
			return err
		}
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
