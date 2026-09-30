// Command ingestor connects to public exchange websocket feeds and publishes
// raw trade frames to raw.trades.<exchange>, stamped with the receive time.
package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/exchange"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
)

type Config struct {
	config.Common
	config.Kafka
	config.Topics
	config.Symbols
	Exchanges      []string      `env:"EXCHANGES" envSeparator:"," envDefault:"coinbase,kraken"`
	ReadTimeout    time.Duration `env:"WS_READ_TIMEOUT" envDefault:"30s"`
	BackoffBase    time.Duration `env:"WS_BACKOFF_BASE" envDefault:"500ms"`
	BackoffMax     time.Duration `env:"WS_BACKOFF_MAX" envDefault:"30s"`
	GapThreshold   time.Duration `env:"FEED_GAP_ALERT_THRESHOLD" envDefault:"2s"`
	StaleAfter     time.Duration `env:"FEED_STALE_AFTER" envDefault:"15s"`
	HealthInterval time.Duration `env:"FEED_HEALTH_INTERVAL" envDefault:"5s"`
}

func (c *Config) Validate() error {
	if err := config.ValidateAll(c.Common, c.Kafka, c.Symbols); err != nil {
		return err
	}
	for _, e := range c.Exchanges {
		if _, err := exchange.Get(e); err != nil {
			return err
		}
	}
	if c.BackoffBase <= 0 || c.BackoffMax < c.BackoffBase {
		return fmt.Errorf("invalid backoff %s..%s", c.BackoffBase, c.BackoffMax)
	}
	return nil
}

func main() {
	recordDir := flag.String("record", "", "also append every raw frame to <dir>/<exchange>.jsonl (for test fixtures)")
	flag.Parse()

	var cfg Config
	config.MustLoad(&cfg)
	symbols := cfg.Symbols.Symbols

	observability.Run("ingestor", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		cl, err := kafkax.NewClient(cfg.Kafka)
		if err != nil {
			return err
		}
		defer cl.Close()
		defer func() { _ = cl.Flush(context.Background()) }()

		publish := func(ctx context.Context, r *kgo.Record) {
			cl.Produce(ctx, r, func(r *kgo.Record, err error) {
				if err != nil {
					o.Log.Error("produce failed", "topic", r.Topic, "err", err)
					return
				}
				observability.MessagesProduced.WithLabelValues(r.Topic).Inc()
			})
		}

		var wg sync.WaitGroup
		for _, name := range cfg.Exchanges {
			ad, _ := exchange.Get(name)
			f := &Feed{
				Adapter:      ad,
				Symbols:      symbols,
				RawTopic:     cfg.RawPrefix + name,
				AlertsTopic:  cfg.Alerts,
				HealthTopic:  cfg.FeedHealth,
				Publish:      publish,
				Backoff:      Backoff{Base: cfg.BackoffBase, Max: cfg.BackoffMax},
				ReadTimeout:  cfg.ReadTimeout,
				GapThreshold: cfg.GapThreshold,
				StaleAfter:   cfg.StaleAfter,
				Tracer:       o.Tracer,
				Log:          o.Log,
			}
			if *recordDir != "" {
				rec, err := NewFileRecorder(*recordDir, name)
				if err != nil {
					return err
				}
				defer rec.Close()
				f.Recorder = rec.Write
			}
			wg.Add(2)
			go func() { defer wg.Done(); _ = f.Run(ctx) }()
			go func() { defer wg.Done(); f.ReportHealth(ctx, cfg.HealthInterval) }()
		}
		o.SetReady(true)
		o.Log.Info("ingesting", "exchanges", strings.Join(cfg.Exchanges, ","), "symbols", symbols)
		wg.Wait()
		return ctx.Err()
	})
}
