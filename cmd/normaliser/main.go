// Command normaliser parses raw exchange frames into canonical Trade protobufs
// on md.trades, deduplicating on (exchange, trade_id) and dead-lettering bad input.
package main

import (
	"context"
	"fmt"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
)

type Config struct {
	config.Common
	config.Kafka
	config.Topics
	config.Symbols
	Exchanges     []string `env:"EXCHANGES" envSeparator:"," envDefault:"coinbase,kraken"`
	DedupCapacity int      `env:"DEDUP_CAPACITY" envDefault:"500000"`
}

func (c *Config) Validate() error {
	if c.GroupID == "" {
		c.GroupID = "normaliser"
	}
	if c.DedupCapacity < 1000 {
		return fmt.Errorf("DEDUP_CAPACITY must be >= 1000")
	}
	return config.ValidateAll(c.Common, c.Kafka, c.Symbols)
}

func main() {
	var cfg Config
	config.MustLoad(&cfg)

	observability.Run("normaliser", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		h := &Handler{
			RawPrefix:   cfg.RawPrefix,
			TradesTopic: cfg.Trades,
			DLQTopic:    cfg.DLQPrefix + "normaliser",
			Allowed:     cfg.Symbols.Set(),
			Dedup:       NewDedupSet(cfg.DedupCapacity),
			Tracer:      o.Tracer,
			Log:         o.Log,
		}
		topics := make([]string, len(cfg.Exchanges))
		for i, e := range cfg.Exchanges {
			topics[i] = cfg.RawPrefix + e
		}
		loop := &kafkax.Loop{Handler: h, Log: o.Log, Group: cfg.GroupID}
		cl, err := kafkax.NewClient(cfg.Kafka, append(kafkax.ConsumerOpts(cfg.GroupID, topics...), loop.RebalanceOpts()...)...)
		if err != nil {
			return err
		}
		defer cl.Close()
		loop.Client = cl
		o.SetReady(true)
		return loop.Run(ctx)
	})
}
