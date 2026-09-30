// Command metrics-engine computes per-symbol event-time window metrics from
// md.trades and publishes one record per symbol per closed window to md.metrics.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/metrics"
	"github.com/anibitri/tickstream/internal/observability"
)

type Config struct {
	config.Common
	config.Kafka
	config.Topics
	WindowSecs  []int32       `env:"WINDOW_SECS" envSeparator:"," envDefault:"1,10,60"`
	Lateness    time.Duration `env:"ALLOWED_LATENESS" envDefault:"500ms"`
	Mode        string        `env:"MODE" envDefault:"live"` // live | replay
	IdleAdvance time.Duration `env:"IDLE_WATERMARK_ADVANCE" envDefault:"2s"`
}

func (c *Config) Validate() error {
	if c.GroupID == "" {
		c.GroupID = "metrics-engine"
	}
	if c.Mode != "live" && c.Mode != "replay" {
		return fmt.Errorf("MODE must be live or replay, got %q", c.Mode)
	}
	if len(c.WindowSecs) == 0 {
		return fmt.Errorf("WINDOW_SECS must not be empty")
	}
	for _, s := range c.WindowSecs {
		if s <= 0 || 3600%s != 0 {
			return fmt.Errorf("window size %ds must be a positive divisor of 3600", s)
		}
	}
	return config.ValidateAll(c.Common, c.Kafka)
}

func main() {
	var cfg Config
	config.MustLoad(&cfg)
	observability.Run("metrics-engine", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		h := &metrics.Handler{
			Cfg:         metrics.Config{WindowSecs: cfg.WindowSecs, AllowedLateness: cfg.Lateness},
			InTopic:     cfg.Trades,
			OutTopic:    cfg.Metrics,
			DLQTopic:    cfg.DLQPrefix + "metrics-engine",
			Live:        cfg.Mode == "live",
			IdleAdvance: cfg.IdleAdvance,
			Tracer:      o.Tracer,
			Log:         o.Log,
		}
		loop := &kafkax.Loop{Handler: h, Log: o.Log, Group: cfg.GroupID}
		cl, err := kafkax.NewClient(cfg.Kafka, append(kafkax.ConsumerOpts(cfg.GroupID, cfg.Trades), loop.RebalanceOpts()...)...)
		if err != nil {
			return err
		}
		defer cl.Close()
		loop.Client = cl
		o.SetReady(true)
		o.Log.Info("computing metrics", "in", cfg.Trades, "out", cfg.Metrics, "windows", cfg.WindowSecs, "mode", cfg.Mode)
		return loop.Run(ctx)
	})
}
