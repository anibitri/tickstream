// Command archiver writes every canonical trade from md.trades to S3 as
// Parquet, partitioned by exchange, symbol, date and hour.
package main

import (
	"context"
	"errors"
	"time"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
	"github.com/anibitri/tickstream/internal/storage"
)

type Config struct {
	config.Common
	config.Kafka
	config.Topics
	config.AWS
	Bucket    string        `env:"ARCHIVE_BUCKET" envDefault:"tickstream-archive"`
	MaxRows   int           `env:"ARCHIVE_MAX_ROWS" envDefault:"500000"` // roughly 32–64 MB of Parquet
	MaxAge    time.Duration `env:"ARCHIVE_MAX_AGE" envDefault:"5m"`
	HourGrace time.Duration `env:"ARCHIVE_HOUR_GRACE" envDefault:"1m"`
}

func (c *Config) Validate() error {
	if c.GroupID == "" {
		c.GroupID = "archiver"
	}
	if c.MaxRows < 1 {
		return errors.New("ARCHIVE_MAX_ROWS must be at least 1")
	}
	return config.ValidateAll(c.Common, c.Kafka)
}

func main() {
	var cfg Config
	config.MustLoad(&cfg)
	observability.Run("archiver", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		ac, err := storage.LoadAWS(ctx, cfg.AWS)
		if err != nil {
			return err
		}
		h := &Archiver{
			Store:     &storage.S3Store{Client: storage.NewS3(ac, cfg.Endpoint), Bucket: cfg.Bucket},
			InTopic:   cfg.Trades,
			MaxRows:   cfg.MaxRows,
			MaxAge:    cfg.MaxAge,
			HourGrace: cfg.HourGrace,
			RetryBase: 200 * time.Millisecond,
			Log:       o.Log,
		}
		loop := &kafkax.Loop{Handler: h, Log: o.Log, Group: cfg.GroupID, PollTimeout: 5 * time.Second}
		cl, err := kafkax.NewClient(cfg.Kafka, append(kafkax.ConsumerOpts(cfg.GroupID, cfg.Trades), loop.RebalanceOpts()...)...)
		if err != nil {
			return err
		}
		defer cl.Close()
		loop.Client = cl
		o.SetReady(true)
		o.Log.Info("archiving", "topic", cfg.Trades, "bucket", cfg.Bucket)
		return loop.Run(ctx)
	})
}
