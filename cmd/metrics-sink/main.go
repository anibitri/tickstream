// Command metrics-sink stores the latest metrics per symbol and window size,
// and the latest feed health per exchange and symbol, in DynamoDB.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
	"github.com/anibitri/tickstream/internal/storage"
)

type Config struct {
	config.Common
	config.Kafka
	config.Topics
	config.AWS
	config.Tables
	Workers int `env:"SINK_WORKERS" envDefault:"8"`
}

func (c *Config) Validate() error {
	if c.GroupID == "" {
		c.GroupID = "metrics-sink"
	}
	if c.Workers < 1 {
		return fmt.Errorf("SINK_WORKERS must be at least 1")
	}
	return config.ValidateAll(c.Common, c.Kafka)
}

// Sink writes each batch to DynamoDB before the loop commits it.
type Sink struct {
	Metrics      *storage.MetricsRepo
	Health       *storage.FeedHealthRepo
	MetricsTopic string
	HealthTopic  string
	Workers      int
	RetryBase    time.Duration
	Log          *slog.Logger
}

type metricsKey struct {
	symbol string
	secs   int32
}

type healthKey struct{ exchange, symbol string }

// Handle keeps only the newest record per key from the batch (older ones
// would be rejected by DynamoDB's condition anyway) and writes them in parallel.
func (s *Sink) Handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
	latest := map[metricsKey]*domain.Metrics{}
	health := map[healthKey]*domain.FeedHealth{}
	for _, r := range recs {
		switch r.Topic {
		case s.MetricsTopic:
			var m domain.Metrics
			if err := proto.Unmarshal(r.Value, &m); err != nil {
				s.Log.Warn("skipping undecodable metrics", "offset", r.Offset, "err", err)
				continue
			}
			k := metricsKey{m.Symbol, m.WindowSecs}
			if cur := latest[k]; cur == nil || m.WindowEndNs > cur.WindowEndNs {
				latest[k] = &m
			}
		case s.HealthTopic:
			var h domain.FeedHealth
			if err := proto.Unmarshal(r.Value, &h); err != nil {
				s.Log.Warn("skipping undecodable feed health", "offset", r.Offset, "err", err)
				continue
			}
			k := healthKey{h.Exchange, h.Symbol}
			if cur := health[k]; cur == nil || h.ReportedAtNs > cur.ReportedAtNs {
				health[k] = &h
			}
		}
	}

	var jobs []func(context.Context) error
	for _, m := range latest {
		jobs = append(jobs, func(ctx context.Context) error { _, err := s.Metrics.PutLatest(ctx, m); return err })
	}
	for _, h := range health {
		jobs = append(jobs, func(ctx context.Context) error { return s.Health.Put(ctx, h) })
	}
	return nil, s.runAll(ctx, jobs)
}

// runAll runs jobs on a few workers, retrying each failed write a few times.
func (s *Sink) runAll(ctx context.Context, jobs []func(context.Context) error) error {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		next     = make(chan func(context.Context) error)
	)
	for range min(s.Workers, len(jobs)) {
		wg.Go(func() {
			for job := range next {
				if err := s.retry(ctx, job); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		})
	}
	for _, j := range jobs {
		next <- j
	}
	close(next)
	wg.Wait()
	return firstErr
}

func (s *Sink) retry(ctx context.Context, job func(context.Context) error) error {
	var err error
	for attempt := range 4 {
		if err = job(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.RetryBase << attempt):
		}
	}
	return err
}

func main() {
	var cfg Config
	config.MustLoad(&cfg)
	observability.Run("metrics-sink", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		ac, err := storage.LoadAWS(ctx, cfg.AWS)
		if err != nil {
			return err
		}
		db := storage.NewDynamoDB(ac)
		h := &Sink{
			Metrics:      &storage.MetricsRepo{DB: db, Table: cfg.LatestMetrics},
			Health:       &storage.FeedHealthRepo{DB: db, Table: cfg.Tables.FeedHealth},
			MetricsTopic: cfg.Metrics,
			HealthTopic:  cfg.Topics.FeedHealth,
			Workers:      cfg.Workers,
			RetryBase:    100 * time.Millisecond,
			Log:          o.Log,
		}
		loop := &kafkax.Loop{Handler: h, Log: o.Log, Group: cfg.GroupID}
		cl, err := kafkax.NewClient(cfg.Kafka, append(kafkax.ConsumerOpts(cfg.GroupID, cfg.Metrics, cfg.Topics.FeedHealth),
			loop.RebalanceOpts()...)...)
		if err != nil {
			return err
		}
		defer cl.Close()
		loop.Client = cl
		o.SetReady(true)
		return loop.Run(ctx)
	})
}
