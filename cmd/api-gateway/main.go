// Command api-gateway serves the dashboard: a REST API (latest metrics, alert
// history, feed health, backtest reports) and a websocket that streams live
// metrics, alerts and feed health from Kafka.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
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
	config.Symbols
	APIAddr string `env:"API_ADDR" envDefault:":8080"`
	Static  string `env:"DASHBOARD_DIR" envDefault:"/app/dashboard"`
	Bucket  string `env:"ARCHIVE_BUCKET" envDefault:"tickstream-archive"`
}

func (c *Config) Validate() error {
	return config.ValidateAll(c.Common, c.Kafka, c.Symbols)
}

// envelope is the websocket message format: {"type": "...", "data": {...}}.
type envelope struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

// streamLive reads the live topics from their current end (no consumer group:
// every api-gateway instance sees every message) and pushes them to the hub.
func streamLive(ctx context.Context, cl *kgo.Client, cfg Config, hub *Hub, hist *History, log *slog.Logger) error {
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, p int32, err error) { log.Warn("fetch error", "topic", t, "err", err) })
		fetches.EachRecord(func(r *kgo.Record) {
			var (
				kind, symbol string
				data         any
			)
			switch r.Topic {
			case cfg.Metrics:
				var m domain.Metrics
				if proto.Unmarshal(r.Value, &m) != nil {
					return
				}
				item := storage.MetricsItemFrom(&m)
				hist.Add(item)
				kind, symbol, data = KindMetrics, m.Symbol, item
			case cfg.Topics.Alerts:
				var a domain.Alert
				if proto.Unmarshal(r.Value, &a) != nil {
					return
				}
				kind, symbol, data = KindAlert, a.Symbol, storage.AlertItemFrom(&a)
			case cfg.Topics.FeedHealth:
				var h domain.FeedHealth
				if proto.Unmarshal(r.Value, &h) != nil {
					return
				}
				kind, symbol, data = KindHealth, h.Symbol, storage.FeedHealthItemFrom(&h)
			default:
				return
			}
			b, _ := json.Marshal(envelope{Type: kind, Data: data})
			hub.Broadcast(kind, symbol, b)
		})
	}
}

func main() {
	var cfg Config
	config.MustLoad(&cfg)
	observability.Run("api-gateway", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		ac, err := storage.LoadAWS(ctx, cfg.AWS)
		if err != nil {
			return err
		}
		db := storage.NewDynamoDB(ac)
		cl, err := kafkax.NewClient(cfg.Kafka,
			kgo.ConsumeTopics(cfg.Metrics, cfg.Topics.Alerts, cfg.Topics.FeedHealth),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
		if err != nil {
			return err
		}
		defer cl.Close()

		hub, hist := NewHub(), &History{}
		api := &API{
			Symbols: cfg.Symbols.Symbols,
			Metrics: &storage.MetricsRepo{DB: db, Table: cfg.LatestMetrics},
			Alerts:  &storage.AlertsRepo{DB: db, Table: cfg.Tables.Alerts},
			Health:  &storage.FeedHealthRepo{DB: db, Table: cfg.Tables.FeedHealth},
			Archive: &storage.S3Store{Client: storage.NewS3(ac, cfg.Endpoint), Bucket: cfg.Bucket},
			History: hist,
			Hub:     hub,
			Ops:     o.Handler(),
			Static:  cfg.Static,
			Log:     o.Log,
		}
		srv := &http.Server{Addr: cfg.APIAddr, Handler: api.Router(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
		go func() {
			if err := streamLive(ctx, cl, cfg, hub, hist, o.Log); err != nil && !errors.Is(err, context.Canceled) {
				o.Log.Error("live stream stopped", "err", err)
			}
		}()
		o.SetReady(true)
		o.Log.Info("serving", "addr", cfg.APIAddr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return ctx.Err()
	})
}
