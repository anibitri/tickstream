// Command risk-engine evaluates YAML-defined alert rules against md.metrics
// and publishes deduplicated, cooled-down alerts to risk.alerts.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
	"github.com/anibitri/tickstream/internal/rules"
)

type Config struct {
	config.Common
	config.Kafka
	config.Topics
	RulesFile string `env:"RULES_FILE" envDefault:"/etc/tickstream/rules.yaml"`
}

func (c *Config) Validate() error {
	if c.GroupID == "" {
		c.GroupID = "risk-engine"
	}
	return config.ValidateAll(c.Common, c.Kafka)
}

func main() {
	var cfg Config
	config.MustLoad(&cfg)
	set, err := rules.Load(cfg.RulesFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load rules:", err)
		os.Exit(2)
	}
	observability.Run("risk-engine", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		for _, r := range set.Rules {
			o.Log.Info("rule loaded", "name", r.Name, "window_secs", r.WindowSecs, "condition", r.Cond.String(),
				"severity", r.Severity.String(), "cooldown", r.Cooldown.String())
		}
		h := &rules.Handler{Set: set, InTopic: cfg.Metrics, OutTopic: cfg.Alerts, DLQTopic: cfg.DLQPrefix + "risk-engine",
			Tracer: o.Tracer, Log: o.Log}
		loop := &kafkax.Loop{Handler: h, Log: o.Log, Group: cfg.GroupID}
		cl, err := kafkax.NewClient(cfg.Kafka, append(kafkax.ConsumerOpts(cfg.GroupID, cfg.Metrics), loop.RebalanceOpts()...)...)
		if err != nil {
			return err
		}
		defer cl.Close()
		loop.Client = cl
		o.SetReady(true)
		return loop.Run(ctx)
	})
}
