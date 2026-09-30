// Package config reads settings from environment variables into typed
// structs and checks them at startup, so a bad setting fails fast.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/anibitri/tickstream/internal/domain"
)

// Validator is implemented by config structs that need cross-field checks.
type Validator interface{ Validate() error }

// Load parses environment variables into cfg and validates it. Nested structs
// that implement Validator are validated too.
func Load(cfg any) error {
	if err := env.Parse(cfg); err != nil {
		return fmt.Errorf("parse env: %w", err)
	}
	if v, ok := cfg.(Validator); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}
	}
	return nil
}

// MustLoad is Load for main functions: it exits the process on error.
func MustLoad(cfg any) {
	if err := Load(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

// Common holds settings shared by every service.
type Common struct {
	ServiceName      string        `env:"SERVICE_NAME"`
	LogLevel         string        `env:"LOG_LEVEL" envDefault:"info"`
	HTTPAddr         string        `env:"HTTP_ADDR" envDefault:":9100"` // /metrics, /healthz, /readyz
	OTLPEndpoint     string        `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`  // e.g. jaeger:4317; empty disables tracing
	TraceSampleRatio float64       `env:"OTEL_TRACE_SAMPLE_RATIO" envDefault:"0.01"`
	ShutdownTimeout  time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

func (c Common) Validate() error {
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("LOG_LEVEL %q must be debug|info|warn|error", c.LogLevel)
	}
	if c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 {
		return fmt.Errorf("OTEL_TRACE_SAMPLE_RATIO must be in [0,1], got %v", c.TraceSampleRatio)
	}
	return nil
}

// Kafka holds broker connection settings.
type Kafka struct {
	Brokers  []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	GroupID  string   `env:"KAFKA_GROUP_ID"`
	ClientID string   `env:"KAFKA_CLIENT_ID"`
}

func (k Kafka) Validate() error {
	if len(k.Brokers) == 0 || k.Brokers[0] == "" {
		return errors.New("KAFKA_BROKERS is required")
	}
	return nil
}

// Topics are configurable so the same binaries can run against isolated
// replay.<run_id>.* topics during backtests.
type Topics struct {
	RawPrefix  string `env:"TOPIC_RAW_PREFIX" envDefault:"raw.trades."`
	Trades     string `env:"TOPIC_TRADES" envDefault:"md.trades"`
	Metrics    string `env:"TOPIC_METRICS" envDefault:"md.metrics"`
	Alerts     string `env:"TOPIC_ALERTS" envDefault:"risk.alerts"`
	FeedHealth string `env:"TOPIC_FEED_HEALTH" envDefault:"md.feed_health"`
	DLQPrefix  string `env:"TOPIC_DLQ_PREFIX" envDefault:"dlq."`
}

// AWS holds AWS SDK settings. AWS_ENDPOINT_URL points the SDK at LocalStack;
// credentials come from the standard AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY chain.
type AWS struct {
	Region   string `env:"AWS_REGION" envDefault:"eu-west-2"`
	Endpoint string `env:"AWS_ENDPOINT_URL"`
}

// Tables names the DynamoDB tables.
type Tables struct {
	LatestMetrics string `env:"DDB_TABLE_LATEST_METRICS" envDefault:"latest_metrics"`
	Alerts        string `env:"DDB_TABLE_ALERTS" envDefault:"alerts"`
	FeedHealth    string `env:"DDB_TABLE_FEED_HEALTH" envDefault:"feed_health"`
}

// Symbols is the list of tracked symbols, e.g. SYMBOLS=BTC-USD,ETH-USD.
type Symbols struct {
	Symbols []string `env:"SYMBOLS" envSeparator:"," envDefault:"BTC-USD,ETH-USD,SOL-USD"`
}

func (s Symbols) Validate() error {
	if len(s.Symbols) == 0 {
		return errors.New("SYMBOLS must not be empty")
	}
	for _, sym := range s.Symbols {
		if !domain.ValidSymbol(sym) {
			return fmt.Errorf("SYMBOLS: %q is not in BASE-QUOTE form (e.g. BTC-USD)", sym)
		}
	}
	return nil
}

// Set returns the symbols as a lookup set.
func (s Symbols) Set() map[string]bool {
	m := make(map[string]bool, len(s.Symbols))
	for _, sym := range s.Symbols {
		m[sym] = true
	}
	return m
}

// ValidateAll returns the first error from the given validators.
func ValidateAll(vs ...Validator) error {
	for _, v := range vs {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	return nil
}
