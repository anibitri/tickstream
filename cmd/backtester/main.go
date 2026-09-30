// Command backtester replays archived trades through the metrics and risk
// engines (the same code the live services run) and evaluates the alert rules
// and a simple trading signal with walk-forward testing. It writes report.json
// and report.md locally and, when reading from S3, to backtests/<run_id>/.
//
//	backtester -from 2026-10-04T13:50:00Z -to 2026-10-04T14:10:00Z -dir testdata/archive -run-id fixture
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/metrics"
	"github.com/anibitri/tickstream/internal/replay"
	"github.com/anibitri/tickstream/internal/rules"
	"github.com/anibitri/tickstream/internal/storage"
)

type flags struct {
	from, to   time.Time
	symbols    []string
	runID      string
	dir        string
	rulesFile  string
	outDir     string
	opts       Options
	windowSecs []int32
	lateness   time.Duration
}

func parseFlags(args []string) (flags, error) {
	fs := flag.NewFlagSet("backtester", flag.ContinueOnError)
	from := fs.String("from", "", "start of the range (RFC 3339)")
	to := fs.String("to", "", "end of the range (RFC 3339)")
	symbols := fs.String("symbols", "", "comma-separated symbols (default: all)")
	runID := fs.String("run-id", "", "name of this run")
	dir := fs.String("dir", "", "read the archive from this folder instead of S3")
	rulesFile := fs.String("rules", "deploy/rules.yaml", "alert rules to evaluate")
	outDir := fs.String("out", "reports", "local folder for report.json / report.md")
	folds := fs.Int("folds", 5, "walk-forward blocks")
	train := fs.Int("train", 2, "blocks used for tuning before each test block")
	horizon := fs.Duration("horizon", 60*time.Second, "look-ahead for labelling true events")
	eventBps := fs.Float64("event-bps", 20, "move (bps) that counts as a true event")
	cost := fs.Float64("cost-bps", 5, "trading cost per fill in bps")
	lookback := fs.Int("signal-lookback", 6, "10s windows in the signal's VWAP baseline")
	hold := fs.Duration("signal-hold", 60*time.Second, "signal holding period")
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	f := flags{runID: *runID, dir: *dir, rulesFile: *rulesFile, outDir: *outDir,
		windowSecs: []int32{1, 10, 60}, lateness: 500 * time.Millisecond,
		opts: Options{Folds: *folds, TrainBlocks: *train, HorizonNs: int64(*horizon), EventBps: *eventBps,
			Signal: SignalParams{Lookback: *lookback, HoldNs: int64(*hold), CostBps: *cost}}}
	if *symbols != "" {
		f.symbols = strings.Split(*symbols, ",")
	}
	var err error
	if f.from, err = time.Parse(time.RFC3339, *from); err != nil {
		return f, fmt.Errorf("-from: %w", err)
	}
	if f.to, err = time.Parse(time.RFC3339, *to); err != nil {
		return f, fmt.Errorf("-to: %w", err)
	}
	switch {
	case !f.to.After(f.from):
		return f, errors.New("-to must be after -from")
	case f.runID == "" || strings.ContainsAny(f.runID, "/. "):
		return f, errors.New("-run-id is required (letters, digits, - and _ only)")
	case f.opts.Folds < 2 || f.opts.TrainBlocks < 1 || f.opts.TrainBlocks >= f.opts.Folds:
		return f, errors.New("need -folds >= 2 and 1 <= -train < -folds")
	case f.opts.Signal.Lookback < 1:
		return f, errors.New("-signal-lookback must be >= 1")
	}
	return f, nil
}

// Backtest runs the whole evaluation and returns the report.
func Backtest(ctx context.Context, store storage.ObjectStore, f flags) (*Report, error) {
	set, err := rules.Load(f.rulesFile)
	if err != nil {
		return nil, err
	}
	r, err := replay.Open(ctx, store, f.symbols, f.from, f.to)
	if err != nil {
		return nil, err
	}
	run, err := RunPipeline(r, metrics.Config{WindowSecs: f.windowSecs, AllowedLateness: f.lateness}, set)
	if err != nil {
		return nil, err
	}
	if run.Trades == 0 {
		return nil, errors.New("no trades in the selected range")
	}
	ruleOut, sig := Evaluate(run, set, f.from.UnixNano(), f.to.UnixNano(), f.opts)
	syms := f.symbols
	if len(syms) == 0 {
		seen := map[string]bool{}
		for _, m := range run.Metrics {
			seen[m.Symbol] = true
		}
		syms = sortedSymbols(seen)
	}
	return &Report{
		RunID: f.runID, From: f.from.UTC().Format(time.RFC3339), To: f.to.UTC().Format(time.RFC3339), Symbols: syms,
		Options: OptionsOut{Folds: f.opts.Folds, TrainBlocks: f.opts.TrainBlocks, HorizonSecs: time.Duration(f.opts.HorizonNs).Seconds(),
			EventBps: f.opts.EventBps, CostBpsPerFill: f.opts.Signal.CostBps, SignalLookback: f.opts.Signal.Lookback,
			SignalHoldSecs: time.Duration(f.opts.Signal.HoldNs).Seconds()},
		Data: DataOut{Trades: run.Trades, Windows: len(run.Metrics), Alerts: len(run.Alerts),
			InputSHA256: run.InputSHA256, MetricsSHA256: run.MetricsSHA256},
		Rules:  ruleOut,
		Signal: sig,
		Notes: []string{
			"Thresholds and the signal's k are tuned on earlier blocks only; every number above is out of sample.",
			"The z-score baseline and the signal's VWAP baseline use previous windows only (no lookahead).",
			"Windows still open when the range ends are not emitted, matching the live system.",
			fmt.Sprintf("True events are labelled with prices up to %.0fs after each window, used for scoring only.",
				time.Duration(f.opts.HorizonNs).Seconds()),
		},
	}, nil
}

func main() {
	f, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "backtester")
	if err := run(context.Background(), f, log); err != nil {
		log.Error("backtest failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, f flags, log *slog.Logger) error {
	var cfg struct {
		config.AWS
		Bucket string `env:"ARCHIVE_BUCKET" envDefault:"tickstream-archive"`
	}
	if err := config.Load(&cfg); err != nil {
		return err
	}
	var store storage.ObjectStore = &storage.DirStore{Root: f.dir}
	if f.dir == "" {
		ac, err := storage.LoadAWS(ctx, cfg.AWS)
		if err != nil {
			return err
		}
		store = &storage.S3Store{Client: storage.NewS3(ac, cfg.Endpoint), Bucket: cfg.Bucket}
	}
	start := time.Now()
	rep, err := Backtest(ctx, store, f)
	if err != nil {
		return err
	}
	body, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	md := rep.Markdown()
	dir := filepath.Join(f.outDir, f.runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), body, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.md"), []byte(md), 0o644); err != nil {
		return err
	}
	if f.dir == "" { // reading from S3: publish the report next to the archive
		prefix := "backtests/" + f.runID + "/"
		if err := store.Put(ctx, prefix+"report.json", body, "application/json"); err != nil {
			return err
		}
		if err := store.Put(ctx, prefix+"report.md", []byte(md), "text/markdown"); err != nil {
			return err
		}
	}
	log.Info("backtest finished", "run_id", f.runID, "trades", rep.Data.Trades, "windows", rep.Data.Windows,
		"alerts", rep.Data.Alerts, "seconds", time.Since(start).Seconds(), "report", filepath.Join(dir, "report.md"))
	return nil
}
