package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/anibitri/tickstream/internal/replay"
	"github.com/anibitri/tickstream/internal/rules"
)

// Options controls the evaluation.
type Options struct {
	Folds       int     // number of equal time blocks
	TrainBlocks int     // blocks used to tune before each test block
	HorizonNs   int64   // how far ahead a "true event" is measured
	EventBps    float64 // move size that counts as a true event
	Signal      SignalParams
}

// Report is written as report.json. It contains no wall-clock times, so the
// same input and options always give byte-identical output.
type Report struct {
	RunID   string     `json:"run_id"`
	From    string     `json:"from"`
	To      string     `json:"to"`
	Symbols []string   `json:"symbols"`
	Options OptionsOut `json:"options"`
	Data    DataOut    `json:"data"`
	Rules   []RuleOut  `json:"rules"`
	Signal  SignalOut  `json:"signal"`
	Notes   []string   `json:"notes"`
}

type OptionsOut struct {
	Folds          int     `json:"folds"`
	TrainBlocks    int     `json:"train_blocks"`
	HorizonSecs    float64 `json:"horizon_secs"`
	EventBps       float64 `json:"event_bps"`
	CostBpsPerFill float64 `json:"cost_bps_per_fill"`
	SignalLookback int     `json:"signal_lookback_windows"`
	SignalHoldSecs float64 `json:"signal_hold_secs"`
}

type DataOut struct {
	Trades        int    `json:"trades"`
	Windows       int    `json:"windows"`
	Alerts        int    `json:"alerts"`
	InputSHA256   string `json:"input_sha256"`
	MetricsSHA256 string `json:"metrics_sha256"`
}

type RuleFold struct {
	Fold      int       `json:"fold"`
	TestFrom  string    `json:"test_from"`
	Threshold float64   `json:"threshold"`
	TrainF1   float64   `json:"train_f1"`
	Test      Confusion `json:"test"`
}

type RuleOut struct {
	Name                string     `json:"name"`
	Condition           string     `json:"condition"`
	ConfiguredThreshold float64    `json:"configured_threshold"`
	Folds               []RuleFold `json:"folds"`
	OutOfSampleTuned    Confusion  `json:"out_of_sample_tuned"`
	OutOfSampleFixed    Confusion  `json:"out_of_sample_configured"`
}

type SignalFold struct {
	Fold        int     `json:"fold"`
	TestFrom    string  `json:"test_from"`
	KBps        float64 `json:"k_bps"` // 0 = no setting was good enough; fold not traded
	TrainSharpe float64 `json:"train_sharpe_per_trade"`
	TestTrades  int     `json:"test_trades"`
}

type SignalOut struct {
	Description string        `json:"description"`
	Folds       []SignalFold  `json:"folds"`
	OutOfSample Stats         `json:"out_of_sample"`
	Equity      []EquityPoint `json:"equity_curve"`
}

var (
	thresholdScales = []float64{0.5, 0.75, 1, 1.25, 1.5, 2, 3}
	signalGrid      = []float64{5, 10, 15, 20, 30, 50}
)

// minTrainTrades is the fewest trades a signal setting needs on the training
// blocks before we trust its Sharpe ratio.
const minTrainTrades = 5

func fmtTime(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339) }

// Evaluate runs walk-forward evaluation. The time range is cut into Folds
// equal blocks. For every block after the first TrainBlocks, parameters are
// tuned on the TrainBlocks blocks just before it and then scored on that block
// only, so no test result ever uses data that was seen during tuning.
func Evaluate(run *replay.Run, set *rules.Set, fromNs, toNs int64, o Options) (RuleResults []RuleOut, sig SignalOut) {
	block := (toNs - fromNs) / int64(o.Folds)
	blockStart := func(i int) int64 { return fromNs + int64(i)*block }
	prices := buildPrices(run.Metrics)

	for _, r := range set.Rules {
		if r.PerExch {
			continue // stale_feed is an operational rule, not a market prediction
		}
		var points []labelled
		for _, e := range run.Evals {
			if e.Rule != r.Name || !e.Defined {
				continue
			}
			move, ok := futureMoveBps(prices[e.Symbol], e.EndNs, o.HorizonNs)
			if !ok {
				continue
			}
			points = append(points, labelled{e.Symbol, e.EndNs, e.Value, move >= o.EventBps})
		}
		sort.SliceStable(points, func(i, j int) bool { return points[i].endNs < points[j].endNs })

		out := RuleOut{Name: r.Name, Condition: r.Cond.String(), ConfiguredThreshold: r.Cond.Threshold}
		for i := o.TrainBlocks; i < o.Folds; i++ {
			trainFrom, testFrom, testTo := blockStart(i-o.TrainBlocks), blockStart(i), blockStart(i+1)
			best, bestF1 := r.Cond.Threshold, -1.0
			for _, s := range thresholdScales {
				th := r.Cond.Threshold * s
				c := scoreRule(points, r, th, trainFrom, testFrom)
				c.finish()
				if c.F1 > bestF1 || (c.F1 == bestF1 && th > best) {
					best, bestF1 = th, c.F1
				}
			}
			test := scoreRule(points, r, best, testFrom, testTo)
			test.finish()
			fixed := scoreRule(points, r, r.Cond.Threshold, testFrom, testTo)
			out.OutOfSampleTuned.add(test)
			out.OutOfSampleFixed.add(fixed)
			out.Folds = append(out.Folds, RuleFold{Fold: i + 1, TestFrom: fmtTime(testFrom), Threshold: round6(best),
				TrainF1: round6(bestF1), Test: test})
		}
		out.OutOfSampleTuned.finish()
		out.OutOfSampleFixed.finish()
		RuleResults = append(RuleResults, out)
	}

	bars := buildBars(run.Metrics)
	sig.Description = fmt.Sprintf("Mean reversion on 10s windows: when the last price is at least k bps from the "+
		"average VWAP of the previous %d windows, trade against the move and exit after %s. Costs: %.1f bps per fill.",
		o.Signal.Lookback, time.Duration(o.Signal.HoldNs), o.Signal.CostBps)
	var oos []SignalTrade
	for i := o.TrainBlocks; i < o.Folds; i++ {
		trainFrom, testFrom, testTo := blockStart(i-o.TrainBlocks), blockStart(i), blockStart(i+1)
		bestK, bestSharpe := 0.0, math.Inf(-1)
		for _, k := range signalGrid {
			st, _ := computeStats(simulateSignal(bars, o.Signal, k, trainFrom, testFrom), testFrom-trainFrom)
			if st.Trades >= minTrainTrades && st.SharpePerTrade > bestSharpe {
				bestK, bestSharpe = k, st.SharpePerTrade
			}
		}
		fold := SignalFold{Fold: i + 1, TestFrom: fmtTime(testFrom), KBps: bestK}
		if bestK > 0 {
			fold.TrainSharpe = round6(bestSharpe)
			test := simulateSignal(bars, o.Signal, bestK, testFrom, testTo)
			fold.TestTrades = len(test)
			oos = append(oos, test...)
		}
		sig.Folds = append(sig.Folds, fold)
	}
	sort.SliceStable(oos, func(i, j int) bool { return oos[i].ExitNs < oos[j].ExitNs })
	sig.OutOfSample, sig.Equity = computeStats(oos, blockStart(o.Folds)-blockStart(o.TrainBlocks))
	return RuleResults, sig
}

// Markdown renders a short human-readable summary of the report.
func (r *Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Backtest %s\n\n", r.RunID)
	fmt.Fprintf(&b, "%s → %s · symbols: %s · %d trades · %d windows · %d alerts\n\n",
		r.From, r.To, strings.Join(r.Symbols, ", "), r.Data.Trades, r.Data.Windows, r.Data.Alerts)
	fmt.Fprintf(&b, "Walk-forward: %d blocks, tuned on the previous %d. A true event is a move of at least %.0f bps "+
		"within %.0fs.\n\n", r.Options.Folds, r.Options.TrainBlocks, r.Options.EventBps, r.Options.HorizonSecs)
	b.WriteString("## Alert rules (out of sample)\n\n| Rule | Precision (tuned) | Recall (tuned) | Precision (configured) | Recall (configured) |\n|---|---|---|---|---|\n")
	for _, x := range r.Rules {
		fmt.Fprintf(&b, "| %s | %.2f | %.2f | %.2f | %.2f |\n", x.Name, x.OutOfSampleTuned.Precision,
			x.OutOfSampleTuned.Recall, x.OutOfSampleFixed.Precision, x.OutOfSampleFixed.Recall)
	}
	s := r.Signal.OutOfSample
	fmt.Fprintf(&b, "\n## Signal (out of sample)\n\n%s\n\n| Trades | Hit rate | Mean (bps) | Sharpe/trade | Sharpe (ann.) | Max drawdown | Total return |\n|---|---|---|---|---|---|---|\n",
		r.Signal.Description)
	fmt.Fprintf(&b, "| %d | %.2f | %.2f | %.3f | %.2f | %.2f%% | %.2f%% |\n", s.Trades, s.HitRate, s.MeanReturnBps,
		s.SharpePerTrade, s.SharpeAnnualised, s.MaxDrawdown*100, s.TotalReturn*100)
	if len(r.Notes) > 0 {
		b.WriteString("\n## Notes\n\n")
		for _, n := range r.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
	}
	fmt.Fprintf(&b, "\nInput SHA-256: `%s`\n", r.Data.InputSHA256)
	return b.String()
}
