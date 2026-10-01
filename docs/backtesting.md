# How backtesting works

The backtester answers two questions about past data: *would the alert rules
have been useful?* and *would a simple trading signal have made money after
costs?* It is built to avoid the usual ways backtests fool you.

## 1. Same code as production

Archived trades are read back in exact timestamp order and pushed through the
same metrics and risk engine code the live services run, split into the same 6
partitions Kafka would use. Because windows depend only on exchange
timestamps (see [decisions](decisions.md#3-event-time-not-processing-time)),
the output matches what the live services compute from the same trades, and
running the same backtest twice gives a byte-identical report. CI checks this
on every push.

## 2. Walk-forward testing

The same method (tune on a rolling window, score only on the period that
follows, charge transaction costs) is used in my paper *Portfolio
Construction: A Comparative Study*
([code](https://github.com/anibitri/portfolio_construction)), which compares
five allocation strategies on the Ken French 49-industry portfolios from 1995
to 2026.

Tuning a threshold on a whole dataset and then reporting results on that same
dataset flatters it. Instead the time range is cut into equal blocks:

```
block:   1       2       3       4       5
fold 3:  [ tune  tune ]  test
fold 4:          [ tune  tune ]  test
fold 5:                  [ tune  tune ]  test
```

For each fold, settings are chosen on the two blocks before it and then scored
on the next block only. Every number in the report comes from test blocks, so
no result uses data that was seen while tuning.

## 3. Scoring the alert rules

An alert is useful if something actually happened. A **true event** is a price
move of at least 20 bps (0.2%) in the 60 seconds after the window. Future
prices are only used to label outcomes, never as an input to a rule.

For each rule the report gives **precision** (what fraction of alerts were
followed by a true event) and **recall** (what fraction of true events had an
alert). It shows both the tuned threshold and the threshold in
`deploy/rules.yaml`, so you can see whether tuning helped out of sample.

The `stale_feed` rule is about connections, not prices, so it is not scored.

## 4. Scoring the signal

The signal is deliberately simple: if the last price in a 10s window is at
least *k* bps away from the average VWAP of the previous six windows, bet on it
moving back (sell above, buy below) and exit 60 seconds later.

- The baseline uses **previous windows only**, never the current one.
- Trades fill at the window's closing price and pay **5 bps per fill** (10 bps
  per round trip) by default.
- One open position per symbol at a time.

The report gives the number of trades, hit rate, average return, Sharpe ratio
(per trade, and annualised when the range is at least a day), maximum
drawdown and the equity curve.

## 5. Pitfalls this avoids, and ones it can't

| Pitfall | How it's handled |
|---|---|
| **Lookahead bias**: using information from the future | Baselines use past windows only; future prices only label outcomes |
| **Overfitting**: tuning to noise | Walk-forward: tune on one period, score on the next |
| **Ignoring costs** | Costs are charged on every fill |
| **Selection bias**: picking the symbols or days that look good | Not solved automatically. Choose the date range *before* looking at results and report every run, not just the best one |

## Results on one hour of live data

The backtester was run on the hour the stack archived from the live feeds
(1 October 2026, 14:45–15:45 UTC: 73,631 trades, 10,398 windows, 49 alerts).

```bash
backtester -from 2026-10-01T14:45:00Z -to 2026-10-01T15:45:00Z -run-id live-1h
```

| Rule | Precision | Recall | What happened |
|---|---|---|---|
| price_jump | 5% | 10% | Tuning chose the lowest threshold (z > 2): 37 alerts, 2 of them before a 20 bps move |
| volume_spike | 0% | 0% | 27 alerts, none followed by a 20 bps move |
| cross_exchange_divergence | – | 0% | The two exchanges never differed by more than 25 bps, so it never fired |

| Signal | Trades | Hit rate | Mean return | Max drawdown |
|---|---|---|---|---|
| Mean reversion, 5 bps costs per fill | 28 | 0% | −13.3 bps | 3.7% |

**How to read this.** One calm hour is far too little data to judge anything:
only 21 windows were followed by a 20 bps move. Even so, two things are clear
and expected. Volume spikes on their own say nothing about the next price move.
And the signal's typical gross move is a few basis points, smaller than its 10
bps round-trip cost, so it loses on every trade. The value of the exercise is
that the whole loop (live feeds → archive → replay → walk-forward scores) works
on real data, and gives the same answer every time it is run.

## The bundled fixture

`testdata/archive` is 20 minutes of synthetic data: a random walk with a few
price jumps, volume bursts and short gaps between exchanges. It has no trading
edge by design, so the signal is expected to lose roughly its costs. It exists
to test the pipeline and to pin the metrics output (golden file), not to judge
a strategy. Real results need real archived data.
