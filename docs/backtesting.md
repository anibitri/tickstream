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

<!-- TODO(you): add a link to your portfolio paper on walk-forward methodology here. -->

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

## The bundled fixture

`testdata/archive` is 20 minutes of synthetic data: a random walk with a few
price jumps, volume bursts and short gaps between exchanges. It has no trading
edge by design, so the signal is expected to lose roughly its costs. It exists
to test the pipeline and to pin the metrics output (golden file), not to judge
a strategy. Real results need real archived data.
