# Benchmarks

All numbers are measured, not estimated. Anything not measured yet says so.

**Machine:** Apple M2 (8 cores), 8 GB RAM, macOS 26, Go 1.27. Docker Desktop
has 6 GB of memory; the whole stack (Kafka, LocalStack, Prometheus, Grafana,
Jaeger and the services) runs on it at the same time.

## Throughput

| What | Result |
|---|---|
| Metrics engine alone, one core | **~715,000 trades/s** (1.4 µs and 68 allocations per trade) |
| Replay pipeline in one process (Parquet → ordered merge → metrics and risk engines → scoring) | **2.17M trades in 4.1 s ≈ 520,000 trades/s**, 174 MB peak memory |
| **Through Kafka**: replayer → Kafka → metrics-engine container → Kafka | **2.17M trades in 11.4 s ≈ 190,000 trades/s** end to end (target was 50,000) |

The Kafka figure is the time from starting the replay until the metrics-engine
has published every window (6,021 windows for this dataset), including the
replayer container's start-up. The replayer alone produced about 425,000
trades/s.

```bash
go test -run x -bench EngineAdd ./internal/metrics/                    # engine alone
go run ./cmd/replayer -generate -dir data/bench -minutes 30 -tps 1000  # 2.17M synthetic trades
go run ./cmd/backtester -dir data/bench -run-id bench \
  -from 2026-10-04T13:50:00Z -to 2026-10-04T14:20:00Z                  # in one process
make up && scripts/bench.sh                                            # through Kafka
```

Most of the 1.4 µs per trade is exact decimal arithmetic (VWAP sums for three
window sizes and two exchanges). Floats would be faster but inexact
(see [decisions](decisions.md#4-exact-decimals-for-prices-and-sizes)).

### More metrics-engine instances don't help here

| Instances | 1 | 2 | 3 | 6 |
|---|---|---|---|---|
| Trades/s end to end | 189,000 | 191,000 | 55,000 | 92,000 |

Two things explain this:

1. **Only three symbols means only three busy partitions.** Trades are keyed by
   symbol, so all of BTC-USD goes to one partition. With three symbols at
   most three partitions (and so three instances) ever have work. This is the
   "hot partition" trade-off in [decisions](decisions.md#1-kafka-as-the-backbone):
   scaling out needs more symbols, or a finer key.
2. **Rebalancing.** The 3- and 6-instance runs started while instances were
   still joining the consumer group, so partitions moved mid-run. These two
   numbers measure start-up noise more than steady-state throughput.

With one instance already faster than the replayer can feed it, the pipeline
is limited by producing, not by processing.

## Latency

*Not measured yet.* It needs the live feeds running for about an hour: the
Grafana panel "Receive → metric published" shows p50/p95/p99 of the time from
the ingestor receiving the trade that closes a window to that window's metrics
being published.

## Recovery (chaos experiments)

`scripts/chaos.sh` runs these against the running stack. All three passed:

| Experiment | Result |
|---|---|
| **Kill metrics-engine** with SIGKILL while replaying the fixture at 20× speed, then restart it | **Pass.** The windows it published through Kafka, with repeats removed, have exactly the same SHA-256 as the in-process result |
| **Restart the Kafka broker** | **Pass.** Broker back after 6 s; new metrics flowing 12 s after the restart began (about 6 s after the broker was back). No service crashed |
| **Cut the ingestor's network for 45 s** | **Pass.** The feed hit its 30 s read timeout, reconnected with backoff, measured a 54.9 s gap and raised a `feed_gap` alert per symbol, which reached DynamoDB through SNS → SQS → Lambda |

A 15-second network cut does not drop the websocket at all: TCP delivers the
delayed messages once the network is back, so no data is lost and no gap
alert is raised. That is the correct behaviour, so the experiment cuts for
longer than the read timeout.

The crash test also runs in CI without the full stack
(`internal/kafkax/integration_test.go`, against a real Kafka container).
