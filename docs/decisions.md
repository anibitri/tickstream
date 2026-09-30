# Design decisions

Short records of the main choices, why they were made and what they cost.

## 1. Kafka as the backbone

**Context.** Trades arrive continuously and several services need the same
stream (metrics, archive, API). We need to replay history, keep per-symbol
order and survive a consumer crashing.

**Decision.** Use Kafka (in KRaft mode, so no ZooKeeper), with every topic
keyed by symbol.

**Why not the alternatives?**
- **SQS** deletes a message once it is processed, so there is no replay and
  no way for several services to read the same stream. It is still used for
  alert delivery, where "process once, then forget" is exactly right.
- **Kinesis** would work, but it costs money even when idle and can't run
  locally for free.
- **RabbitMQ** is a queue, not a log: replaying yesterday or adding a new
  consumer that starts from the beginning is not what it is built for.

**Cost.** Kafka is heavier to run than a queue. Keying by symbol means one
very busy symbol lands on one partition (a "hot partition"); with a handful
of symbols this is fine, with thousands you would spread them more evenly.

## 2. At-least-once delivery with idempotent writes

**Context.** A service can crash after doing its work but before telling
Kafka it's done. On restart it redoes that work. We must not lose data, and
the repeat must not create duplicates.

**Decision.** Every service commits its Kafka offset only after its output is
safely stored (at-least-once). Every place that stores output is written so
that doing the same write twice changes nothing:

| Where | How repeats are harmless |
|---|---|
| normaliser | remembers recent `(exchange, trade_id)` pairs and drops repeats |
| metrics engine | the same trades always produce the same windows; duplicates inside a window are dropped |
| archiver | files are named after the offsets they contain, so a rewrite replaces the same file |
| DynamoDB | writes only succeed if they are newer than what is stored |
| Lambda | alert IDs are a hash of rule + symbol + window, and are stored only once |

The metrics and risk engines keep state in memory (open windows, rule
history). They commit the offset of the **oldest** record still in that state,
plus a small checkpoint (the watermark) stored in the offset commit itself.
After a restart they re-read from there and rebuild exactly the same state.
Randomised tests check this for arbitrary crash points.

**Why not Kafka transactions (exactly-once)?** They would give the same
result for the Kafka-to-Kafka steps, but not for S3, DynamoDB or the webhook,
which need idempotent writes anyway. Transactions also add latency.

**Cost.** Output topics can contain repeated records after a crash. Anything
reading them has to tolerate that (all our consumers do).

## 3. Event time, not processing time

**Context.** A 10-second window can mean "trades whose exchange timestamp is
in these 10 seconds" (event time) or "trades we happened to receive in these
10 seconds" (processing time). Processing time changes with network delay,
restarts and replay speed.

**Decision.** Windows use the exchange's timestamp. A window closes when the
watermark (latest timestamp seen minus 500 ms) passes its end. Trades that
arrive after their window closed are counted in `late_trades_total` and
dropped.

**Why.** The same trades always give the same windows. That is what makes a
replay at 100× speed produce exactly the same output as the live run, and
what makes backtests trustworthy.

**Cost.** Every window is emitted at least 500 ms after it ends. A trade more
than 500 ms late is lost from the metrics (it is still archived). In live mode
an idle symbol's windows are closed by the wall clock after 2 s, so they are
not held open forever; replays never do this.

## 4. Exact decimals for prices and sizes

**Context.** Computers store floats in binary, which can't represent most
decimal numbers exactly: `0.1 + 0.2` is `0.30000000000000004`. VWAP sums
price × size over thousands of trades, and those tiny errors add up.

**Decision.** Prices and sizes stay decimal strings end to end (in Kafka, S3
and DynamoDB) and all sums use an exact decimal library
(`shopspring/decimal`). Floats are only used where the maths needs logs and
square roots (volatility), where tiny errors don't matter.

**Cost.** Decimal arithmetic is slower than float arithmetic. At our volumes
it is not the bottleneck.
