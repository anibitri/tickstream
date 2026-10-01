#!/bin/sh
# Creates the Kafka topics (run once by the kafka-init container in Compose).
# Every topic is keyed by symbol, so all trades for a symbol stay in order on
# one partition.
set -eu
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh
DAY=86400000

create() { # name partitions retention_ms
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$1" \
    --partitions "$2" --replication-factor 1 --config "retention.ms=$3"
}

create raw.trades.coinbase 6 "$DAY"
create raw.trades.kraken   6 "$DAY"
create md.trades           6 $((7 * DAY))
create md.metrics          6 $((7 * DAY))
create md.feed_health      3 "$DAY"
create risk.alerts         3 $((30 * DAY))
for svc in normaliser metrics-engine risk-engine; do
  create "dlq.$svc" 1 $((30 * DAY))
done
$KT --bootstrap-server "$BOOTSTRAP" --list
