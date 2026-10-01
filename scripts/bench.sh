#!/usr/bin/env bash
# Throughput through Kafka: replay a dataset at full speed into an isolated
# topic while a metrics-engine consumes it, and time how long until the engine
# has processed everything. Needs the stack running (`make up`) and a dataset:
#
#   go run ./cmd/replayer -generate -dir data/bench -minutes 30 -tps 1000
#   scripts/bench.sh [instances]
set -euo pipefail
cd "$(dirname "$0")/.."

INSTANCES="${1:-1}"
RUN_ID="bench$(date +%s)"
FROM=2026-10-04T13:50:00Z
TO=2026-10-04T14:20:00Z
COMPOSE=(docker compose -f deploy/compose.yml --env-file .env)
kafka() { "${COMPOSE[@]}" exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server kafka:9092 "${@:2}"; }

[ -d data/bench ] || { echo "no dataset in data/bench (see the comment at the top)"; exit 1; }

# The run is done when the engines have published as many windows as the
# in-process pipeline computes for the same data. (Committed offsets can't be
# used: the engine commits the oldest trade still in an open window.)
want=$("${COMPOSE[@]}" run --rm --no-deps -v "$PWD/data:/data:ro" -v "$PWD/deploy:/deploy:ro" --entrypoint /app/backtester \
  ingestor -dir /data/bench -from "$FROM" -to "$TO" -run-id "$RUN_ID" -rules /deploy/rules.yaml -out /tmp 2>/dev/null \
  | grep -oE '"windows":[0-9]+' | cut -d: -f2)
echo "expecting $want windows"
for t in trades metrics; do
  kafka kafka-topics.sh --create --topic "replay.$RUN_ID.$t" --partitions 6 --replication-factor 1 >/dev/null
done

for i in $(seq "$INSTANCES"); do
  "${COMPOSE[@]}" run -d --no-deps --name "$RUN_ID-me$i" -e MODE=replay -e KAFKA_GROUP_ID="$RUN_ID" \
    -e TOPIC_TRADES="replay.$RUN_ID.trades" -e TOPIC_METRICS="replay.$RUN_ID.metrics" -e LOG_LEVEL=warn \
    --entrypoint /app/metrics-engine metrics-engine >/dev/null
done
sleep 3

start=$(python3 -c 'import time; print(time.time())')
"${COMPOSE[@]}" run --rm --no-deps -v "$PWD/data:/data:ro" -e KAFKA_BROKERS=kafka:9092 \
  --entrypoint /app/replayer ingestor -dir /data/bench -from "$FROM" -to "$TO" -speed max -run-id "$RUN_ID" 2>/dev/null \
  | grep '"replay finished"' | grep -oE '"trades":[0-9]+|"per_sec":[0-9]+' | tr '\n' ' '
echo "(replayer)"

total=$(kafka kafka-get-offsets.sh --topic "replay.$RUN_ID.trades" | awk -F: '{s+=$3} END {print s}')
until [ "$(kafka kafka-get-offsets.sh --topic "replay.$RUN_ID.metrics" | awk -F: '{s+=$3} END {print s}')" -ge "$want" ]; do
  sleep 0.2
done
end=$(python3 -c 'import time; print(time.time())')

python3 - "$start" "$end" "$total" "$INSTANCES" <<'EOF'
import sys
start, end, total, n = float(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
secs = end - start
print(f"{total:,} trades through Kafka and {n} metrics-engine instance(s) in {secs:.1f}s = {total/secs:,.0f} trades/s end to end")
EOF
docker rm -f $(docker ps -aq --filter "name=$RUN_ID-me") >/dev/null
