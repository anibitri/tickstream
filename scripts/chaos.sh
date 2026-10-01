#!/usr/bin/env bash
# Chaos experiments against the running stack (`make up` first).
#
#   1. Kill a metrics-engine in the middle of a replay, restart it, and check
#      the windows it published match the in-process result exactly.
#   2. Restart the Kafka broker and time how long the pipeline takes to recover.
#   3. Cut the ingestor's network for longer than its 30s read timeout and
#      check it reconnects and raises a gap alert. (Shorter cuts don't drop the
#      websocket at all: TCP just delivers the delayed frames afterwards.)
#
# Usage: scripts/chaos.sh [1|2|3|all]
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f deploy/compose.yml --env-file .env)
RUN_ID="chaos$(date +%s)"
FROM=2026-10-04T13:50:00Z
TO=2026-10-04T14:10:00Z
KT="/opt/kafka/bin"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { # run a Go binary from the app image on the compose network
  "${COMPOSE[@]}" run --rm --no-deps -v "$PWD/testdata:/testdata:ro" -v "$PWD/deploy:/deploy:ro" \
    -e KAFKA_BROKERS=kafka:9092 --entrypoint "/app/$1" ingestor "${@:2}"
}
kafka() { "${COMPOSE[@]}" exec -T kafka "$KT/$1" --bootstrap-server kafka:9092 "${@:2}"; }

experiment_kill_metrics_engine() {
  say "1. kill metrics-engine mid-replay (run $RUN_ID)"
  kafka kafka-topics.sh --create --if-not-exists --topic "replay.$RUN_ID.metrics" --partitions 6 --replication-factor 1 >/dev/null

  # A dedicated metrics-engine reads the replay topic.
  local me="tickstream-chaos-me"
  start_engine() {
    "${COMPOSE[@]}" run -d --no-deps --name "$me" -e MODE=replay -e KAFKA_GROUP_ID="chaos-$RUN_ID" \
      -e TOPIC_TRADES="replay.$RUN_ID.trades" -e TOPIC_METRICS="replay.$RUN_ID.metrics" \
      --entrypoint /app/metrics-engine metrics-engine >/dev/null
  }

  # Replay the fixture at 20x (about a minute) and kill the engine part-way.
  app replayer -dir /testdata/archive -from "$FROM" -to "$TO" -speed 20 -run-id "$RUN_ID" >/dev/null &
  local replay_pid=$!
  sleep 5 # let the replay topic exist
  start_engine
  sleep 20
  echo "killing metrics-engine (SIGKILL)"
  docker kill "$me" >/dev/null && docker rm "$me" >/dev/null
  sleep 3
  echo "restarting it"
  start_engine
  wait "$replay_pid"
  echo "replay finished; waiting for the engine to catch up"
  for _ in $(seq 60); do
    lag=$(kafka kafka-consumer-groups.sh --describe --group "chaos-$RUN_ID" 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')
    [ "$lag" = 0 ] && break
    sleep 2
  done
  sleep 5
  docker rm -f "$me" >/dev/null

  local want got
  want=$(app backtester -dir /testdata/archive -from "$FROM" -to "$TO" -run-id "$RUN_ID-local" -rules /deploy/rules.yaml -out /tmp \
    | grep -o '"metrics_sha256":"[0-9a-f]*"' || true)
  got=$(app backtester -dir /testdata/archive -from "$FROM" -to "$TO" -run-id "$RUN_ID-kafka" -rules /deploy/rules.yaml -out /tmp \
    -kafka-metrics "replay.$RUN_ID.metrics" | grep -o '"metrics_sha256":"[0-9a-f]*"' || true)
  echo "in-process: $want"
  echo "via kafka:  $got"
  [ -n "$want" ] && [ "$want" = "$got" ] && echo "PASS: identical windows after a crash" || { echo "FAIL"; return 1; }
}

experiment_restart_kafka() {
  say "2. restart the Kafka broker"
  local start end
  start=$(date +%s)
  "${COMPOSE[@]}" restart kafka >/dev/null
  until "${COMPOSE[@]}" exec -T kafka "$KT/kafka-broker-api-versions.sh" --bootstrap-server kafka:9092 >/dev/null 2>&1; do sleep 1; done
  echo "broker back after $(( $(date +%s) - start ))s; waiting for new metrics"
  local before after
  before=$(kafka kafka-get-offsets.sh --topic md.metrics | awk -F: '{s+=$3} END {print s}')
  for _ in $(seq 120); do
    after=$(kafka kafka-get-offsets.sh --topic md.metrics | awk -F: '{s+=$3} END {print s}')
    [ "$after" -gt "$before" ] && break
    sleep 1
  done
  end=$(date +%s)
  [ "$after" -gt "$before" ] && echo "PASS: metrics flowing again $((end - start))s after the restart began" \
    || { echo "FAIL: no new metrics"; return 1; }
}

experiment_cut_websocket() {
  say "3. cut the ingestor's network for 45s"
  local ctr net before
  ctr=$("${COMPOSE[@]}" ps -q ingestor)
  net=tickstream_default
  before=$(kafka kafka-get-offsets.sh --topic risk.alerts | awk -F: '{s+=$3} END {print s}')
  docker network disconnect "$net" "$ctr"
  sleep 45
  docker network connect "$net" "$ctr"
  echo "reconnected; waiting for the gap alert"
  for _ in $(seq 60); do
    if "${COMPOSE[@]}" logs --since 90s ingestor 2>/dev/null | grep -q '"feed gap after reconnect"'; then
      echo "PASS: ingestor reconnected and logged the gap:"
      "${COMPOSE[@]}" logs --since 90s ingestor | grep '"feed gap after reconnect"' | tail -2
      echo "alerts topic grew from $before to $(kafka kafka-get-offsets.sh --topic risk.alerts | awk -F: '{s+=$3} END {print s}')"
      return 0
    fi
    sleep 2
  done
  echo "FAIL: no reconnect logged"
  return 1
}

case "${1:-all}" in
  1) experiment_kill_metrics_engine ;;
  2) experiment_restart_kafka ;;
  3) experiment_cut_websocket ;;
  all) experiment_kill_metrics_engine && experiment_restart_kafka && experiment_cut_websocket ;;
  *) echo "usage: $0 [1|2|3|all]" && exit 2 ;;
esac
