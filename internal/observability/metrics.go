package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Latency buckets from 0.5 ms to ~16 s.
var latencyBuckets = prometheus.ExponentialBuckets(0.0005, 2, 16)

// The metric set shared by all services (see spec §12). Every binary registers
// the full set; unused series simply stay at zero.
var (
	MessagesConsumed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_consumed_total", Help: "Kafka records consumed.",
	}, []string{"topic"})
	MessagesProduced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_produced_total", Help: "Kafka records produced.",
	}, []string{"topic"})
	ProcessingLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "processing_latency_seconds", Help: "Ingestor receive time to record published by this stage.",
		Buckets: latencyBuckets,
	})
	EndToEndLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "end_to_end_latency_seconds", Help: "Exchange event time (window end for metrics) to publish.",
		Buckets: latencyBuckets,
	})
	ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_lag", Help: "High watermark minus next offset to consume.",
	}, []string{"topic", "partition"})
	LateTrades = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "late_trades_total", Help: "Trades dropped because their window had already closed.",
	})
	DLQMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dlq_messages_total", Help: "Messages sent to a dead-letter topic.",
	}, []string{"reason"})
	DuplicatesDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "duplicates_dropped_total", Help: "Duplicate records neutralised.",
	}, []string{"stage"})
	WSReconnects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_reconnects_total", Help: "Exchange websocket reconnects.",
	}, []string{"exchange"})
	WSClientDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ws_client_drops_total", Help: "Metrics messages dropped for slow dashboard clients.",
	})
	S3WriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "s3_write_duration_seconds", Help: "Duration of S3 PutObject calls.",
		Buckets: latencyBuckets,
	})
	DynamoDBWriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dynamodb_write_errors_total", Help: "Failed DynamoDB writes (excluding expected conditional-check failures).",
	}, []string{"table"})
	DynamoDBStaleWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dynamodb_stale_writes_total", Help: "Writes rejected by a condition because newer or identical data was already stored.",
	}, []string{"table"})
	FeedLastMessageAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "feed_last_message_age_seconds", Help: "Seconds since the last websocket message per exchange.",
	}, []string{"exchange"})
	FeedConnected = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "feed_connected", Help: "1 if the exchange websocket is connected.",
	}, []string{"exchange"})
	FeedGapSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "feed_gap_seconds", Help: "Length of data gaps caused by websocket disconnects.",
		Buckets: prometheus.ExponentialBuckets(0.25, 2, 12),
	}, []string{"exchange"})
	TradesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trades_received_total", Help: "Trades received per exchange and symbol.",
	}, []string{"exchange", "symbol"})
	WindowsEmitted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "windows_emitted_total", Help: "Metric windows emitted.",
	}, []string{"window_secs"})
	AlertsFired = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alerts_fired_total", Help: "Alerts emitted by the risk engine.",
	}, []string{"rule", "severity"})
	LastVWAP = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vwap", Help: "Latest VWAP per symbol and window size.",
	}, []string{"symbol", "window_secs"})
	LastSpreadBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "xex_spread_bps", Help: "Latest cross-exchange spread per symbol (1s windows).",
	}, []string{"symbol"})
	WSClients = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ws_clients", Help: "Connected dashboard websocket clients.",
	})
)

var registerOnce sync.Map

// Register adds the shared metric set to reg (idempotent per registry).
func Register(reg *prometheus.Registry) {
	if _, loaded := registerOnce.LoadOrStore(reg, true); loaded {
		return
	}
	reg.MustRegister(MessagesConsumed, MessagesProduced, ProcessingLatency, EndToEndLatency,
		ConsumerLag, LateTrades, DLQMessages, DuplicatesDropped, WSReconnects, WSClientDrops,
		S3WriteDuration, DynamoDBWriteErrors, DynamoDBStaleWrites, FeedLastMessageAge, FeedConnected,
		FeedGapSeconds, TradesReceived, WindowsEmitted, AlertsFired, LastVWAP, LastSpreadBps, WSClients)
}
