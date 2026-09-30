// Package kafkax wraps franz-go with the conventions every Tickstream service
// follows: idempotent producers with acks=all, manual commits after side
// effects, OpenTelemetry context in record headers, and dead-letter helpers.
package kafkax

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/domain"
)

// Header keys set on Tickstream records.
const (
	HeaderRecvTimeNs = "recv_time_ns"
	HeaderExchange   = "exchange"
	HeaderSchema     = "schema"
)

// ProducerOpts returns the producer defaults: idempotence is on by default in
// franz-go; acks=all is set explicitly for clarity.
func ProducerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(2 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.ProducerBatchMaxBytes(1 << 20),
		kgo.MaxBufferedRecords(100_000),
	}
}

// NewClient builds a client from the shared Kafka config plus extra options.
func NewClient(cfg config.Kafka, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	if cfg.ClientID != "" {
		base = append(base, kgo.ClientID(cfg.ClientID))
	}
	base = append(base, ProducerOpts()...)
	return kgo.NewClient(append(base, opts...)...)
}

// ConsumerOpts configures a group consumer that never auto-commits and blocks
// rebalances while a polled batch is being processed.
func ConsumerOpts(group string, topics ...string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100 * time.Millisecond),
		kgo.FetchMaxBytes(16 << 20),
		kgo.SessionTimeout(10 * time.Second),
		kgo.HeartbeatInterval(1 * time.Second),
		kgo.RebalanceTimeout(20 * time.Second),
	}
}

// headerCarrier adapts record headers to an OpenTelemetry TextMapCarrier.
type headerCarrier struct{ r *kgo.Record }

func (c headerCarrier) Get(key string) string {
	for _, h := range c.r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range c.r.Headers {
		if h.Key == key {
			c.r.Headers[i].Value = []byte(value)
			return
		}
	}
	c.r.Headers = append(c.r.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	out := make([]string, 0, len(c.r.Headers))
	for _, h := range c.r.Headers {
		out = append(out, h.Key)
	}
	return out
}

var _ propagation.TextMapCarrier = headerCarrier{}

// Inject writes the trace context from ctx into r's headers.
func Inject(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{r})
}

// Extract returns ctx enriched with the trace context carried by r.
func Extract(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{r})
}

// Header returns the value of header key, or "".
func Header(r *kgo.Record, key string) string { return headerCarrier{r}.Get(key) }

// SetHeader sets header key on r.
func SetHeader(r *kgo.Record, key, value string) { headerCarrier{r}.Set(key, value) }

// HeaderInt64 parses an int64 header, returning 0 if absent or malformed.
func HeaderInt64(r *kgo.Record, key string) int64 {
	v, _ := strconv.ParseInt(Header(r, key), 10, 64)
	return v
}

// ProtoRecord marshals msg into a record for topic keyed by key. The record
// timestamp is set to tsNs (event time) so Kafka time indexes follow event time.
func ProtoRecord(topic, key string, msg proto.Message, tsNs int64) (*kgo.Record, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal %T: %w", msg, err)
	}
	r := &kgo.Record{Topic: topic, Key: []byte(key), Value: b}
	if tsNs > 0 {
		r.Timestamp = time.Unix(0, tsNs)
	}
	return r, nil
}

// DeadLetterRecord wraps a failed record with the error that rejected it.
func DeadLetterRecord(topic, service string, src *kgo.Record, cause error, nowNs int64) *kgo.Record {
	dl := &domain.DeadLetter{
		SourceTopic:     src.Topic,
		SourcePartition: src.Partition,
		SourceOffset:    src.Offset,
		Error:           cause.Error(),
		Payload:         src.Value,
		FailedAtNs:      nowNs,
		Service:         service,
	}
	b, _ := proto.Marshal(dl)
	r := &kgo.Record{Topic: topic, Key: src.Key, Value: b}
	r.Headers = append(r.Headers, kgo.RecordHeader{Key: "error", Value: []byte(cause.Error())})
	return r
}

func kerrFor(code int16) error { return kerr.ErrorForCode(code) }
