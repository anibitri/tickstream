package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/exchange"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
)

// Handler turns raw exchange messages (raw.trades.*) into canonical trades on
// md.trades. Messages that fail to parse or validate go to dlq.normaliser.
type Handler struct {
	RawPrefix    string          // "raw.trades."
	TradesTopic  string          // "md.trades"
	DLQTopic     string          // "dlq.normaliser"
	Allowed      map[string]bool // configured symbols (nil = allow any valid symbol)
	Dedup        *DedupSet
	Tracer       trace.Tracer
	Log          *slog.Logger
	Now          func() time.Time
	ServiceLabel string
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// Handle parses every raw record. Unparseable or invalid input is dead-lettered
// with the reason attached rather than failing the batch.
func (h *Handler) Handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
	out := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		trades, err := h.parse(r)
		if err != nil {
			reason := reasonOf(err)
			observability.DLQMessages.WithLabelValues(reason).Inc()
			h.Log.Warn("dead-lettering raw record", "topic", r.Topic, "partition", r.Partition,
				"offset", r.Offset, "err", err)
			out = append(out, kafkax.DeadLetterRecord(h.DLQTopic, "normaliser", r, err, h.now().UnixNano()))
			continue
		}
		parent := kafkax.Extract(ctx, r)
		for _, t := range trades {
			if h.Dedup.Seen(domain.DedupKey(t.Exchange, t.TradeId)) {
				observability.DuplicatesDropped.WithLabelValues("normaliser").Inc()
				continue
			}
			rec, err := kafkax.ProtoRecord(h.TradesTopic, t.Symbol, t)
			if err != nil {
				return nil, err
			}
			kafkax.SetHeader(rec, kafkax.HeaderRecvTimeNs, strconv.FormatInt(t.RecvTimeNs, 10))
			if h.Tracer != nil && trace.SpanContextFromContext(parent).IsSampled() {
				spanCtx, span := h.Tracer.Start(parent, "normalise "+t.Exchange,
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(attribute.String("symbol", t.Symbol), attribute.String("trade_id", t.TradeId),
						attribute.Int64("partition", int64(r.Partition)), attribute.Int64("offset", r.Offset)))
				kafkax.Inject(spanCtx, rec)
				span.End()
			}
			if t.RecvTimeNs > 0 {
				observability.ProcessingLatency.Observe(float64(h.now().UnixNano()-t.RecvTimeNs) / 1e9)
			}
			out = append(out, rec)
		}
	}
	return out, nil
}

func (h *Handler) parse(r *kgo.Record) ([]*domain.Trade, error) {
	name := strings.TrimPrefix(r.Topic, h.RawPrefix)
	if hdr := kafkax.Header(r, kafkax.HeaderExchange); hdr != "" {
		name = hdr
	}
	ad, err := exchange.Get(name)
	if err != nil {
		return nil, &parseError{"unknown_exchange", err}
	}
	raws, err := ad.Parse(r.Value)
	if err != nil {
		return nil, &parseError{"parse", err}
	}
	recv := kafkax.HeaderInt64(r, kafkax.HeaderRecvTimeNs)
	out := make([]*domain.Trade, 0, len(raws))
	for _, rt := range raws {
		t, err := exchange.ToCanonical(name, rt, recv, h.Allowed)
		if err != nil {
			return nil, &parseError{"validation", fmt.Errorf("trade %s: %w", rt.TradeID, err)}
		}
		out = append(out, t)
	}
	return out, nil
}

type parseError struct {
	reason string
	err    error
}

func (e *parseError) Error() string { return e.reason + ": " + e.err.Error() }
func (e *parseError) Unwrap() error { return e.err }

func reasonOf(err error) string {
	var pe *parseError
	if errors.As(err, &pe) {
		return pe.reason
	}
	return "other"
}

// DedupSet remembers the last Capacity trade keys. When full it forgets the
// oldest, so memory stays bounded. Trades arrive roughly in time order, so
// this works like "have we seen this trade recently?".
type DedupSet struct {
	capacity int
	keys     map[string]struct{}
	ring     []string
	next     int
}

// NewDedupSet returns a set holding at most capacity keys.
func NewDedupSet(capacity int) *DedupSet {
	capacity = max(capacity, 1)
	return &DedupSet{capacity: capacity, keys: make(map[string]struct{}, capacity), ring: make([]string, capacity)}
}

// Seen reports whether key was already in the set, and adds it if not.
func (s *DedupSet) Seen(key string) bool {
	if _, ok := s.keys[key]; ok {
		return true
	}
	if old := s.ring[s.next]; old != "" {
		delete(s.keys, old)
	}
	s.ring[s.next] = key
	s.keys[key] = struct{}{}
	s.next = (s.next + 1) % s.capacity
	return false
}

// Len is the number of keys currently remembered.
func (s *DedupSet) Len() int { return len(s.keys) }
