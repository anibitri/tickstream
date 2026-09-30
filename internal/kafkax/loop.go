package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/anibitri/tickstream/internal/observability"
)

// Commit is an offset to commit plus optional checkpoint metadata. Stateful
// handlers store a small checkpoint (e.g. the watermark) in the metadata field
// of the consumer-group offset commit, so offset and state move atomically.
type Commit struct {
	Offset   int64
	Metadata string
}

// Offsets maps topic -> partition -> commit.
type Offsets map[string]map[int32]Commit

// Set records the commit for a partition.
func (o Offsets) Set(topic string, partition int32, c Commit) {
	m := o[topic]
	if m == nil {
		m = make(map[int32]Commit)
		o[topic] = m
	}
	m[partition] = c
}

// Handler processes one polled batch. Records for a partition arrive in offset
// order. It returns records to publish; the loop publishes them, waits for the
// broker to acknowledge, and only then commits.
type Handler interface {
	Handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error)
}

// Committer is optionally implemented by stateful handlers that must commit a
// "low-water" offset (the oldest record still contributing to open state)
// instead of the last consumed offset. See docs/adr/0002-delivery-semantics.md.
type Committer interface {
	CommitOffsets() Offsets
}

// Rebalancer is optionally implemented by handlers that keep per-partition
// state. Assigned receives the checkpoint metadata committed for each partition.
type Rebalancer interface {
	Assigned(topic string, partitions map[int32]string)
	Revoked(topic string, partitions []int32)
}

// Ticker is optionally implemented by handlers that need periodic work even
// when no records arrive (idle watermark advancement, time-based flushes).
type Ticker interface {
	Tick(ctx context.Context) ([]*kgo.Record, error)
}

// Loop drives a consumer group: poll -> handle -> produce -> flush -> commit.
// Delivery is at-least-once: offsets are committed only after every produced
// record has been acknowledged with acks=all.
type Loop struct {
	Client      *kgo.Client
	Group       string
	Handler     Handler
	Log         *slog.Logger
	MaxRecords  int
	PollTimeout time.Duration // bounds idle polls so Tick runs (default 1s)

	mu      sync.Mutex
	pending Offsets // default commit positions (last consumed + 1)
}

// RebalanceOpts returns the kgo options that forward partition assignment
// changes to the handler. They must be passed when constructing the client.
func (l *Loop) RebalanceOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.OnPartitionsAssigned(l.onAssigned),
		kgo.OnPartitionsRevoked(l.onRevoked),
		kgo.OnPartitionsLost(l.onRevoked),
	}
}

func (l *Loop) onAssigned(ctx context.Context, cl *kgo.Client, m map[string][]int32) {
	if l.Log != nil {
		l.Log.Info("partitions assigned", "assignment", fmt.Sprint(m))
	}
	rb, ok := l.Handler.(Rebalancer)
	if !ok {
		return
	}
	meta := make(map[string]map[int32]string, len(m))
	if l.Group != "" {
		resp, err := kadm.NewClient(cl).FetchOffsets(ctx, l.Group)
		if err != nil && l.Log != nil {
			l.Log.Warn("fetch committed checkpoints", "err", err)
		}
		resp.Each(func(o kadm.OffsetResponse) {
			if o.Err == nil && o.Metadata != "" {
				if meta[o.Topic] == nil {
					meta[o.Topic] = make(map[int32]string)
				}
				meta[o.Topic][o.Partition] = o.Metadata
			}
		})
	}
	for t, ps := range m {
		pm := make(map[int32]string, len(ps))
		for _, p := range ps {
			pm[p] = meta[t][p]
		}
		rb.Assigned(t, pm)
	}
}

func (l *Loop) onRevoked(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	// Everything handled so far was committed at the end of the last batch
	// (rebalances are blocked mid-batch), so only in-memory state needs dropping.
	if rb, ok := l.Handler.(Rebalancer); ok {
		for t, ps := range m {
			rb.Revoked(t, ps)
		}
	}
	l.mu.Lock()
	for t, ps := range m {
		for _, p := range ps {
			delete(l.pending[t], p)
		}
	}
	l.mu.Unlock()
	if l.Log != nil {
		l.Log.Info("partitions revoked", "assignment", fmt.Sprint(m))
	}
}

// Run blocks until ctx is cancelled or a fatal error occurs. A handler error is
// fatal by design: the process exits without committing, and on restart the
// batch is re-consumed from the last committed offset.
func (l *Loop) Run(ctx context.Context) error {
	if l.MaxRecords <= 0 {
		l.MaxRecords = 10_000
	}
	if l.PollTimeout <= 0 {
		l.PollTimeout = time.Second
	}
	l.mu.Lock()
	l.pending = make(Offsets)
	l.mu.Unlock()
	ticker, _ := l.Handler.(Ticker)
	for {
		pollCtx, cancel := context.WithTimeout(ctx, l.PollTimeout)
		fetches := l.Client.PollRecords(pollCtx, l.MaxRecords)
		cancel()
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, p int32, err error) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			l.Log.Warn("fetch error", "topic", t, "partition", p, "err", err)
		})

		var recs []*kgo.Record
		fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
			if len(fp.Records) == 0 {
				return
			}
			last := fp.Records[len(fp.Records)-1]
			observability.ConsumerLag.WithLabelValues(fp.Topic, strconv.Itoa(int(fp.Partition))).
				Set(float64(fp.HighWatermark - last.Offset - 1))
			observability.MessagesConsumed.WithLabelValues(fp.Topic).Add(float64(len(fp.Records)))
			recs = append(recs, fp.Records...)
			l.mu.Lock()
			l.pending.Set(fp.Topic, fp.Partition, Commit{Offset: last.Offset + 1})
			l.mu.Unlock()
		})

		var out []*kgo.Record
		if len(recs) > 0 {
			o, err := l.Handler.Handle(ctx, recs)
			if err != nil {
				return fmt.Errorf("handler: %w", err)
			}
			out = o
		}
		if ticker != nil {
			o, err := ticker.Tick(ctx)
			if err != nil {
				return fmt.Errorf("tick: %w", err)
			}
			out = append(out, o...)
		}
		if len(recs) == 0 && len(out) == 0 {
			l.Client.AllowRebalance()
			continue
		}
		if err := Publish(ctx, l.Client, out); err != nil {
			return err
		}
		err := l.commit(ctx)
		l.Client.AllowRebalance()
		if err != nil {
			return err
		}
	}
}

func (l *Loop) commit(ctx context.Context) error {
	l.mu.Lock()
	offs := l.pending
	l.mu.Unlock()
	if c, ok := l.Handler.(Committer); ok {
		offs = c.CommitOffsets()
	}
	if len(offs) == 0 {
		return nil
	}
	err := CommitWithMetadata(ctx, l.Client, l.Group, offs)
	if err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

// CommitWithMetadata issues an OffsetCommit for the current group generation,
// including per-partition metadata (franz-go's high-level commit API does not
// expose the metadata field).
func CommitWithMetadata(ctx context.Context, cl *kgo.Client, group string, offs Offsets) error {
	memberID, generation := cl.GroupMetadata()
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = group
	req.MemberID = memberID
	req.Generation = generation
	for t, ps := range offs {
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = t
		for p, c := range ps {
			if c.Offset < 0 {
				continue
			}
			rp := kmsg.NewOffsetCommitRequestTopicPartition()
			rp.Partition = p
			rp.Offset = c.Offset
			rp.LeaderEpoch = -1
			if c.Metadata != "" {
				md := c.Metadata
				rp.Metadata = &md
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		if len(rt.Partitions) > 0 {
			req.Topics = append(req.Topics, rt)
		}
	}
	if len(req.Topics) == 0 {
		return nil
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerrFor(p.ErrorCode); err != nil {
				return fmt.Errorf("commit %s[%d]: %w", t.Topic, p.Partition, err)
			}
		}
	}
	return nil
}

// Publish produces recs and blocks until all are acknowledged.
func Publish(ctx context.Context, cl *kgo.Client, recs []*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	wg.Add(len(recs))
	for _, r := range recs {
		cl.Produce(ctx, r, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("produce to %s: %w", r.Topic, err)
				}
				mu.Unlock()
				return
			}
			observability.MessagesProduced.WithLabelValues(r.Topic).Inc()
		})
	}
	wg.Wait()
	return firstErr
}
