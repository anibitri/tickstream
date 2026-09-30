//go:build integration

// Integration test against a real Kafka broker (testcontainers).
// Run with: go test -tags integration ./internal/kafkax/
package kafkax_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/metrics"
	"github.com/anibitri/tickstream/internal/replay"
	"github.com/anibitri/tickstream/internal/rules"
	"github.com/anibitri/tickstream/internal/storage"
)

var fixtureFrom, fixtureTo = time.Date(2026, 10, 4, 13, 50, 0, 0, time.UTC), time.Date(2026, 10, 4, 14, 10, 0, 0, time.UTC)

// startKafka runs the same apache/kafka image as Compose, in KRaft mode, on a
// fixed free host port so the advertised address is known up front.
func startKafka(t *testing.T) config.Kafka {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	portSpec := fmt.Sprintf("%d/tcp", port)

	ctr, err := testcontainers.Run(context.Background(), "apache/kafka:4.3.1",
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                fmt.Sprintf("PLAINTEXT://:%d,CONTROLLER://:9093", port),
			"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("PLAINTEXT://localhost:%d", port),
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		}),
		testcontainers.WithExposedPorts(portSpec),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			p, _ := network.ParsePort(portSpec)
			hc.PortBindings = network.PortMap{p: {{HostPort: strconv.Itoa(port)}}}
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	return config.Kafka{Brokers: []string{fmt.Sprintf("localhost:%d", port)}}
}

func createTopics(t *testing.T, cl *kgo.Client, topics ...string) {
	t.Helper()
	_, err := kadm.NewClient(cl).CreateTopics(context.Background(), 6, 1, nil, topics...)
	require.NoError(t, err)
}

// readAll returns every record currently in topic.
func readAll(kc config.Kafka, topic string) ([]*domain.Metrics, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kafkax.NewClient(kc, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	left := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			left[o.Partition] = o.Offset
		}
	})
	var out []*domain.Metrics
	for len(left) > 0 {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("read %s: %w (partitions left: %v)", topic, err, left)
		}
		fs.EachRecord(func(r *kgo.Record) {
			var m domain.Metrics
			if proto.Unmarshal(r.Value, &m) == nil {
				out = append(out, &m)
			}
			if r.Offset+1 >= left[r.Partition] {
				delete(left, r.Partition)
			}
		})
	}
	return out, nil
}

// crashing makes the metrics handler fail after `limit` records, like a
// process dying after doing work but before committing it.
type crashing struct {
	*metrics.Handler
	seen, limit int
}

func (c *crashing) Handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
	c.seen += len(recs)
	if c.limit > 0 && c.seen > c.limit {
		return nil, errors.New("simulated crash")
	}
	return c.Handler.Handle(ctx, recs)
}

func runEngine(ctx context.Context, kc config.Kafka, limit int, log *slog.Logger) error {
	h := &crashing{Handler: &metrics.Handler{Cfg: metrics.DefaultConfig(), InTopic: "it.trades", OutTopic: "it.metrics",
		DLQTopic: "it.dlq", Log: log}, limit: limit}
	loop := &kafkax.Loop{Handler: h, Group: "it-metrics-engine", Log: log, MaxRecords: 500,
		PollTimeout: 200 * time.Millisecond}
	cl, err := kafkax.NewClient(kc, append(kafkax.ConsumerOpts(loop.Group, "it.trades"), loop.RebalanceOpts()...)...)
	if err != nil {
		return err
	}
	defer cl.Close()
	loop.Client = cl
	return loop.Run(ctx)
}

// TestMetricsEngineCrashAndRestart replays the fixture through real Kafka,
// kills the metrics engine part-way, restarts it, and checks that the windows
// it published (after removing repeats) are exactly those computed in-process.
func TestMetricsEngineCrashAndRestart(t *testing.T) {
	kc := startKafka(t)
	ctx := context.Background()
	cl, err := kafkax.NewClient(kc)
	require.NoError(t, err)
	defer cl.Close()
	createTopics(t, cl, "it.trades", "it.metrics", "it.dlq")

	store := &storage.DirStore{Root: "../../testdata/archive"}
	set, err := rules.Load("../../deploy/rules.yaml")
	require.NoError(t, err)
	src, err := replay.Open(ctx, store, nil, fixtureFrom, fixtureTo)
	require.NoError(t, err)
	want, err := replay.RunPipeline(src, metrics.DefaultConfig(), set)
	require.NoError(t, err)

	src, err = replay.Open(ctx, store, nil, fixtureFrom, fixtureTo)
	require.NoError(t, err)
	var recs []*kgo.Record
	for {
		tr, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		r, err := kafkax.ProtoRecord("it.trades", tr.Symbol, tr)
		require.NoError(t, err)
		recs = append(recs, r)
	}
	require.NoError(t, kafkax.Publish(ctx, cl, recs))

	log := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// First run dies after roughly 40% of the input.
	err = runEngine(ctx, kc, len(recs)*2/5, log)
	require.ErrorContains(t, err, "simulated crash")

	// Second run resumes from the committed checkpoint and finishes.
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runEngine(runCtx, kc, 0, log) }()
	var got *replay.Run
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("restarted engine stopped early: %v", err)
		case <-time.After(time.Second):
		}
		ms, err := readAll(kc, "it.metrics")
		if err != nil {
			t.Logf("read: %v", err)
			continue
		}
		if got = replay.RunFromMetrics(ms, set); len(got.Metrics) >= len(want.Metrics) {
			break
		}
	}
	stop()
	<-done
	require.NotNil(t, got)

	require.Equal(t, len(want.Metrics), len(got.Metrics))
	require.Equal(t, want.MetricsSHA256, got.MetricsSHA256, "restarted engine produced different windows")
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }
