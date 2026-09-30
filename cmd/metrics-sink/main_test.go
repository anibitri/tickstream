package main

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/storage"
)

// fakeDB keeps items in memory and applies the "newer only" condition the way
// DynamoDB would, using the attribute named in the condition.
type fakeDB struct {
	storage.DynamoAPI
	mu       sync.Mutex
	items    map[string]map[string]ddbtypes.AttributeValue
	puts     int
	failNext int
}

func num(av ddbtypes.AttributeValue) int64 {
	n, _ := strconv.ParseInt(av.(*ddbtypes.AttributeValueMemberN).Value, 10, 64)
	return n
}

func (f *fakeDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.failNext > 0 {
		f.failNext--
		return nil, errors.New("throttled")
	}
	var key, attr string
	if *in.TableName == "latest_metrics" {
		key = in.Item["symbol"].(*ddbtypes.AttributeValueMemberS).Value + "/" + strconv.FormatInt(num(in.Item["window_secs"]), 10)
		attr = "window_end_ns"
	} else {
		key = in.Item["exchange"].(*ddbtypes.AttributeValueMemberS).Value + "/" + in.Item["symbol"].(*ddbtypes.AttributeValueMemberS).Value
		attr = "reported_at_ns"
	}
	if cur, ok := f.items[key]; ok && num(cur[attr]) >= num(in.Item[attr]) {
		return nil, &ddbtypes.ConditionalCheckFailedException{}
	}
	f.items[key] = in.Item
	return &dynamodb.PutItemOutput{}, nil
}

func newSink(db *fakeDB) *Sink {
	return &Sink{
		Metrics:      &storage.MetricsRepo{DB: db, Table: "latest_metrics"},
		Health:       &storage.FeedHealthRepo{DB: db, Table: "feed_health"},
		MetricsTopic: "md.metrics", HealthTopic: "md.feed_health", Workers: 4, Log: slog.New(slog.DiscardHandler),
	}
}

func metricsRec(t *testing.T, sym string, secs int32, end int64) *kgo.Record {
	b, err := proto.Marshal(&domain.Metrics{Symbol: sym, WindowSecs: secs, WindowEndNs: end, Vwap: "1"})
	require.NoError(t, err)
	return &kgo.Record{Topic: "md.metrics", Value: b}
}

func TestSinkWritesOnlyNewestPerKey(t *testing.T) {
	db := &fakeDB{items: map[string]map[string]ddbtypes.AttributeValue{}}
	s := newSink(db)
	hb, _ := proto.Marshal(&domain.FeedHealth{Exchange: "kraken", Symbol: "BTC-USD", ReportedAtNs: 5})
	_, err := s.Handle(context.Background(), []*kgo.Record{
		metricsRec(t, "BTC-USD", 1, 100), metricsRec(t, "BTC-USD", 1, 300), metricsRec(t, "BTC-USD", 1, 200),
		metricsRec(t, "BTC-USD", 10, 100), metricsRec(t, "ETH-USD", 1, 100),
		{Topic: "md.feed_health", Value: hb},
	})
	require.NoError(t, err)
	assert.Equal(t, 4, db.puts, "three metrics keys + one health key")
	assert.Equal(t, int64(300), num(db.items["BTC-USD/1"]["window_end_ns"]))

	// Redelivering an older window is ignored, not an error.
	_, err = s.Handle(context.Background(), []*kgo.Record{metricsRec(t, "BTC-USD", 1, 250)})
	require.NoError(t, err)
	assert.Equal(t, int64(300), num(db.items["BTC-USD/1"]["window_end_ns"]))
}

func TestSinkRetriesThenFails(t *testing.T) {
	db := &fakeDB{items: map[string]map[string]ddbtypes.AttributeValue{}, failNext: 2}
	_, err := newSink(db).Handle(context.Background(), []*kgo.Record{metricsRec(t, "BTC-USD", 1, 1)})
	require.NoError(t, err, "succeeds on the third attempt")

	db.failNext = 10
	_, err = newSink(db).Handle(context.Background(), []*kgo.Record{metricsRec(t, "BTC-USD", 1, 2)})
	assert.Error(t, err, "gives up after 4 attempts so the batch is not committed")
}
