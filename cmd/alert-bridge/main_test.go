package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/domain"
)

type fakeSNS struct {
	calls    [][]snstypes.PublishBatchRequestEntry
	failIDs  map[string]int // entry id -> times to fail
	errTimes int
}

func (f *fakeSNS) PublishBatch(_ context.Context, in *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	f.calls = append(f.calls, in.PublishBatchRequestEntries)
	if f.errTimes > 0 {
		f.errTimes--
		return nil, errors.New("connection reset")
	}
	out := &sns.PublishBatchOutput{}
	for _, e := range in.PublishBatchRequestEntries {
		if f.failIDs[aws.ToString(e.Id)] > 0 {
			f.failIDs[aws.ToString(e.Id)]--
			out.Failed = append(out.Failed, snstypes.BatchResultErrorEntry{Id: e.Id, Message: aws.String("throttled")})
		}
	}
	return out, nil
}

func alerts(t *testing.T, n int) []*kgo.Record {
	var recs []*kgo.Record
	for i := range n {
		b, err := proto.Marshal(&domain.Alert{AlertId: "id", RuleName: "price_jump", Symbol: "BTC-USD",
			Severity: domain.SeverityWarn, WindowEndNs: int64(i)})
		require.NoError(t, err)
		recs = append(recs, &kgo.Record{Value: b, Offset: int64(i)})
	}
	return recs
}

func TestBridgeBatchesInTens(t *testing.T) {
	f := &fakeSNS{}
	b := &Bridge{SNS: f, TopicARN: "arn", Log: slog.New(slog.DiscardHandler)}
	_, err := b.Handle(context.Background(), alerts(t, 23))
	require.NoError(t, err)
	require.Len(t, f.calls, 3)
	assert.Len(t, f.calls[0], 10)
	assert.Len(t, f.calls[2], 3)

	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(f.calls[0][0].Message)), &body))
	assert.Equal(t, "price_jump", body["rule_name"], "JSON uses the proto field names")
	assert.Equal(t, "WARN", aws.ToString(f.calls[0][0].MessageAttributes["severity"].StringValue))
}

func TestBridgeRetriesOnlyFailedEntries(t *testing.T) {
	f := &fakeSNS{failIDs: map[string]int{"e1": 1}, errTimes: 1}
	b := &Bridge{SNS: f, TopicARN: "arn", Log: slog.New(slog.DiscardHandler)}
	_, err := b.Handle(context.Background(), alerts(t, 3))
	require.NoError(t, err)
	require.Len(t, f.calls, 3, "network error, then e1 fails, then e1 alone succeeds")
	assert.Len(t, f.calls[2], 1)
	assert.Equal(t, "e1", aws.ToString(f.calls[2][0].Id))
}

func TestBridgeGivesUp(t *testing.T) {
	f := &fakeSNS{errTimes: 100}
	b := &Bridge{SNS: f, TopicARN: "arn", Log: slog.New(slog.DiscardHandler)}
	_, err := b.Handle(context.Background(), alerts(t, 1))
	assert.Error(t, err)
	assert.Len(t, f.calls, 5)
}
