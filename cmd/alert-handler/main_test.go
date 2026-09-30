package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anibitri/tickstream/internal/storage"
)

// fakeAlerts applies attribute_not_exists(alert_id) like DynamoDB.
type fakeAlerts struct {
	storage.DynamoAPI
	mu   sync.Mutex
	seen map[string]bool
}

func (f *fakeAlerts) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := in.Item["alert_id"].(*ddbtypes.AttributeValueMemberS).Value
	if f.seen[id] {
		return nil, &ddbtypes.ConditionalCheckFailedException{}
	}
	f.seen[id] = true
	return &dynamodb.PutItemOutput{}, nil
}

const alertJSON = `{"alert_id":"abc123","rule_name":"price_jump","symbol":"BTC-USD","severity":"WARN","window_end_ns":"1791118810000000000","message":"price_jump BTC-USD: z = 5.10 > 4","values":{"value":5.1}}`

func TestHandlerStoresOnceAndPostsWebhookOnce(t *testing.T) {
	var posts []map[string]string
	var mu sync.Mutex
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]string
		_ = json.Unmarshal(b, &m)
		mu.Lock()
		posts = append(posts, m)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer hook.Close()

	db := &fakeAlerts{seen: map[string]bool{}}
	h := &Handler{Alerts: &storage.AlertsRepo{DB: db, Table: "alerts"}, WebhookURL: hook.URL, HTTP: hook.Client(),
		Log: slog.New(slog.DiscardHandler)}

	envelope, _ := json.Marshal(map[string]string{"Type": "Notification", "Message": alertJSON})
	resp, err := h.Handle(context.Background(), events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: "m1", Body: alertJSON},
		{MessageId: "m2", Body: string(envelope)}, // same alert redelivered, wrapped by SNS
		{MessageId: "m3", Body: "not json"},
	}})
	require.NoError(t, err)
	require.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "m3", resp.BatchItemFailures[0].ItemIdentifier, "only the bad message is retried")
	require.Len(t, posts, 1, "duplicate does not notify twice")
	assert.Equal(t, "[WARN] price_jump BTC-USD: z = 5.10 > 4", posts[0]["content"])
	assert.Equal(t, posts[0]["content"], posts[0]["text"])
}

func TestDecodeAlertRejectsIncompleteAlerts(t *testing.T) {
	_, err := decodeAlert(`{"rule_name":"x"}`)
	assert.Error(t, err)
	a, err := decodeAlert(alertJSON)
	require.NoError(t, err)
	assert.Equal(t, int64(1791118810000000000), a.WindowEndNs)
}
