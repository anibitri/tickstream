package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/observability"
)

// NewDynamoDB returns a DynamoDB client.
func NewDynamoDB(ac aws.Config) *dynamodb.Client { return dynamodb.NewFromConfig(ac) }

// DynamoAPI is the subset of the DynamoDB client used here (mockable in tests).
type DynamoAPI interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, in *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// IsConditionFailed reports whether err is a DynamoDB conditional-check failure.
func IsConditionFailed(err error) bool {
	var ccf *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

// ExchangeItem is the per-exchange breakdown stored with latest metrics.
type ExchangeItem struct {
	Exchange    string `dynamodbav:"exchange" json:"exchange"`
	VWAP        string `dynamodbav:"vwap" json:"vwap"`
	Volume      string `dynamodbav:"volume" json:"volume"`
	TradeCount  int64  `dynamodbav:"trade_count" json:"trade_count"`
	LastTradeNs int64  `dynamodbav:"last_trade_ns" json:"last_trade_ns"`
}

// MetricsItem is a row of latest_metrics (PK symbol, SK window_secs).
type MetricsItem struct {
	Symbol        string         `dynamodbav:"symbol" json:"symbol"`
	WindowSecs    int32          `dynamodbav:"window_secs" json:"window_secs"`
	WindowStartNs int64          `dynamodbav:"window_start_ns" json:"window_start_ns"`
	WindowEndNs   int64          `dynamodbav:"window_end_ns" json:"window_end_ns"`
	VWAP          string         `dynamodbav:"vwap" json:"vwap"`
	Volume        string         `dynamodbav:"volume" json:"volume"`
	TradeCount    int64          `dynamodbav:"trade_count" json:"trade_count"`
	RealisedVol   float64        `dynamodbav:"realised_vol" json:"realised_vol"`
	High          string         `dynamodbav:"high" json:"high"`
	Low           string         `dynamodbav:"low" json:"low"`
	LastPrice     string         `dynamodbav:"last_price" json:"last_price"`
	XexSpreadBps  float64        `dynamodbav:"xex_spread_bps" json:"xex_spread_bps"`
	EmittedAtNs   int64          `dynamodbav:"emitted_at_ns" json:"emitted_at_ns"`
	Exchanges     []ExchangeItem `dynamodbav:"exchanges" json:"exchanges"`
}

// MetricsItemFrom converts a Metrics message.
func MetricsItemFrom(m *domain.Metrics) MetricsItem {
	it := MetricsItem{Symbol: m.Symbol, WindowSecs: m.WindowSecs, WindowStartNs: m.WindowStartNs, WindowEndNs: m.WindowEndNs,
		VWAP: m.Vwap, Volume: m.Volume, TradeCount: m.TradeCount, RealisedVol: m.RealisedVol, High: m.High, Low: m.Low,
		LastPrice: m.LastPrice, XexSpreadBps: m.XexSpreadBps, EmittedAtNs: m.EmittedAtNs,
		Exchanges: make([]ExchangeItem, 0, len(m.Exchanges))}
	for _, x := range m.Exchanges {
		it.Exchanges = append(it.Exchanges, ExchangeItem{Exchange: x.Exchange, VWAP: x.Vwap, Volume: x.Volume,
			TradeCount: x.TradeCount, LastTradeNs: x.LastTradeNs})
	}
	return it
}

// MetricsRepo reads and writes latest_metrics.
type MetricsRepo struct {
	DB    DynamoAPI
	Table string
}

// PutLatest stores m unless a newer (or the same) window is already stored.
// The condition makes the write idempotent and ignores stale/out-of-order
// updates. It reports whether the item was written.
func (r *MetricsRepo) PutLatest(ctx context.Context, m *domain.Metrics) (bool, error) {
	av, err := attributevalue.MarshalMap(MetricsItemFrom(m))
	if err != nil {
		return false, err
	}
	_, err = r.DB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(r.Table),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(window_end_ns) OR window_end_ns < :new"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":new": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(m.WindowEndNs, 10)},
		},
	})
	if IsConditionFailed(err) {
		observability.DynamoDBStaleWrites.WithLabelValues(r.Table).Inc()
		return false, nil
	}
	if err != nil {
		observability.DynamoDBWriteErrors.WithLabelValues(r.Table).Inc()
		return false, fmt.Errorf("put latest metrics: %w", err)
	}
	return true, nil
}

// GetLatest returns the latest metrics for (symbol, windowSecs).
func (r *MetricsRepo) GetLatest(ctx context.Context, symbol string, windowSecs int32) (*MetricsItem, error) {
	out, err := r.DB.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.Table),
		Key: map[string]ddbtypes.AttributeValue{
			"symbol":      &ddbtypes.AttributeValueMemberS{Value: symbol},
			"window_secs": &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(int(windowSecs))},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get latest metrics: %w", err)
	}
	if len(out.Item) == 0 {
		return nil, ErrNotFound
	}
	var it MetricsItem
	if err := attributevalue.UnmarshalMap(out.Item, &it); err != nil {
		return nil, err
	}
	return &it, nil
}

// AlertItem is a row of the alerts table (PK symbol, SK window_end_ns#alert_id).
type AlertItem struct {
	Symbol      string             `dynamodbav:"symbol" json:"symbol"`
	SK          string             `dynamodbav:"sk" json:"-"`
	AlertID     string             `dynamodbav:"alert_id" json:"alert_id"`
	RuleName    string             `dynamodbav:"rule_name" json:"rule_name"`
	Severity    string             `dynamodbav:"severity" json:"severity"`
	WindowEndNs int64              `dynamodbav:"window_end_ns" json:"window_end_ns"`
	Exchange    string             `dynamodbav:"exchange,omitempty" json:"exchange,omitempty"`
	Message     string             `dynamodbav:"message" json:"message"`
	Values      map[string]float64 `dynamodbav:"values" json:"values"`
	ExpiresAt   int64              `dynamodbav:"expires_at" json:"-"` // TTL attribute (epoch seconds)
}

// AlertTTL is how long alerts are retained.
const AlertTTL = 30 * 24 * time.Hour

// AlertSK builds the sort key; the zero-padded time keeps lexical order = time order.
func AlertSK(windowEndNs int64, alertID string) string {
	return fmt.Sprintf("%020d#%s", windowEndNs, alertID)
}

// AlertItemFrom converts an Alert message.
func AlertItemFrom(a *domain.Alert) AlertItem {
	return AlertItem{Symbol: a.Symbol, SK: AlertSK(a.WindowEndNs, a.AlertId), AlertID: a.AlertId, RuleName: a.RuleName,
		Severity: a.Severity.String(), WindowEndNs: a.WindowEndNs, Exchange: a.Exchange, Message: a.Message,
		Values:    a.Values,
		ExpiresAt: time.Unix(0, a.WindowEndNs).Add(AlertTTL).Unix()}
}

// AlertsRepo reads and writes the alerts table.
type AlertsRepo struct {
	DB    DynamoAPI
	Table string
}

// PutIfAbsent inserts the alert unless it already exists (deterministic
// alert_id => redelivered messages are no-ops). It reports whether it inserted.
func (r *AlertsRepo) PutIfAbsent(ctx context.Context, a *domain.Alert) (bool, error) {
	av, err := attributevalue.MarshalMap(AlertItemFrom(a))
	if err != nil {
		return false, err
	}
	_, err = r.DB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(r.Table),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(alert_id)"),
	})
	if IsConditionFailed(err) {
		observability.DynamoDBStaleWrites.WithLabelValues(r.Table).Inc()
		return false, nil
	}
	if err != nil {
		observability.DynamoDBWriteErrors.WithLabelValues(r.Table).Inc()
		return false, fmt.Errorf("put alert: %w", err)
	}
	return true, nil
}

// AlertPage is one page of alert history.
type AlertPage struct {
	Items      []AlertItem `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

// Query returns a symbol's alerts with window_end_ns >= sinceNs, newest first.
func (r *AlertsRepo) Query(ctx context.Context, symbol string, sinceNs int64, limit int32, cursor string) (*AlertPage, error) {
	in := &dynamodb.QueryInput{
		TableName:              aws.String(r.Table),
		KeyConditionExpression: aws.String("symbol = :s AND sk >= :since"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":s":     &ddbtypes.AttributeValueMemberS{Value: symbol},
			":since": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("%020d", max(sinceNs, 0))},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(limit),
	}
	if cursor != "" {
		key, err := decodeCursor(cursor)
		if err != nil {
			return nil, err
		}
		in.ExclusiveStartKey = key
	}
	out, err := r.DB.Query(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("query alerts: %w", err)
	}
	page := &AlertPage{Items: make([]AlertItem, 0, len(out.Items))}
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &page.Items); err != nil {
		return nil, err
	}
	if len(out.LastEvaluatedKey) > 0 {
		page.NextCursor = encodeCursor(out.LastEvaluatedKey)
	}
	return page, nil
}

func encodeCursor(key map[string]ddbtypes.AttributeValue) string {
	var sym, sk string
	_ = attributevalue.Unmarshal(key["symbol"], &sym)
	_ = attributevalue.Unmarshal(key["sk"], &sk)
	b, _ := json.Marshal([2]string{sym, sk})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(c string) (map[string]ddbtypes.AttributeValue, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	var parts [2]string
	if err == nil {
		err = json.Unmarshal(b, &parts)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	return map[string]ddbtypes.AttributeValue{
		"symbol": &ddbtypes.AttributeValueMemberS{Value: parts[0]},
		"sk":     &ddbtypes.AttributeValueMemberS{Value: parts[1]},
	}, nil
}

// FeedHealthItem is a row of feed_health (PK exchange, SK symbol).
type FeedHealthItem struct {
	Exchange     string `dynamodbav:"exchange" json:"exchange"`
	Symbol       string `dynamodbav:"symbol" json:"symbol"`
	LastTradeNs  int64  `dynamodbav:"last_trade_ns" json:"last_trade_ns"`
	Reconnects   int64  `dynamodbav:"reconnects" json:"reconnects"`
	Status       string `dynamodbav:"status" json:"status"`
	ReportedAtNs int64  `dynamodbav:"reported_at_ns" json:"reported_at_ns"`
}

// FeedHealthItemFrom converts a FeedHealth message.
func FeedHealthItemFrom(h *domain.FeedHealth) FeedHealthItem {
	return FeedHealthItem{Exchange: h.Exchange, Symbol: h.Symbol, LastTradeNs: h.LastTradeNs, Reconnects: h.Reconnects,
		Status: StatusName(h.Status.String()), ReportedAtNs: h.ReportedAtNs}
}

// StatusName shortens "FEED_STATUS_CONNECTED" to "connected".
func StatusName(s string) string {
	switch s {
	case "FEED_STATUS_CONNECTED":
		return "connected"
	case "FEED_STATUS_RECONNECTING":
		return "reconnecting"
	case "FEED_STATUS_STALE":
		return "stale"
	}
	return "unknown"
}

// FeedHealthRepo reads and writes feed_health.
type FeedHealthRepo struct {
	DB    DynamoAPI
	Table string
}

// Put upserts a health report unless a newer report is already stored.
func (r *FeedHealthRepo) Put(ctx context.Context, h *domain.FeedHealth) error {
	av, err := attributevalue.MarshalMap(FeedHealthItemFrom(h))
	if err != nil {
		return err
	}
	_, err = r.DB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(r.Table),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(reported_at_ns) OR reported_at_ns < :new"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":new": &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(h.ReportedAtNs, 10)},
		},
	})
	if IsConditionFailed(err) {
		return nil
	}
	if err != nil {
		observability.DynamoDBWriteErrors.WithLabelValues(r.Table).Inc()
		return fmt.Errorf("put feed health: %w", err)
	}
	return nil
}

// List returns every feed-health row (the table has exchanges × symbols rows).
func (r *FeedHealthRepo) List(ctx context.Context) ([]FeedHealthItem, error) {
	var items []FeedHealthItem
	var start map[string]ddbtypes.AttributeValue
	for {
		out, err := r.DB.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(r.Table), ExclusiveStartKey: start})
		if err != nil {
			return nil, fmt.Errorf("scan feed health: %w", err)
		}
		var page []FeedHealthItem
		if err := attributevalue.UnmarshalListOfMaps(out.Items, &page); err != nil {
			return nil, err
		}
		items = append(items, page...)
		if len(out.LastEvaluatedKey) == 0 {
			return items, nil
		}
		start = out.LastEvaluatedKey
	}
}
