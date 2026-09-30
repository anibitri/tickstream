//go:build integration

// Integration test against LocalStack (testcontainers): real S3 and DynamoDB
// APIs, including the conditional writes that make repeats harmless.
// Run with: LOCALSTACK_AUTH_TOKEN=... go test -tags integration ./internal/storage/
package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/domain"
)

func startLocalStack(t *testing.T) config.AWS {
	t.Helper()
	ctx := context.Background()
	// A plain container: the testcontainers LocalStack module can't parse
	// LocalStack's calendar version tags (2026.08.5).
	ctr, err := testcontainers.Run(ctx, "localstack/localstack:2026.08.5",
		testcontainers.WithEnv(map[string]string{
			"SERVICES":              "s3,dynamodb",
			"LOCALSTACK_AUTH_TOKEN": os.Getenv("LOCALSTACK_AUTH_TOKEN"),
		}),
		testcontainers.WithExposedPorts("4566/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/_localstack/health").WithPort("4566/tcp").
			WithStartupTimeout(3*time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "4566/tcp")
	require.NoError(t, err)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	return config.AWS{Region: "eu-west-2", Endpoint: fmt.Sprintf("http://%s:%s", host, port.Port())}
}

// createTable mirrors infra/dynamodb.tf.
func createTable(t *testing.T, db *dynamodb.Client, name, pk string, pkType ddbtypes.ScalarAttributeType, sk string, skType ddbtypes.ScalarAttributeType) {
	t.Helper()
	_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String(pk), AttributeType: pkType}, {AttributeName: aws.String(sk), AttributeType: skType},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String(pk), KeyType: ddbtypes.KeyTypeHash}, {AttributeName: aws.String(sk), KeyType: ddbtypes.KeyTypeRange},
		},
	})
	require.NoError(t, err)
}

func TestAgainstLocalStack(t *testing.T) {
	ctx := context.Background()
	cfg := startLocalStack(t)
	ac, err := LoadAWS(ctx, cfg)
	require.NoError(t, err)

	t.Run("S3 archive store", func(t *testing.T) {
		client := NewS3(ac, cfg.Endpoint)
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("it-archive"),
			CreateBucketConfiguration: &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraintEuWest2}})
		require.NoError(t, err)
		store := &S3Store{Client: client, Bucket: "it-archive"}
		body, err := EncodeTrades([]TradeRow{{Exchange: "coinbase", Symbol: "BTC-USD", TradeID: "1", EventTimeNs: 1, Price: "1", Size: "1", Side: "buy"}})
		require.NoError(t, err)
		key := ArchiveKey("coinbase", "BTC-USD", time.Date(2026, 10, 4, 13, 0, 0, 0, time.UTC).UnixNano(), 0, 0)
		require.NoError(t, store.Put(ctx, key, body, "application/vnd.apache.parquet"))
		require.NoError(t, store.Put(ctx, key, body, "application/vnd.apache.parquet"), "rewriting the same batch is harmless")
		keys, err := store.List(ctx, "trades/")
		require.NoError(t, err)
		assert.Equal(t, []string{key}, keys)
		got, err := store.Get(ctx, key)
		require.NoError(t, err)
		rows, err := DecodeTrades(got)
		require.NoError(t, err)
		assert.Equal(t, "BTC-USD", rows[0].Symbol)
		_, err = store.Get(ctx, "missing")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	db := NewDynamoDB(ac)
	createTable(t, db, "latest_metrics", "symbol", ddbtypes.ScalarAttributeTypeS, "window_secs", ddbtypes.ScalarAttributeTypeN)
	createTable(t, db, "alerts", "symbol", ddbtypes.ScalarAttributeTypeS, "sk", ddbtypes.ScalarAttributeTypeS)
	createTable(t, db, "feed_health", "exchange", ddbtypes.ScalarAttributeTypeS, "symbol", ddbtypes.ScalarAttributeTypeS)

	t.Run("latest metrics only move forward", func(t *testing.T) {
		repo := &MetricsRepo{DB: db, Table: "latest_metrics"}
		m := func(end int64, vwap string) *domain.Metrics {
			return &domain.Metrics{Symbol: "BTC-USD", WindowSecs: 10, WindowEndNs: end, Vwap: vwap}
		}
		ok, err := repo.PutLatest(ctx, m(200, "2"))
		require.NoError(t, err)
		assert.True(t, ok)
		ok, err = repo.PutLatest(ctx, m(100, "1")) // older: rejected
		require.NoError(t, err)
		assert.False(t, ok)
		ok, err = repo.PutLatest(ctx, m(200, "2")) // repeat: rejected
		require.NoError(t, err)
		assert.False(t, ok)
		got, err := repo.GetLatest(ctx, "BTC-USD", 10)
		require.NoError(t, err)
		assert.Equal(t, "2", got.VWAP)
		_, err = repo.GetLatest(ctx, "ETH-USD", 10)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("alerts are stored once and page newest first", func(t *testing.T) {
		repo := &AlertsRepo{DB: db, Table: "alerts"}
		for i := range 5 {
			a := &domain.Alert{AlertId: fmt.Sprint("a", i), RuleName: "price_jump", Symbol: "BTC-USD",
				Severity: domain.SeverityWarn, WindowEndNs: int64(i+1) * 1e9, Values: map[string]float64{"value": float64(i)}}
			ok, err := repo.PutIfAbsent(ctx, a)
			require.NoError(t, err)
			assert.True(t, ok)
			ok, err = repo.PutIfAbsent(ctx, a)
			require.NoError(t, err)
			assert.False(t, ok, "second copy is ignored")
		}
		page, err := repo.Query(ctx, "BTC-USD", 2e9, 2, "")
		require.NoError(t, err)
		require.Len(t, page.Items, 2)
		assert.Equal(t, "a4", page.Items[0].AlertID)
		require.NotEmpty(t, page.NextCursor)
		page, err = repo.Query(ctx, "BTC-USD", 2e9, 10, page.NextCursor)
		require.NoError(t, err)
		assert.Len(t, page.Items, 2, "a2 and a1 remain at or after since=2s")
	})

	t.Run("feed health", func(t *testing.T) {
		repo := &FeedHealthRepo{DB: db, Table: "feed_health"}
		require.NoError(t, repo.Put(ctx, &domain.FeedHealth{Exchange: "kraken", Symbol: "BTC-USD", ReportedAtNs: 2, Status: domain.FeedStatus_FEED_STATUS_CONNECTED}))
		require.NoError(t, repo.Put(ctx, &domain.FeedHealth{Exchange: "kraken", Symbol: "BTC-USD", ReportedAtNs: 1, Status: domain.FeedStatus_FEED_STATUS_STALE}))
		items, err := repo.List(ctx)
		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Equal(t, "connected", items[0].Status, "the older report did not overwrite the newer one")
	})
}
