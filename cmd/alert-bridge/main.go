// Command alert-bridge forwards alerts from risk.alerts to an SNS topic. SNS
// then delivers them to an SQS queue, which triggers the alert-handler Lambda.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/kafkax"
	"github.com/anibitri/tickstream/internal/observability"
	"github.com/anibitri/tickstream/internal/storage"
)

type Config struct {
	config.Common
	config.Kafka
	config.Topics
	config.AWS
	TopicARN string `env:"ALERTS_SNS_TOPIC_ARN" envDefault:"arn:aws:sns:eu-west-2:000000000000:tickstream-alerts"`
}

func (c *Config) Validate() error {
	if c.GroupID == "" {
		c.GroupID = "alert-bridge"
	}
	return config.ValidateAll(c.Common, c.Kafka)
}

// SNSAPI is the part of the SNS client we use.
type SNSAPI interface {
	PublishBatch(ctx context.Context, in *sns.PublishBatchInput, opts ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

// Bridge publishes each batch of alerts to SNS before the loop commits it.
type Bridge struct {
	SNS       SNSAPI
	TopicARN  string
	RetryBase time.Duration
	Log       *slog.Logger
}

// maxBatch is the most entries SNS accepts in one PublishBatch call.
const maxBatch = 10

func (b *Bridge) Handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
	var entries []snstypes.PublishBatchRequestEntry
	for _, r := range recs {
		var a domain.Alert
		if err := proto.Unmarshal(r.Value, &a); err != nil {
			b.Log.Warn("skipping undecodable alert", "offset", r.Offset, "err", err)
			continue
		}
		body, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(&a)
		if err != nil {
			return nil, err
		}
		entries = append(entries, snstypes.PublishBatchRequestEntry{
			Id:      aws.String(fmt.Sprintf("e%d", len(entries))),
			Message: aws.String(string(body)),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"severity": {DataType: aws.String("String"), StringValue: aws.String(a.Severity.String())},
			},
		})
	}
	for len(entries) > 0 {
		n := min(maxBatch, len(entries))
		if err := b.publish(ctx, entries[:n]); err != nil {
			return nil, err
		}
		entries = entries[n:]
	}
	return nil, nil
}

// publish sends one batch, retrying only the entries SNS reports as failed.
func (b *Bridge) publish(ctx context.Context, batch []snstypes.PublishBatchRequestEntry) error {
	for attempt := 0; ; attempt++ {
		out, err := b.SNS.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(b.TopicARN), PublishBatchRequestEntries: batch})
		if err == nil && len(out.Failed) == 0 {
			observability.MessagesProduced.WithLabelValues("sns").Add(float64(len(batch)))
			return nil
		}
		if err == nil {
			failed := map[string]bool{}
			for _, f := range out.Failed {
				failed[aws.ToString(f.Id)] = true
			}
			var retry []snstypes.PublishBatchRequestEntry
			for _, e := range batch {
				if failed[aws.ToString(e.Id)] {
					retry = append(retry, e)
				}
			}
			observability.MessagesProduced.WithLabelValues("sns").Add(float64(len(batch) - len(retry)))
			batch = retry
			err = fmt.Errorf("%d entries failed, first: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
		}
		if attempt == 4 {
			return fmt.Errorf("publish alerts to SNS: %w", err)
		}
		b.Log.Warn("SNS publish failed; retrying", "attempt", attempt+1, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(b.RetryBase << attempt):
		}
	}
}

func main() {
	var cfg Config
	config.MustLoad(&cfg)
	observability.Run("alert-bridge", cfg.Common, func(ctx context.Context, o *observability.Obs) error {
		ac, err := storage.LoadAWS(ctx, cfg.AWS)
		if err != nil {
			return err
		}
		h := &Bridge{SNS: sns.NewFromConfig(ac), TopicARN: cfg.TopicARN, RetryBase: 200 * time.Millisecond, Log: o.Log}
		loop := &kafkax.Loop{Handler: h, Log: o.Log, Group: cfg.GroupID}
		cl, err := kafkax.NewClient(cfg.Kafka, append(kafkax.ConsumerOpts(cfg.GroupID, cfg.Alerts), loop.RebalanceOpts()...)...)
		if err != nil {
			return err
		}
		defer cl.Close()
		loop.Client = cl
		o.SetReady(true)
		return loop.Run(ctx)
	})
}
