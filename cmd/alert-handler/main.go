// Command alert-handler is an AWS Lambda function triggered by the alert SQS
// queue. It stores each alert in DynamoDB exactly once and, for new alerts,
// posts a message to an optional Discord/Slack webhook.
//
// Build it for Lambda with:
//
//	GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o bootstrap ./cmd/alert-handler
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/anibitri/tickstream/internal/config"
	"github.com/anibitri/tickstream/internal/domain"
	"github.com/anibitri/tickstream/internal/storage"
)

// Handler processes one batch of SQS messages.
type Handler struct {
	Alerts     *storage.AlertsRepo
	WebhookURL string // empty disables webhook delivery
	HTTP       *http.Client
	Log        *slog.Logger
}

// Handle stores every alert in the batch. Messages that fail are reported
// back to SQS individually, so only they are retried; after 3 failed attempts
// SQS moves them to the dead-letter queue.
func (h *Handler) Handle(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, msg := range ev.Records {
		if err := h.handleOne(ctx, msg.Body); err != nil {
			h.Log.Error("alert failed", "message_id", msg.MessageId, "err", err)
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: msg.MessageId})
		}
	}
	return resp, nil
}

func (h *Handler) handleOne(ctx context.Context, body string) error {
	a, err := decodeAlert(body)
	if err != nil {
		return err
	}
	inserted, err := h.Alerts.PutIfAbsent(ctx, a)
	if err != nil {
		return err
	}
	if !inserted {
		h.Log.Info("duplicate alert ignored", "alert_id", a.AlertId)
		return nil
	}
	h.Log.Info("alert stored", "alert_id", a.AlertId, "rule", a.RuleName, "symbol", a.Symbol)
	if h.WebhookURL != "" {
		// A failed webhook is logged but not retried: the alert is already
		// stored, and a retry would be skipped as a duplicate anyway.
		if err := h.postWebhook(ctx, a); err != nil {
			h.Log.Warn("webhook failed", "alert_id", a.AlertId, "err", err)
		}
	}
	return nil
}

// decodeAlert accepts the alert JSON directly (SNS raw delivery) or wrapped in
// an SNS notification envelope.
func decodeAlert(body string) (*domain.Alert, error) {
	var envelope struct {
		Type    string `json:"Type"`
		Message string `json:"Message"`
	}
	if json.Unmarshal([]byte(body), &envelope) == nil && envelope.Type == "Notification" {
		body = envelope.Message
	}
	var a domain.Alert
	if err := protojson.Unmarshal([]byte(body), &a); err != nil {
		return nil, fmt.Errorf("decode alert: %w", err)
	}
	if a.AlertId == "" || a.Symbol == "" {
		return nil, fmt.Errorf("alert is missing alert_id or symbol")
	}
	return &a, nil
}

func (h *Handler) postWebhook(ctx context.Context, a *domain.Alert) error {
	text := fmt.Sprintf("[%s] %s", a.Severity.String(), a.Message)
	// "content" is read by Discord, "text" by Slack; each ignores the other.
	payload, _ := json.Marshal(map[string]string{"content": text, "text": text})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.WebhookURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := h.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("webhook returned %s", res.Status)
	}
	return nil
}

func main() {
	var cfg struct {
		config.AWS
		config.Tables
		// Optional Discord or Slack incoming-webhook URL (set in .env and
		// passed to the Lambda by Terraform).
		WebhookURL string `env:"ALERT_WEBHOOK_URL"`
	}
	config.MustLoad(&cfg)
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "alert-handler")

	ac, err := storage.LoadAWS(context.Background(), cfg.AWS)
	if err != nil {
		log.Error("aws config", "err", err)
		os.Exit(1)
	}
	h := &Handler{
		Alerts:     &storage.AlertsRepo{DB: storage.NewDynamoDB(ac), Table: cfg.Alerts},
		WebhookURL: cfg.WebhookURL,
		HTTP:       &http.Client{Timeout: 5 * time.Second},
		Log:        log,
	}
	lambda.Start(h.Handle)
}
