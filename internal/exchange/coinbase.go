package exchange

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/anibitri/tickstream/internal/domain"
)

// CoinbaseURL is Coinbase Exchange's public market data feed (no API key).
const CoinbaseURL = "wss://ws-feed.exchange.coinbase.com"

// Coinbase reads the "matches" channel. Endpoint overrides the URL in tests.
type Coinbase struct{ Endpoint string }

func (c *Coinbase) Name() string { return "coinbase" }

func (c *Coinbase) URL() string {
	if c.Endpoint != "" {
		return c.Endpoint
	}
	return CoinbaseURL
}

// SubscribeMessages subscribes to trades plus a once-a-second heartbeat per
// product, so we can tell a quiet market from a dead connection.
// Coinbase already uses the BTC-USD symbol format.
func (c *Coinbase) SubscribeMessages(symbols []string) ([][]byte, error) {
	b, err := json.Marshal(map[string]any{
		"type":        "subscribe",
		"product_ids": symbols,
		"channels":    []string{"matches", "heartbeat"},
	})
	return [][]byte{b}, err
}

func (c *Coinbase) Inspect(payload []byte) (Frame, error) {
	var m struct {
		Type      string `json:"type"`
		ProductID string `json:"product_id"`
		Message   string `json:"message"`
		Reason    string `json:"reason"`
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		return Frame{}, fmt.Errorf("coinbase: invalid json: %w", err)
	}
	switch m.Type {
	case "match", "last_match":
		return Frame{Kind: FrameTrade, Symbol: m.ProductID}, nil
	case "heartbeat":
		return Frame{Kind: FrameHeartbeat, Symbol: m.ProductID}, nil
	case "subscriptions":
		return Frame{Kind: FrameControl}, nil
	case "error":
		return Frame{Kind: FrameError, Err: m.Message + ": " + m.Reason}, nil
	}
	return Frame{Kind: FrameUnknown}, nil
}

// Parse reads a match. Coinbase reports the side of the *maker* (the resting
// order), so the taker's side is the opposite one.
func (c *Coinbase) Parse(payload []byte) ([]RawTrade, error) {
	var m struct {
		Type      string       `json:"type"`
		TradeID   *json.Number `json:"trade_id"`
		ProductID string       `json:"product_id"`
		Price     string       `json:"price"`
		Size      string       `json:"size"`
		Side      string       `json:"side"`
		Time      string       `json:"time"`
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("coinbase: invalid json: %w", err)
	}
	if m.Type != "match" && m.Type != "last_match" {
		return nil, ErrNotTrade
	}
	if m.TradeID == nil || m.TradeID.String() == "" {
		return nil, fmt.Errorf("coinbase: missing trade_id")
	}
	ts, err := time.Parse(time.RFC3339Nano, m.Time)
	if err != nil {
		return nil, fmt.Errorf("coinbase: bad time %q: %w", m.Time, err)
	}
	var side domain.Side
	switch m.Side {
	case "buy":
		side = domain.SideSell
	case "sell":
		side = domain.SideBuy
	default:
		return nil, fmt.Errorf("coinbase: bad side %q", m.Side)
	}
	return []RawTrade{{Symbol: m.ProductID, TradeID: m.TradeID.String(), EventTimeNs: ts.UnixNano(),
		Price: m.Price, Size: m.Size, Side: side}}, nil
}
