package exchange

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/anibitri/tickstream/internal/domain"
)

// KrakenURL is Kraken's public v2 websocket (no API key).
const KrakenURL = "wss://ws.kraken.com/v2"

// Kraken reads the "trade" channel. Endpoint overrides the URL in tests.
type Kraken struct{ Endpoint string }

func (k *Kraken) Name() string { return "kraken" }

func (k *Kraken) URL() string {
	if k.Endpoint != "" {
		return k.Endpoint
	}
	return KrakenURL
}

// Kraken writes symbols as "BTC/USD"; we use "BTC-USD".
func toKrakenSymbol(s string) string   { return strings.Replace(s, "-", "/", 1) }
func fromKrakenSymbol(s string) string { return strings.Replace(s, "/", "-", 1) }

// SubscribeMessages asks for a snapshot of recent trades on every (re)connect.
// It fills part of any gap left by a disconnect; trades we already have are
// removed by the normaliser's duplicate check.
func (k *Kraken) SubscribeMessages(symbols []string) ([][]byte, error) {
	ks := make([]string, len(symbols))
	for i, s := range symbols {
		ks[i] = toKrakenSymbol(s)
	}
	b, err := json.Marshal(map[string]any{
		"method": "subscribe",
		"params": map[string]any{"channel": "trade", "symbol": ks, "snapshot": true},
	})
	return [][]byte{b}, err
}

func (k *Kraken) Inspect(payload []byte) (Frame, error) {
	var m struct {
		Channel string `json:"channel"`
		Method  string `json:"method"`
		Success *bool  `json:"success"`
		Error   string `json:"error"`
		Data    []struct {
			Symbol string `json:"symbol"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		return Frame{}, fmt.Errorf("kraken: invalid json: %w", err)
	}
	switch {
	case m.Channel == "trade":
		f := Frame{Kind: FrameTrade}
		if len(m.Data) > 0 {
			f.Symbol = fromKrakenSymbol(m.Data[0].Symbol)
		}
		return f, nil
	case m.Channel == "heartbeat":
		return Frame{Kind: FrameHeartbeat}, nil
	case m.Channel == "status":
		return Frame{Kind: FrameControl}, nil
	case m.Method != "":
		if m.Success != nil && !*m.Success {
			return Frame{Kind: FrameError, Err: m.Error}, nil
		}
		return Frame{Kind: FrameControl}, nil
	}
	return Frame{Kind: FrameUnknown}, nil
}

// Parse reads a trade message. Kraken sends price and qty as JSON numbers, so
// they are read as json.Number to keep the exact digits (a float would round
// them). Kraken's side is already the taker's side.
func (k *Kraken) Parse(payload []byte) ([]RawTrade, error) {
	var m struct {
		Channel string `json:"channel"`
		Data    []struct {
			Symbol    string       `json:"symbol"`
			Side      string       `json:"side"`
			Price     *json.Number `json:"price"`
			Qty       *json.Number `json:"qty"`
			TradeID   *json.Number `json:"trade_id"`
			Timestamp string       `json:"timestamp"`
		} `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("kraken: invalid json: %w", err)
	}
	if m.Channel != "trade" {
		return nil, ErrNotTrade
	}
	out := make([]RawTrade, 0, len(m.Data))
	for i, d := range m.Data {
		if d.Price == nil || d.Qty == nil || d.TradeID == nil {
			return nil, fmt.Errorf("kraken: trade %d missing price/qty/trade_id", i)
		}
		ts, err := time.Parse(time.RFC3339Nano, d.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("kraken: bad timestamp %q: %w", d.Timestamp, err)
		}
		var side domain.Side
		switch d.Side {
		case "buy":
			side = domain.SideBuy
		case "sell":
			side = domain.SideSell
		default:
			return nil, fmt.Errorf("kraken: bad side %q", d.Side)
		}
		out = append(out, RawTrade{Symbol: fromKrakenSymbol(d.Symbol), TradeID: d.TradeID.String(),
			EventTimeNs: ts.UnixNano(), Price: d.Price.String(), Size: d.Qty.String(), Side: side})
	}
	return out, nil
}
