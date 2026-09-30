// Package exchange hides each exchange's websocket format behind one
// interface. The ingestor uses it to connect and subscribe; the normaliser
// uses it to turn raw messages into canonical trades.
package exchange

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/anibitri/tickstream/internal/domain"
)

// FrameKind says what a websocket message is.
type FrameKind int

const (
	FrameUnknown FrameKind = iota
	FrameTrade
	FrameHeartbeat
	FrameControl // subscription replies, status messages
	FrameError
)

func (k FrameKind) String() string {
	return [...]string{"unknown", "trade", "heartbeat", "control", "error"}[k]
}

// Frame is what Inspect learns about a message without fully parsing it.
type Frame struct {
	Kind   FrameKind
	Symbol string // canonical symbol of a trade message (used as the Kafka key)
	Err    string // error text of an error message
}

// RawTrade is one trade read from an exchange message. Price and size stay as
// exact decimal strings.
type RawTrade struct {
	Symbol      string
	TradeID     string
	EventTimeNs int64
	Price       string
	Size        string
	Side        domain.Side // side of the taker (the order that caused the trade)
}

// Adapter is one exchange's public trade feed.
type Adapter interface {
	Name() string // "coinbase", "kraken"
	URL() string
	SubscribeMessages(symbols []string) ([][]byte, error)
	Inspect(payload []byte) (Frame, error)
	Parse(payload []byte) ([]RawTrade, error)
}

// ErrNotTrade is returned by Parse for messages that carry no trades.
var ErrNotTrade = errors.New("message is not a trade")

var adapters = map[string]Adapter{
	"coinbase": &Coinbase{},
	"kraken":   &Kraken{},
}

// Get returns the adapter for an exchange name.
func Get(name string) (Adapter, error) {
	a, ok := adapters[name]
	if !ok {
		names := make([]string, 0, len(adapters))
		for n := range adapters {
			names = append(names, n)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("unknown exchange %q (known: %v)", name, names)
	}
	return a, nil
}

// Limits for exchange timestamps compared with when we received the trade.
const (
	MaxClockSkew = 5 * time.Second // a trade can't be further in the future than this
	MaxAge       = 24 * time.Hour  // or older than this (reconnect snapshots can be minutes old)
)

// ToCanonical checks a RawTrade and turns it into a domain.Trade. The error
// explains what was wrong, so the caller can send it to the dead-letter topic.
// allowed is the set of configured symbols (nil allows any valid symbol).
func ToCanonical(exch string, rt RawTrade, recvTimeNs int64, allowed map[string]bool) (*domain.Trade, error) {
	if !domain.ValidSymbol(rt.Symbol) {
		return nil, fmt.Errorf("invalid symbol %q", rt.Symbol)
	}
	if allowed != nil && !allowed[rt.Symbol] {
		return nil, fmt.Errorf("symbol %q not configured", rt.Symbol)
	}
	if rt.TradeID == "" {
		return nil, errors.New("empty trade_id")
	}
	price, err := domain.ParsePositiveDecimal(rt.Price)
	if err != nil {
		return nil, fmt.Errorf("price: %w", err)
	}
	size, err := domain.ParsePositiveDecimal(rt.Size)
	if err != nil {
		return nil, fmt.Errorf("size: %w", err)
	}
	if rt.EventTimeNs <= 0 {
		return nil, errors.New("missing event time")
	}
	if recvTimeNs > 0 {
		if rt.EventTimeNs > recvTimeNs+int64(MaxClockSkew) {
			return nil, fmt.Errorf("event time is %s ahead of receive time", time.Duration(rt.EventTimeNs-recvTimeNs))
		}
		if rt.EventTimeNs < recvTimeNs-int64(MaxAge) {
			return nil, fmt.Errorf("event time is older than %s", MaxAge)
		}
	}
	if rt.Side != domain.SideBuy && rt.Side != domain.SideSell {
		return nil, errors.New("unknown side")
	}
	return &domain.Trade{
		Exchange:    exch,
		Symbol:      rt.Symbol,
		TradeId:     rt.TradeID,
		EventTimeNs: rt.EventTimeNs,
		RecvTimeNs:  recvTimeNs,
		Price:       price.String(), // shortest exact form: "1.500" -> "1.5"
		Size:        size.String(),
		Side:        rt.Side,
	}, nil
}
