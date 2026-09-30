// Package domain holds the types every service shares. The message types
// (Trade, Metrics, Alert, ...) are generated from proto/tickstream/v1 into
// tickstream.pb.go; this file adds small helpers around them.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

// Shorter names for the generated enum values.
const (
	SideBuy  = Side_SIDE_BUY
	SideSell = Side_SIDE_SELL

	SeverityInfo     = Severity_INFO
	SeverityWarn     = Severity_WARN
	SeverityCritical = Severity_CRITICAL
)

// ParseSeverity turns "INFO", "WARN" or "CRITICAL" into a Severity.
func ParseSeverity(s string) (Severity, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "INFO":
		return SeverityInfo, nil
	case "WARN", "WARNING":
		return SeverityWarn, nil
	case "CRITICAL":
		return SeverityCritical, nil
	}
	return Severity_SEVERITY_UNSPECIFIED, fmt.Errorf("unknown severity %q", s)
}

// ParsePositiveDecimal parses a price or size. Prices are never parsed as
// floats, because floats can't store most decimal numbers exactly.
func ParsePositiveDecimal(s string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("invalid decimal %q: %w", s, err)
	}
	if !d.IsPositive() {
		return decimal.Decimal{}, fmt.Errorf("decimal %q must be > 0", s)
	}
	return d, nil
}

// ValidSymbol checks the BASE-QUOTE format, e.g. "BTC-USD".
func ValidSymbol(s string) bool {
	base, quote, ok := strings.Cut(s, "-")
	return ok && isUpperAlnum(base) && isUpperAlnum(quote)
}

func isUpperAlnum(s string) bool {
	if s == "" || len(s) > 12 {
		return false
	}
	for _, r := range s {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// AlertID gives the same ID every time the same rule fires for the same
// symbol and window. Because of that, storing an alert twice is harmless.
func AlertID(rule, symbol string, windowEndNs int64, exchange string) string {
	parts := []string{rule, symbol, strconv.FormatInt(windowEndNs, 10)}
	if exchange != "" {
		parts = append(parts, exchange)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])[:32]
}

// DedupKey identifies a trade across all exchanges.
func DedupKey(exchange, tradeID string) string { return exchange + "|" + tradeID }
