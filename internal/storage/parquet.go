package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/anibitri/tickstream/internal/domain"
)

// TradeRow is the Parquet schema of the archive. Prices and sizes stay as
// exact decimal strings (DuckDB: CAST(price AS DECIMAL(38,18))).
type TradeRow struct {
	Exchange    string `parquet:"exchange,dict"`
	Symbol      string `parquet:"symbol,dict"`
	TradeID     string `parquet:"trade_id"`
	EventTimeNs int64  `parquet:"event_time_ns"`
	RecvTimeNs  int64  `parquet:"recv_time_ns"`
	Price       string `parquet:"price"`
	Size        string `parquet:"size"`
	Side        string `parquet:"side,dict"`
	KafkaOffset int64  `parquet:"kafka_offset"`
}

// RowFromTrade converts a canonical trade.
func RowFromTrade(t *domain.Trade, offset int64) TradeRow {
	side := "unknown"
	switch t.Side {
	case domain.SideBuy:
		side = "buy"
	case domain.SideSell:
		side = "sell"
	}
	return TradeRow{Exchange: t.Exchange, Symbol: t.Symbol, TradeID: t.TradeId, EventTimeNs: t.EventTimeNs,
		RecvTimeNs: t.RecvTimeNs, Price: t.Price, Size: t.Size, Side: side, KafkaOffset: offset}
}

// Trade converts a row back into a canonical trade.
func (r TradeRow) Trade() *domain.Trade {
	side := domain.Side_SIDE_UNSPECIFIED
	switch r.Side {
	case "buy":
		side = domain.SideBuy
	case "sell":
		side = domain.SideSell
	}
	return &domain.Trade{Exchange: r.Exchange, Symbol: r.Symbol, TradeId: r.TradeID, EventTimeNs: r.EventTimeNs,
		RecvTimeNs: r.RecvTimeNs, Price: r.Price, Size: r.Size, Side: side}
}

// LessRow is the canonical replay order: event time, then exchange, then trade ID.
func LessRow(a, b *TradeRow) bool {
	if a.EventTimeNs != b.EventTimeNs {
		return a.EventTimeNs < b.EventTimeNs
	}
	if a.Exchange != b.Exchange {
		return a.Exchange < b.Exchange
	}
	return a.TradeID < b.TradeID
}

// EncodeTrades sorts rows into replay order and encodes them as zstd Parquet.
// Sorting at write time lets the replayer stream files with a k-way merge.
func EncodeTrades(rows []TradeRow) ([]byte, error) {
	sort.SliceStable(rows, func(i, j int) bool { return LessRow(&rows[i], &rows[j]) })
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[TradeRow](&buf, parquet.Compression(&zstd.Codec{}), parquet.PageBufferSize(256<<10))
	if _, err := w.Write(rows); err != nil {
		return nil, fmt.Errorf("parquet write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("parquet close: %w", err)
	}
	return buf.Bytes(), nil
}

// TradeReader streams rows from one Parquet file.
type TradeReader struct {
	r    *parquet.GenericReader[TradeRow]
	buf  []TradeRow
	pos  int
	done bool
}

// NewTradeReader opens an encoded Parquet file held in memory.
func NewTradeReader(b []byte) *TradeReader {
	return &TradeReader{r: parquet.NewGenericReader[TradeRow](bytes.NewReader(b)), buf: make([]TradeRow, 0, 4096)}
}

// Next returns the next row, or io.EOF.
func (tr *TradeReader) Next() (TradeRow, error) {
	if tr.pos >= len(tr.buf) {
		if tr.done {
			return TradeRow{}, io.EOF
		}
		tr.buf = tr.buf[:cap(tr.buf)]
		n, err := tr.r.Read(tr.buf)
		tr.buf, tr.pos = tr.buf[:n], 0
		if errors.Is(err, io.EOF) {
			tr.done = true
		} else if err != nil {
			return TradeRow{}, err
		}
		if n == 0 {
			return TradeRow{}, io.EOF
		}
	}
	row := tr.buf[tr.pos]
	tr.pos++
	return row, nil
}

// Close releases the reader.
func (tr *TradeReader) Close() error { return tr.r.Close() }

// DecodeTrades reads every row of a Parquet file.
func DecodeTrades(b []byte) ([]TradeRow, error) {
	tr := NewTradeReader(b)
	defer tr.Close()
	var out []TradeRow
	for {
		row, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
}

// ArchivePartition is the Hive-style prefix of one (exchange, symbol, hour).
func ArchivePartition(exchange, symbol string, eventNs int64) string {
	t := time.Unix(0, eventNs).UTC()
	return fmt.Sprintf("trades/exchange=%s/symbol=%s/date=%s/hour=%02d/", exchange, symbol, t.Format("2006-01-02"), t.Hour())
}

// ArchiveKey names a file by the Kafka offset range it covers, so rewriting the
// same batch after a crash overwrites the same key instead of duplicating it.
func ArchiveKey(exchange, symbol string, eventNs, firstOffset, lastOffset int64) string {
	return fmt.Sprintf("%spart-%020d-%020d.parquet", ArchivePartition(exchange, symbol, eventNs), firstOffset, lastOffset)
}

// ArchiveFile describes a parsed archive key.
type ArchiveFile struct {
	Key      string
	Exchange string
	Symbol   string
	Hour     time.Time
}

// ParseArchiveKey parses a key produced by ArchiveKey.
func ParseArchiveKey(key string) (ArchiveFile, bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 6 || parts[0] != "trades" || !strings.HasSuffix(parts[5], ".parquet") {
		return ArchiveFile{}, false
	}
	get := func(s, k string) (string, bool) { return strings.CutPrefix(s, k+"=") }
	ex, ok1 := get(parts[1], "exchange")
	sym, ok2 := get(parts[2], "symbol")
	date, ok3 := get(parts[3], "date")
	hour, ok4 := get(parts[4], "hour")
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return ArchiveFile{}, false
	}
	d, err := time.Parse("2006-01-02", date)
	h, err2 := strconv.Atoi(hour)
	if err != nil || err2 != nil || h < 0 || h > 23 {
		return ArchiveFile{}, false
	}
	return ArchiveFile{Key: key, Exchange: ex, Symbol: sym, Hour: d.Add(time.Duration(h) * time.Hour)}, true
}
