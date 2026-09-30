package main

import (
	"context"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/anibitri/tickstream/internal/observability"
)

// Message kinds sent to dashboard clients.
const (
	KindMetrics = "metrics"
	KindAlert   = "alert"
	KindHealth  = "health"
)

// Hub fans live messages out to connected websocket clients.
type Hub struct {
	mu      sync.Mutex
	clients map[*client]struct{}
	// BufferSize is how many metrics/health messages a slow client may fall
	// behind before the oldest are dropped. Alerts are never dropped.
	BufferSize int
	// MaxAlerts disconnects a client that falls this many alerts behind.
	MaxAlerts int
}

// NewHub returns an empty hub.
func NewHub() *Hub {
	return &Hub{clients: make(map[*client]struct{}), BufferSize: 256, MaxAlerts: 1000}
}

type client struct {
	symbols map[string]bool // empty = all symbols
	mu      sync.Mutex
	alerts  [][]byte
	others  [][]byte // metrics and health, oldest first
	wake    chan struct{}
	gone    chan struct{}
	closed  bool
}

// Broadcast queues msg for every client subscribed to symbol.
func (h *Hub) Broadcast(kind, symbol string, msg []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if len(c.symbols) > 0 && !c.symbols[symbol] {
			continue
		}
		c.enqueue(kind, msg, h.BufferSize, h.MaxAlerts)
	}
}

func (c *client) enqueue(kind string, msg []byte, bufSize, maxAlerts int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if kind == KindAlert {
		if len(c.alerts) >= maxAlerts {
			c.closed = true // far too slow: disconnect rather than lose alerts silently
			close(c.gone)
			return
		}
		c.alerts = append(c.alerts, msg)
	} else {
		if len(c.others) >= bufSize {
			c.others = c.others[1:] // drop the oldest
			observability.WSClientDrops.Inc()
		}
		c.others = append(c.others, msg)
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// next returns queued messages, alerts first.
func (c *client) next() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append(c.alerts, c.others...)
	c.alerts, c.others = nil, nil
	return out
}

// Serve runs one websocket connection until it closes.
func (h *Hub) Serve(ctx context.Context, conn *websocket.Conn, symbols []string) {
	c := &client{symbols: map[string]bool{}, wake: make(chan struct{}, 1), gone: make(chan struct{})}
	for _, s := range symbols {
		c.symbols[s] = true
	}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	observability.WSClients.Set(float64(len(h.clients)))
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.clients, c)
		observability.WSClients.Set(float64(len(h.clients)))
		h.mu.Unlock()
	}()

	// The dashboard never sends anything; CloseRead handles pings and
	// cancels ctx when the client goes away.
	ctx = conn.CloseRead(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.gone:
			conn.Close(websocket.StatusPolicyViolation, "client too slow")
			return
		case <-c.wake:
			for _, msg := range c.next() {
				wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := conn.Write(wctx, websocket.MessageText, msg)
				cancel()
				if err != nil {
					return
				}
			}
		}
	}
}
