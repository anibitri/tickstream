package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFlags(t *testing.T) {
	f, err := parseFlags([]string{"-from", "2026-10-04T13:00:00Z", "-to", "2026-10-04T14:00:00Z", "-run-id", "r1", "-speed", "10", "-symbols", "BTC-USD,ETH-USD"})
	require.NoError(t, err)
	assert.Equal(t, "replay.r1.trades", f.topic)
	assert.Equal(t, 10.0, f.speed)
	assert.Equal(t, []string{"BTC-USD", "ETH-USD"}, f.symbols)
	assert.Equal(t, time.Hour, f.to.Sub(f.from))

	f, err = parseFlags([]string{"-from", "2026-10-04T13:00:00Z", "-to", "2026-10-04T14:00:00Z", "-topic", "md.trades"})
	require.NoError(t, err)
	assert.Equal(t, "md.trades", f.topic)
	assert.Zero(t, f.speed, "max speed by default")

	for _, bad := range [][]string{
		{"-from", "yesterday", "-to", "2026-10-04T14:00:00Z", "-run-id", "r"},
		{"-from", "2026-10-04T14:00:00Z", "-to", "2026-10-04T13:00:00Z", "-run-id", "r"},
		{"-from", "2026-10-04T13:00:00Z", "-to", "2026-10-04T14:00:00Z"},
		{"-from", "2026-10-04T13:00:00Z", "-to", "2026-10-04T14:00:00Z", "-run-id", "r", "-speed", "-1"},
	} {
		_, err := parseFlags(bad)
		assert.Error(t, err, bad)
	}

	f, err = parseFlags([]string{"-generate", "-dir", "x", "-minutes", "5"})
	require.NoError(t, err)
	assert.True(t, f.generate)
	assert.Equal(t, 5, f.minutes)
}
