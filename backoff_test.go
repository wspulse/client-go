package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackoff_DoublesEachAttempt(t *testing.T) {
	t.Parallel()
	base := 100 * time.Millisecond
	max := 10 * time.Second
	for i := 0; i < 5; i++ {
		fullDelay := base * time.Duration(1<<uint(i))
		half := fullDelay / 2
		got := backoff(i, base, max)
		require.GreaterOrEqual(t, got, half, "attempt %d: got %v, want >= %v", i, got, half)
		require.LessOrEqual(t, got, fullDelay, "attempt %d: got %v, want <= %v", i, got, fullDelay)
	}
}

func TestBackoff_CappedAtMax(t *testing.T) {
	t.Parallel()
	base := 1 * time.Second
	max := 5 * time.Second
	half := max / 2
	got := backoff(10, base, max)
	require.GreaterOrEqual(t, got, half, "got %v, want >= %v", got, half)
	require.LessOrEqual(t, got, max, "got %v, want <= %v", got, max)
}

func TestBackoff_AttemptAbove62_CapsAtMaxShift(t *testing.T) {
	base := 1 * time.Nanosecond
	max := 30 * time.Second
	half := max / 2
	got := backoff(63, base, max)
	require.GreaterOrEqual(t, got, half, "attempt=63: got %v, want >= %v", got, half)
	require.LessOrEqual(t, got, max, "attempt=63: got %v, want <= %v", got, max)
	got100 := backoff(100, base, max)
	require.GreaterOrEqual(t, got100, half, "attempt=100: got %v, want >= %v", got100, half)
	require.LessOrEqual(t, got100, max, "attempt=100: got %v, want <= %v", got100, max)
}

func TestBackoff_OverflowToNegative_CapsAtMax(t *testing.T) {
	t.Parallel()
	base := 1 * time.Second
	max := 30 * time.Second
	half := max / 2
	got := backoff(62, base, max)
	require.GreaterOrEqual(t, got, half, "overflow case: got %v, want >= %v", got, half)
	require.LessOrEqual(t, got, max, "overflow case: got %v, want <= %v", got, max)
}

func TestBackoff_ZeroAttempt_ReturnsBase(t *testing.T) {
	t.Parallel()
	base := 500 * time.Millisecond
	max := 30 * time.Second
	half := base / 2
	got := backoff(0, base, max)
	require.GreaterOrEqual(t, got, half, "attempt=0: got %v, want >= %v", got, half)
	require.LessOrEqual(t, got, base, "attempt=0: got %v, want <= %v", got, base)
}

func TestBackoff_HasJitter(t *testing.T) {
	t.Parallel()
	base := 1 * time.Second
	max := 30 * time.Second
	attempt := 3 // deterministic delay = 8s

	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		d := backoff(attempt, base, max)
		seen[d] = true
	}
	assert.GreaterOrEqual(t, len(seen), 2, "backoff returned identical value 100 times — no jitter")
}

func TestBackoff_JitterWithinRange(t *testing.T) {
	t.Parallel()
	base := 1 * time.Second
	max := 30 * time.Second
	attempt := 3
	fullDelay := base * time.Duration(1<<uint(attempt)) // 8s
	half := fullDelay / 2                               // 4s

	for i := 0; i < 200; i++ {
		d := backoff(attempt, base, max)
		require.GreaterOrEqual(t, d, half, "attempt %d, iter %d: got %v, want >= %v", attempt, i, d, half)
		require.LessOrEqual(t, d, fullDelay, "attempt %d, iter %d: got %v, want <= %v", attempt, i, d, fullDelay)
	}
}
