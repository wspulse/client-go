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
		require.True(t, got >= half && got <= fullDelay,
			"attempt %d: want [%v, %v], got %v", i, half, fullDelay, got)
	}
}

func TestBackoff_CappedAtMax(t *testing.T) {
	t.Parallel()
	base := 1 * time.Second
	max := 5 * time.Second
	half := max / 2
	got := backoff(10, base, max)
	require.True(t, got >= half && got <= max,
		"want [%v, %v], got %v", half, max, got)
}

func TestBackoff_AttemptAbove62_CapsAtMaxShift(t *testing.T) {
	base := 1 * time.Nanosecond
	max := 30 * time.Second
	half := max / 2
	got := backoff(63, base, max)
	require.True(t, got >= half && got <= max,
		"attempt=63: want [%v, %v], got %v", half, max, got)
	got100 := backoff(100, base, max)
	require.True(t, got100 >= half && got100 <= max,
		"attempt=100: want [%v, %v], got %v", half, max, got100)
}

func TestBackoff_OverflowToNegative_CapsAtMax(t *testing.T) {
	t.Parallel()
	base := 1 * time.Second
	max := 30 * time.Second
	half := max / 2
	got := backoff(62, base, max)
	require.True(t, got >= half && got <= max,
		"overflow case: want [%v, %v], got %v", half, max, got)
}

func TestBackoff_ZeroAttempt_ReturnsBase(t *testing.T) {
	t.Parallel()
	base := 500 * time.Millisecond
	max := 30 * time.Second
	half := base / 2
	got := backoff(0, base, max)
	require.True(t, got >= half && got <= base,
		"attempt=0: want [%v, %v], got %v", half, base, got)
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
	require.Greater(t, len(seen), 1,
		"backoff returned identical value %d times — no jitter", 100)
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
		assert.True(t, d >= half && d <= fullDelay,
			"attempt %d: want d in [%v, %v], got %v", attempt, half, fullDelay, d)
	}
}
