package client

import (
	"testing"
	"time"
)

func TestBackoff_DoublesEachAttempt(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second
	for i := 0; i < 5; i++ {
		got := backoff(i, base, max)
		want := base * time.Duration(1<<uint(i))
		if got != want {
			t.Fatalf("attempt %d: want %v, got %v", i, want, got)
		}
	}
}

func TestBackoff_CappedAtMax(t *testing.T) {
	base := 1 * time.Second
	max := 5 * time.Second
	got := backoff(10, base, max)
	if got != max {
		t.Fatalf("want max=%v, got %v", max, got)
	}
}

func TestBackoff_AttemptAbove62_CapsAtMaxShift(t *testing.T) {
	base := 1 * time.Nanosecond
	max := 30 * time.Second
	got := backoff(63, base, max)
	if got <= 0 || got > max {
		t.Fatalf("attempt=63: want 0 < d <= max, got %v", got)
	}
	got100 := backoff(100, base, max)
	if got100 <= 0 || got100 > max {
		t.Fatalf("attempt=100: want 0 < d <= max, got %v", got100)
	}
}

func TestBackoff_OverflowToNegative_CapsAtMax(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second
	got := backoff(62, base, max)
	if got != max {
		t.Fatalf("overflow case: want max=%v, got %v", max, got)
	}
}

func TestBackoff_ZeroAttempt_ReturnsBase(t *testing.T) {
	base := 500 * time.Millisecond
	max := 30 * time.Second
	got := backoff(0, base, max)
	if got != base {
		t.Fatalf("attempt=0: want base=%v, got %v", base, got)
	}
}
