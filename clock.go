package client

import "time"

// clock abstracts time operations for testability.
type clock interface {
	NewTimer(d time.Duration) *time.Timer
	NewTicker(d time.Duration) *time.Ticker
}

// realClock delegates to the time package.
type realClock struct{}

func (realClock) NewTimer(d time.Duration) *time.Timer {
	return time.NewTimer(d)
}

func (realClock) NewTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}
