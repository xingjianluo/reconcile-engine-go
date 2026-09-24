// Package queue provides a deduplicating, per-key serialized work queue with
// configurable monotonic backoff and bounded memory usage.
package queue

import "time"

// Backoff computes the delay before the next retry. attempt is the number of
// attempts already made (1 for the first failure). Implementations MUST be
// monotonic non-decreasing in attempt so repeated failures back off at least
// as long as previous ones.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// ExponentialBackoff doubles its base delay each attempt up to a maximum.
// Because each step is max(prev*2, ...) clamped to Max, the sequence is
// guaranteed to be monotonic.
type ExponentialBackoff struct {
	Base time.Duration
	Max  time.Duration
}

// NewExponentialBackoff constructs a clamped exponential policy. Non-positive
// values are normalized to safe defaults so the queue never busy-loops.
func NewExponentialBackoff(base, max time.Duration) *ExponentialBackoff {
	if base <= 0 {
		base = 10 * time.Millisecond
	}
	if max < base {
		max = base
	}
	return &ExponentialBackoff{Base: base, Max: max}
}

// Delay returns min(Base*2^(attempt-1), Max), always monotonic non-decreasing.
func (b *ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	// Double repeatedly but saturate at Max to avoid overflow.
	for i := 1; i < attempt; i++ {
		if d >= b.Max/2 {
			return b.Max
		}
		d *= 2
		if d >= b.Max {
			return b.Max
		}
	}
	if d > b.Max {
		return b.Max
	}
	return d
}

// ConstantBackoff always waits the same duration (monotonic trivially).
type ConstantBackoff struct{ D time.Duration }

// NewConstantBackoff constructs a fixed-delay policy.
func NewConstantBackoff(d time.Duration) *ConstantBackoff {
	if d <= 0 {
		d = 10 * time.Millisecond
	}
	return &ConstantBackoff{D: d}
}

// Delay returns the fixed delay.
func (b *ConstantBackoff) Delay(attempt int) time.Duration { return b.D }
