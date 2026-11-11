package worker

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff returns the wait after the n-th failed attempt (n starts at 1): base × 3^(n-1), shortened by up to a
// quarter at random so retries from many deliveries spread out, and capped at max. With the defaults (base 75 s,
// 8 attempts) the seven waits add up to at most 22.8 hours: 75 s, 3.8 min, 11 min, 34 min, 1.7 h, 5.1 h, 15.2 h.
func Backoff(n int, base, max time.Duration) time.Duration {
	return backoff(n, base, max, rand.Float64()) //nolint:gosec // jitter only spreads retries out
}

func backoff(n int, base, max time.Duration, r float64) time.Duration {
	if n < 1 {
		n = 1
	}
	d := float64(base) * math.Pow(3, float64(n-1))
	d *= 0.75 + 0.25*r
	if d > float64(max) {
		return max
	}
	return time.Duration(d)
}

// Schedule is the worst-case wait after each failed attempt but the last, for docs and tests.
func Schedule(attempts int, base, max time.Duration) []time.Duration {
	out := make([]time.Duration, 0, attempts-1)
	for n := 1; n < attempts; n++ {
		out = append(out, backoff(n, base, max, 1))
	}
	return out
}
