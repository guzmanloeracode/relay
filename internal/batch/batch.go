// Package batch gathers work items from a channel into batches (the group commits of the API and the worker).
package batch

import "time"

// Collect adds to b what is already waiting in in, up to max items. If linger is positive and the batch isn't
// full, it keeps collecting until linger has passed: a short wait that turns many small statements into a few
// large ones once traffic is steady.
func Collect[T any](in <-chan T, b []T, max int, linger time.Duration) []T {
	for len(b) < max {
		select {
		case v := <-in:
			b = append(b, v)
			continue
		default:
		}
		break
	}
	if linger <= 0 || len(b) >= max {
		return b
	}
	timer := time.NewTimer(linger)
	defer timer.Stop()
	for len(b) < max {
		select {
		case v := <-in:
			b = append(b, v)
		case <-timer.C:
			return b
		}
	}
	return b
}
