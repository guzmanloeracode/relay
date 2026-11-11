package worker

import (
	"testing"
	"time"
)

func TestBackoffFitsInADay(t *testing.T) {
	sched := Schedule(8, 75*time.Second, 16*time.Hour)
	if len(sched) != 7 {
		t.Fatalf("8 attempts have 7 waits, got %d", len(sched))
	}
	var total time.Duration
	for i, d := range sched {
		if i > 0 && d <= sched[i-1] {
			t.Fatalf("not increasing: %v", sched)
		}
		total += d
	}
	if total > 24*time.Hour || total < 20*time.Hour {
		t.Fatalf("8 attempts should span up to a day, got %v", total)
	}
	if sched[0] != 75*time.Second {
		t.Fatalf("first wait %v", sched[0])
	}
}

func TestBackoffJitterAndCap(t *testing.T) {
	base := time.Second
	if got := backoff(1, base, time.Hour, 0); got != 750*time.Millisecond {
		t.Fatalf("lowest jitter: %v", got)
	}
	if got := backoff(3, base, time.Hour, 1); got != 9*time.Second {
		t.Fatalf("third: %v", got)
	}
	if got := backoff(30, base, time.Minute, 0.5); got != time.Minute {
		t.Fatalf("cap: %v", got)
	}
	if got := backoff(0, base, time.Hour, 1); got != base {
		t.Fatalf("n<1 treated as 1: %v", got)
	}
	for range 1000 {
		d := Backoff(2, base, time.Hour)
		if d < 2250*time.Millisecond || d > 3*time.Second {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
}
