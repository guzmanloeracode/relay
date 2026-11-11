package batch

import (
	"testing"
	"time"
)

func TestCollect(t *testing.T) {
	in := make(chan int, 10)
	for i := range 5 {
		in <- i
	}
	if got := Collect(in, []int{-1}, 3, 0); len(got) != 3 {
		t.Fatalf("max: %v", got)
	}
	if got := Collect(in, nil, 10, 0); len(got) != 3 {
		t.Fatalf("drain: %v", got)
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		in <- 9
	}()
	start := time.Now()
	got := Collect(in, nil, 10, 50*time.Millisecond)
	if len(got) != 1 || got[0] != 9 || time.Since(start) < 50*time.Millisecond {
		t.Fatalf("linger: %v after %v", got, time.Since(start))
	}
	for i := range 4 {
		in <- i
	}
	start = time.Now()
	if got := Collect(in, nil, 4, time.Second); len(got) != 4 || time.Since(start) > 100*time.Millisecond {
		t.Fatal("a full batch must not linger")
	}
}
