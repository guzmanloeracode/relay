package cache

import (
	"errors"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	now := time.Unix(0, 0)
	c := New[string, int](time.Minute, 3)
	c.now = func() time.Time { return now }

	c.Set("a", 1)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatal("Get after Set")
	}
	now = now.Add(2 * time.Minute)
	if _, ok := c.Get("a"); ok {
		t.Fatal("expired entry returned")
	}

	calls := 0
	load := func() (int, error) { calls++; return 7, nil }
	for range 3 {
		if v, err := c.GetOrLoad("b", load); err != nil || v != 7 {
			t.Fatal("GetOrLoad")
		}
	}
	if calls != 1 {
		t.Fatalf("loaded %d times", calls)
	}
	if _, err := c.GetOrLoad("c", func() (int, error) { return 0, errors.New("x") }); err == nil {
		t.Fatal("error swallowed")
	}
	if _, ok := c.Get("c"); ok {
		t.Fatal("error cached")
	}

	c.Set("d", 4)
	c.Set("e", 5)
	c.Set("f", 6) // full: expired "a" goes first, then everything if still full
	if _, ok := c.Get("f"); !ok {
		t.Fatal("newest entry lost")
	}
	c.DeleteFunc(func(k string, _ int) bool { return k == "f" })
	if _, ok := c.Get("f"); ok {
		t.Fatal("DeleteFunc")
	}
	c.Set("g", 1)
	c.Delete("g")
	if _, ok := c.Get("g"); ok {
		t.Fatal("Delete")
	}
}
