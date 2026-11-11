// Package cache is a small in-process cache with expiry, used for API keys, tenants, routes and limits.
// Writes elsewhere publish an invalidation (see queue.Invalidate), and entries expire anyway, so a missed
// invalidation only delays a change by the TTL.
package cache

import (
	"sync"
	"time"
)

type entry[V any] struct {
	v   V
	exp time.Time
}

// Cache maps K to V for ttl. The zero value is not usable; call New.
type Cache[K comparable, V any] struct {
	mu   sync.RWMutex
	m    map[K]entry[V]
	ttl  time.Duration
	max  int
	now  func() time.Time
	sets int
}

// New returns a cache that keeps entries for ttl and at most max entries.
func New[K comparable, V any](ttl time.Duration, max int) *Cache[K, V] {
	return &Cache[K, V]{m: make(map[K]entry[V]), ttl: ttl, max: max, now: time.Now}
}

// Get returns a live entry.
func (c *Cache[K, V]) Get(k K) (V, bool) {
	c.mu.RLock()
	e, ok := c.m[k]
	c.mu.RUnlock()
	if !ok || c.now().After(e.exp) {
		var zero V
		return zero, false
	}
	return e.v, true
}

// Set stores v under k.
func (c *Cache[K, V]) Set(k K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.sets++
	if len(c.m) >= c.max || c.sets%1024 == 0 {
		for key, e := range c.m {
			if now.After(e.exp) {
				delete(c.m, key)
			}
		}
		if len(c.m) >= c.max { // still full of live entries: start over rather than grow without bound
			clear(c.m)
		}
	}
	c.m[k] = entry[V]{v: v, exp: now.Add(c.ttl)}
}

// Delete drops k.
func (c *Cache[K, V]) Delete(k K) {
	c.mu.Lock()
	delete(c.m, k)
	c.mu.Unlock()
}

// DeleteFunc drops every entry for which drop returns true.
func (c *Cache[K, V]) DeleteFunc(drop func(K, V) bool) {
	c.mu.Lock()
	for k, e := range c.m {
		if drop(k, e.v) {
			delete(c.m, k)
		}
	}
	c.mu.Unlock()
}

// GetOrLoad returns the cached value or calls load and caches its result (errors are not cached).
func (c *Cache[K, V]) GetOrLoad(k K, load func() (V, error)) (V, error) {
	if v, ok := c.Get(k); ok {
		return v, nil
	}
	v, err := load()
	if err != nil {
		return v, err
	}
	c.Set(k, v)
	return v, nil
}
