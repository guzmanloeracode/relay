package integration

import (
	"context"
	"testing"
	"time"

	"github.com/guzmanloeracode/relay/internal/config"
	"github.com/guzmanloeracode/relay/internal/queue"
)

// A slow endpoint holds at most its own max_in_flight slots, so another tenant's deliveries go straight through.
func TestSlowEndpointDoesNotStarveOthers(t *testing.T) {
	e := newEnv(t, func(c *config.Config) {
		c.WorkerConcurrency = 8
		c.WebhookTimeout = 3 * time.Second
	})
	_, slowKey := e.tenant("slowco", map[string]any{"delivery_rate": 1000})
	_, fastKey := e.tenant("fastco", map[string]any{"delivery_rate": 1000})
	slow, fast := newReceiver(t), newReceiver(t)
	slow.delay.Store(int64(700 * time.Millisecond))
	e.endpoint(slowKey, "slowco", map[string]any{"channel": "webhook", "url": slow.url(), "max_in_flight": 2, "event_types": []string{"*"}})
	e.endpoint(fastKey, "fastco", map[string]any{"channel": "webhook", "url": fast.url(), "event_types": []string{"*"}})
	e.startWorker()

	for range 20 {
		e.send(slowKey, map[string]any{"type": "job.done", "payload": map[string]any{}})
	}
	time.Sleep(200 * time.Millisecond) // the slow backlog is parked by now
	start := time.Now()
	for range 20 {
		e.send(fastKey, map[string]any{"type": "job.done", "payload": map[string]any{}})
	}
	eventually(t, 3*time.Second, "the fast tenant's 20 webhooks", func() bool { return fast.count() == 20 })
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the fast tenant waited %v behind the slow endpoint", took)
	}
	if slow.count() >= 20 {
		t.Fatalf("the slow endpoint finished too early (%d): the test proves nothing", slow.count())
	}
	if p := slow.peak.Load(); p > 2 {
		t.Fatalf("slow endpoint saw %d concurrent requests, limit is 2", p)
	}
	// Parked deliveries are woken as slots free up, without polling.
	eventually(t, 15*time.Second, "the slow backlog to drain", func() bool { return slow.count() == 20 })
	if slow.duplicates() != 0 || fast.duplicates() != 0 {
		t.Fatalf("duplicates: %d %d", slow.duplicates(), fast.duplicates())
	}
}

func TestTenantConcurrencyAcrossEndpoints(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", map[string]any{"max_in_flight": 3, "delivery_rate": 1000})
	a, b := newReceiver(t), newReceiver(t)
	for _, r := range []*receiver{a, b} {
		r.delay.Store(int64(200 * time.Millisecond))
		e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": r.url(), "max_in_flight": 10, "event_types": []string{"*"}})
	}
	shared := newReceiver(t)
	shared.delay.Store(int64(200 * time.Millisecond))
	e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": shared.url(), "max_in_flight": 10, "event_types": []string{"*"}})
	e.startWorker()

	var peak int32
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
			}
			if n := a.active.Load() + b.active.Load() + shared.active.Load(); n > peak {
				peak = n
			}
			time.Sleep(time.Millisecond)
		}
	}()
	for range 6 {
		e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	}
	eventually(t, 10*time.Second, "18 deliveries", func() bool { return a.count()+b.count()+shared.count() == 18 })
	close(done)
	<-stopped
	if peak > 3 {
		t.Fatalf("tenant had %d concurrent attempts, limit is 3", peak)
	}
	if peak < 2 {
		t.Fatalf("attempts never overlapped (peak %d): the test proves nothing", peak)
	}
}

func TestDeliveryRateLimit(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", map[string]any{"delivery_rate": 5})
	rcv := newReceiver(t)
	e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "max_in_flight": 64, "event_types": []string{"*"}})
	e.startWorker()
	for range 15 {
		e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	}
	start := time.Now()
	eventually(t, 10*time.Second, "15 deliveries", func() bool { return rcv.count() == 15 })
	// Burst of 5, then 5 a second: the last of 15 can't arrive before ~2 seconds.
	if took := time.Since(start); took < 1500*time.Millisecond {
		t.Fatalf("15 deliveries at 5/s took only %v", took)
	}
	got := rcv.all()
	if span := got[len(got)-1].At.Sub(got[0].At); span < 1500*time.Millisecond {
		t.Fatalf("deliveries spanned %v", span)
	}
}

// Redis loses everything (no persistence, a flush): PostgreSQL still knows what is due.
func TestRecoveryAfterRedisDataLoss(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "event_types": []string{"*"}})
	for range 5 {
		e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	}
	if err := e.rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	w := e.newWorker()
	e.startWorker()
	time.Sleep(300 * time.Millisecond)
	if rcv.count() != 0 {
		t.Fatal("events were delivered without the sweeper")
	}
	time.Sleep(2 * time.Second)   // SWEEP_OVERDUE in the tests
	w.Sweep(context.Background()) // overdue deliveries go back to the stream
	eventually(t, 5*time.Second, "the 5 events after the sweep", func() bool { return rcv.count() == 5 })
	eventually(t, 5*time.Second, "the 5 attempts to be recorded", func() bool {
		return countStatus(e.deliveries(key, "acme", ""), "delivered") == 5
	})

	// A due delivery whose queue entry was lost (pending in PostgreSQL, nowhere in Redis) is found once overdue.
	if _, err := e.st.Pool.Exec(context.Background(),
		`UPDATE deliveries SET status = 'pending', attempts = 0, completed_at = NULL, next_attempt_at = now() - interval '1 hour'
		 WHERE id = (SELECT id FROM deliveries ORDER BY id DESC LIMIT 1)`); err != nil {
		t.Fatal(err)
	}
	w.Sweep(context.Background())
	eventually(t, 5*time.Second, "the overdue redelivery", func() bool { return rcv.count() == 6 })
	if rcv.duplicates() != 1 { // at least once: the consumer dedupes on webhook-id
		t.Fatalf("duplicates %d", rcv.duplicates())
	}
}

// A worker reads an entry and dies before acknowledging it: after the lease, another worker takes it over.
func TestDeadConsumerEntriesAreTakenOver(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.LeaseDuration = 2 * time.Second })
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "event_types": []string{"*"}})
	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	ctx := context.Background()
	msgs, err := e.q.Read(ctx, queue.DeliveriesStream, queue.WorkerGroup, "worker-that-died", 10, 0)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("dead consumer read %d: %v", len(msgs), err)
	}
	e.startWorker() // reads only new entries, so the dead consumer's entry waits
	time.Sleep(500 * time.Millisecond)
	if rcv.count() != 0 {
		t.Fatal("delivered before the lease passed")
	}
	time.Sleep(2 * time.Second)
	e.newWorker().Sweep(ctx)
	eventually(t, 5*time.Second, "the taken-over delivery", func() bool { return rcv.count() == 1 })
}

func TestGracefulShutdownDrainsAttempts(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.WebhookTimeout = 5 * time.Second; c.LeaseDuration = 20 * time.Second })
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	rcv.delay.Store(int64(800 * time.Millisecond))
	e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "max_in_flight": 8, "event_types": []string{"*"}})
	w := e.startWorker()
	for range 4 {
		e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	}
	eventually(t, 5*time.Second, "attempts in progress", func() bool { return rcv.active.Load() == 4 })
	stopped := time.Now()
	w.stop() // like SIGTERM: returns once the attempts in progress are recorded
	if time.Since(stopped) < 300*time.Millisecond {
		t.Fatal("the worker did not wait for its attempts")
	}
	ds := e.deliveries(key, "acme", "")
	if countStatus(ds, "delivered") != 4 || countStatus(ds, "delivering") != 0 {
		t.Fatalf("after shutdown: %+v", ds)
	}
}

func TestWorkersScaleHorizontally(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.WorkerConcurrency = 8 })
	_, key := e.tenant("acme", map[string]any{"ingest_rate": 1000, "ingest_burst": 1000, "delivery_rate": 1000, "max_in_flight": 64})
	rcv := newReceiver(t)
	e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "max_in_flight": 32, "event_types": []string{"*"}})
	for range 3 {
		e.startWorker()
	}
	for range 150 {
		e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	}
	eventually(t, 15*time.Second, "150 webhooks", func() bool { return rcv.unique() == 150 })
	time.Sleep(200 * time.Millisecond)
	if rcv.duplicates() != 0 {
		t.Fatalf("%d duplicate deliveries across workers", rcv.duplicates())
	}
	if n := countStatus(e.deliveries(key, "acme", "&status=delivered"), "delivered"); n != 100 { // one page
		t.Fatalf("delivered page: %d", n)
	}
}
