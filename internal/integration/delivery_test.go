package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/guzmanloeracode/relay/internal/api"
	"github.com/guzmanloeracode/relay/internal/config"
)

func TestWebhookDeliveryEndToEnd(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	epID, secret := e.endpoint(key, "acme", map[string]any{
		"channel": "webhook", "url": rcv.url(), "description": "jobs", "event_types": []string{"job.*"},
	})
	if !strings.HasPrefix(secret, "whsec_") {
		t.Fatalf("secret %q", secret)
	}
	rcv.setSecrets(secret)
	e.startWorker()

	evtID := e.send(key, map[string]any{"type": "job.completed", "payload": map[string]any{"job": "J-1042"}})
	e.send(key, map[string]any{"type": "invoice.paid", "payload": map[string]any{"invoice": "INV-7"}}) // not subscribed

	eventually(t, 5*time.Second, "the webhook", func() bool { return rcv.count() == 1 })
	got := rcv.all()[0]
	if got.Envelope.ID != evtID || got.Envelope.Type != "job.completed" || got.Envelope.Tenant != "acme" ||
		string(got.Envelope.Data) != `{"job":"J-1042"}` {
		t.Fatalf("envelope: %+v", got.Envelope)
	}
	if got.Header.Get("webhook-id") != evtID || got.Header.Get("Relay-Attempt") != "1" ||
		!strings.HasPrefix(got.Header.Get("Relay-Delivery-Id"), "dlv_") || !strings.HasPrefix(got.Header.Get("User-Agent"), "Relay/") {
		t.Fatalf("headers: %v", got.Header)
	}

	// The delivery log.
	var ev api.EventView
	eventually(t, 5*time.Second, "the delivery to be recorded", func() bool {
		r := e.call(key, "GET", "/v1/tenants/acme/events/"+evtID, nil)
		r.json(t, &ev)
		return len(ev.Deliveries) == 1 && ev.Deliveries[0].Status == "delivered"
	})
	if ev.DeliveryCount != 1 || ev.RoutedAt == nil {
		t.Fatalf("event: %+v", ev)
	}
	d := ev.Deliveries[0]
	if d.EndpointID != epID || d.Attempts != 1 || d.LastStatusCode == nil || *d.LastStatusCode != 200 || d.CompletedAt == nil || d.NextAttemptAt != nil {
		t.Fatalf("delivery: %+v", d)
	}
	var full api.DeliveryView
	e.call(key, "GET", "/v1/tenants/acme/deliveries/"+d.ID, nil).json(t, &full)
	if len(full.AttemptLog) != 1 || full.AttemptLog[0].Outcome != "delivered" || *full.AttemptLog[0].ResponseBody != `{"ok":true}` {
		t.Fatalf("attempt log: %+v", full.AttemptLog)
	}

	// Filters on the log.
	if ds := e.deliveries(key, "acme", "&endpoint_id="+epID+"&status=delivered"); len(ds) != 1 {
		t.Fatalf("filtered: %d", len(ds))
	}
	if ds := e.deliveries(key, "acme", "&status=dead"); len(ds) != 0 {
		t.Fatalf("no dead deliveries expected: %d", len(ds))
	}
	var events api.List[api.EventView]
	e.call(key, "GET", "/v1/tenants/acme/events?type=invoice.paid", nil).json(t, &events)
	if len(events.Data) != 1 || events.Data[0].DeliveryCount != 0 {
		t.Fatalf("events by type: %+v", events.Data)
	}
}

func TestIdempotencyKey(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "event_types": []string{"*"}})
	e.startWorker()

	body := map[string]any{"type": "job.created", "payload": map[string]any{"job": "J-1"}}
	first := e.call(key, "POST", "/v1/events", body, "Idempotency-Key", "outbox-41")
	again := e.call(key, "POST", "/v1/events", body, "Idempotency-Key", "outbox-41")
	if first.Status != 202 || again.Status != 202 {
		t.Fatalf("statuses %d %d", first.Status, again.Status)
	}
	if first.obj(t)["id"] != again.obj(t)["id"] || again.Header.Get("Idempotent-Replayed") != "true" || first.Header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("replay: %s / %s %v", first.Body, again.Body, again.Header)
	}
	body["payload"] = map[string]any{"job": "J-2"}
	if r := e.call(key, "POST", "/v1/events", body, "Idempotency-Key", "outbox-41"); r.Status != 422 {
		t.Fatalf("reused key with another body: %d", r.Status)
	} else if code, _ := r.problem(t); code != "idempotency_key_reused" {
		t.Fatalf("code %s", code)
	}
	if r := e.call(key, "POST", "/v1/events", body, "Idempotency-Key", "has space"); r.Status != 422 {
		t.Fatalf("invalid key accepted: %d", r.Status)
	}
	eventually(t, 5*time.Second, "one webhook", func() bool { return rcv.count() == 1 })
	time.Sleep(300 * time.Millisecond)
	if rcv.count() != 1 {
		t.Fatalf("got %d webhooks for one idempotent event", rcv.count())
	}

	// After the window the key starts a new event (the old one keeps its data, without the key).
	if _, err := e.st.Pool.Exec(context.Background(), "UPDATE events SET created_at = now() - interval '25 hours'"); err != nil {
		t.Fatal(err)
	}
	r := e.call(key, "POST", "/v1/events", body, "Idempotency-Key", "outbox-41")
	if r.Status != 202 || r.Header.Get("Idempotent-Replayed") != "" || r.obj(t)["id"] == first.obj(t)["id"] {
		t.Fatalf("expired key: %d %v %s", r.Status, r.Header, r.Body)
	}
	if again := e.call(key, "POST", "/v1/events", body, "Idempotency-Key", "outbox-41"); again.Header.Get("Idempotent-Replayed") != "true" ||
		again.obj(t)["id"] != r.obj(t)["id"] {
		t.Fatalf("the new event should own the key now: %s", again.Body)
	}
}

func TestRetentionDeletesOldEvents(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.Retention = 50 * time.Millisecond })
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "event_types": []string{"*"}})
	e.startWorker()
	old := e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	eventually(t, 5*time.Second, "the webhook", func() bool { return rcv.count() == 1 })
	time.Sleep(100 * time.Millisecond)
	e.newWorker().Cleanup(context.Background())
	if r := e.call(key, "GET", "/v1/tenants/acme/events/"+old, nil); r.Status != 404 {
		t.Fatalf("old event still there: %d", r.Status)
	}
	if n := len(e.deliveries(key, "acme", "")); n != 0 {
		t.Fatalf("its deliveries remain: %d", n)
	}
}

func TestIngestValidationAndAuth(t *testing.T) {
	e := newEnv(t, nil)
	tenant, key := e.tenant("acme", nil)
	e.tenant("globex", nil)

	cases := []struct {
		name   string
		body   any
		status int
		code   string
		paths  []string
	}{
		{"missing type and payload", map[string]any{}, 422, "validation_failed", []string{"type", "payload"}},
		{"bad type", map[string]any{"type": "Job Completed", "payload": map[string]any{}}, 422, "validation_failed", []string{"type"}},
		{"single word type", map[string]any{"type": "completed", "payload": map[string]any{}}, 422, "validation_failed", []string{"type"}},
		{"reserved type", map[string]any{"type": "relay.ping", "payload": map[string]any{}}, 422, "validation_failed", []string{"type"}},
		{"array payload", map[string]any{"type": "job.done", "payload": []int{1}}, 422, "validation_failed", []string{"payload"}},
		{"unknown field", map[string]any{"type": "job.done", "payload": map[string]any{}, "extra": 1}, 422, "validation_failed", []string{"extra"}},
		{"wrong type of field", map[string]any{"type": 5, "payload": map[string]any{}}, 422, "validation_failed", []string{"type"}},
		{"bad message and targets", map[string]any{"type": "job.done", "payload": map[string]any{},
			"message": map[string]any{"title": "", "url": "javascript:alert(1)"},
			"targets": map[string]any{"emails": []string{"not-an-email"}, "endpoints": []string{"evt_x"}}},
			422, "validation_failed", []string{"message.title", "message.url", "targets.emails[0]", "targets.endpoints[0]"}},
		{"other tenant in body", map[string]any{"type": "job.done", "tenant": "globex", "payload": map[string]any{}}, 422, "validation_failed", []string{"tenant"}},
		{"malformed json", `{"type":`, 400, "invalid_json", nil},
	}
	for _, c := range cases {
		r := e.call(key, "POST", "/v1/events", c.body)
		code, paths := r.problem(t)
		if r.Status != c.status || code != c.code || !sameSet(paths, c.paths) {
			t.Errorf("%s: %d %s %v (body %s)", c.name, r.Status, code, paths, r.Body)
		}
	}

	big := fmt.Sprintf(`{"type":"job.done","payload":{"x":"%s"}}`, strings.Repeat("a", 600<<10))
	if r := e.call(key, "POST", "/v1/events", big); r.Status != 413 {
		t.Errorf("oversized body: %d", r.Status)
	}
	if r := e.call("", "POST", "/v1/events", map[string]any{}); r.Status != 401 || r.Header.Get("WWW-Authenticate") == "" {
		t.Errorf("no key: %d", r.Status)
	}
	if r := e.call("rk_"+strings.Repeat("x", 40), "POST", "/v1/events", map[string]any{}); r.Status != 401 {
		t.Errorf("unknown key: %d", r.Status)
	}
	readOnly := e.createKey(&tenant, []string{"read"})
	if r := e.call(readOnly, "POST", "/v1/events", map[string]any{"type": "job.done", "payload": map[string]any{}}); r.Status != 403 {
		t.Errorf("read-only key ingesting: %d", r.Status)
	}

	// A service key (every tenant) must name the tenant, and the tenant must exist.
	service := e.createKey(nil, []string{"ingest", "read"})
	if r := e.call(service, "POST", "/v1/events", map[string]any{"type": "job.done", "payload": map[string]any{}}); r.Status != 422 {
		t.Errorf("service key without tenant: %d", r.Status)
	}
	if r := e.call(service, "POST", "/v1/events", map[string]any{"type": "job.done", "tenant": "nope", "payload": map[string]any{}}); r.Status != 422 {
		t.Errorf("unknown tenant: %d", r.Status)
	}
	id := e.send(service, map[string]any{"type": "job.done", "tenant": "globex", "payload": map[string]any{}})
	if r := e.call(service, "GET", "/v1/tenants/globex/events/"+id, nil); r.Status != 200 {
		t.Errorf("service key reading: %d", r.Status)
	}
	// Tenant isolation: acme's key can't see globex at all.
	if r := e.call(key, "GET", "/v1/tenants/globex/events/"+id, nil); r.Status != 404 {
		t.Errorf("cross-tenant read: %d", r.Status)
	}
	if r := e.call(key, "GET", "/v1/tenants/acme/events/"+id, nil); r.Status != 404 {
		t.Errorf("globex event through acme: %d", r.Status)
	}
	if r := e.call(key, "GET", "/v1/tenants", nil); r.Status != 403 {
		t.Errorf("tenant key listing tenants: %d", r.Status)
	}
	if r := e.call(key, "GET", "/v1/tenants/acme/deliveries?status=lost", nil); r.Status != 422 {
		t.Errorf("bad status filter: %d", r.Status)
	}
	if r := e.call(key, "GET", "/v1/tenants/acme/deliveries?limit=500", nil); r.Status != 422 {
		t.Errorf("bad limit: %d", r.Status)
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range b {
		if !slices.Contains(a, x) {
			return false
		}
	}
	return true
}

func TestRetriesDeadLetterAndReplay(t *testing.T) {
	e := newEnv(t, nil) // 3 attempts, 100 ms base
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	rcv.status.Store(http.StatusInternalServerError)
	epID, _ := e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "event_types": []string{"*"}})
	e.startWorker()

	start := time.Now()
	evt := e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{"n": 1}})
	var dead []api.DeliveryView
	eventually(t, 10*time.Second, "the delivery to be dead-lettered", func() bool {
		var l api.List[api.DeliveryView]
		e.call(key, "GET", "/v1/tenants/acme/dead-letters", nil).json(t, &l)
		dead = l.Data
		return len(dead) == 1
	})
	if took := time.Since(start); took < 150*time.Millisecond {
		t.Fatalf("retries did not back off (%v)", took)
	}
	if rcv.count() != 3 {
		t.Fatalf("want 3 attempts, receiver saw %d", rcv.count())
	}
	for i, r := range rcv.all() {
		if r.Header.Get("Relay-Attempt") != fmt.Sprint(i+1) || r.Envelope.ID != evt {
			t.Fatalf("attempt %d headers %v", i+1, r.Header)
		}
	}
	d := dead[0]
	if d.Status != "dead" || *d.DeadReason != "max_attempts" || d.Attempts != 3 || *d.LastStatusCode != 500 {
		t.Fatalf("dead delivery: %+v", d)
	}
	var full api.DeliveryView
	e.call(key, "GET", "/v1/tenants/acme/deliveries/"+d.ID, nil).json(t, &full)
	outcomes := []string{}
	for _, a := range full.AttemptLog {
		outcomes = append(outcomes, a.Outcome)
	}
	if strings.Join(outcomes, ",") != "retry,retry,dead" {
		t.Fatalf("attempt outcomes %v", outcomes)
	}
	if r := e.call(key, "POST", "/v1/tenants/acme/deliveries/"+d.ID+"/replay", nil); r.Status != 202 {
		t.Fatalf("replay: %d %s", r.Status, r.Body)
	} else {
		rcv.status.Store(http.StatusNoContent) // the receiver is fixed
		var nd api.DeliveryView
		r.json(t, &nd)
		if nd.ReplayOf == nil || *nd.ReplayOf != d.ID || nd.Attempts != 0 {
			t.Fatalf("new delivery %+v", nd)
		}
	}
	eventually(t, 5*time.Second, "the replay to be delivered", func() bool {
		return countStatus(e.deliveries(key, "acme", "&endpoint_id="+epID), "delivered") == 1
	})
	if r := e.call(key, "POST", "/v1/tenants/acme/deliveries/"+d.ID+"/replay", nil); r.Status != 409 {
		t.Fatalf("second replay: %d", r.Status)
	} else if code, _ := r.problem(t); code != "already_replayed" {
		t.Fatalf("code %s", code)
	}
	var l api.List[api.DeliveryView]
	e.call(key, "GET", "/v1/tenants/acme/dead-letters", nil).json(t, &l)
	if len(l.Data) != 0 {
		t.Fatalf("dead-letter queue should be empty after the replay: %d", len(l.Data))
	}

	// Bulk replay.
	rcv.status.Store(http.StatusBadGateway)
	for i := range 3 {
		e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{"bulk": i}})
	}
	eventually(t, 10*time.Second, "3 more dead letters", func() bool {
		var l api.List[api.DeliveryView]
		e.call(key, "GET", "/v1/tenants/acme/dead-letters", nil).json(t, &l)
		return len(l.Data) == 3
	})
	rcv.status.Store(http.StatusOK)
	r := e.call(key, "POST", "/v1/tenants/acme/dead-letters/replay", map[string]any{"endpoint_id": epID, "limit": 10})
	if r.Status != 202 || r.obj(t)["replayed"].(float64) != 3 {
		t.Fatalf("bulk replay: %d %s", r.Status, r.Body)
	}
	eventually(t, 5*time.Second, "the bulk replays", func() bool {
		return countStatus(e.deliveries(key, "acme", ""), "delivered") == 4
	})
	if code, _ := e.call(key, "POST", "/v1/tenants/acme/deliveries/"+d.ID+"/replay", nil).problem(t); code != "already_replayed" {
		t.Fatal(code)
	}
}

func TestEndpointDisabledAfterRepeatedFailures(t *testing.T) {
	e := newEnv(t, func(c *config.Config) {
		c.RetryMaxAttempts = 8
		c.RetryBase = 50 * time.Millisecond
		c.RetryMaxDelay = 100 * time.Millisecond
		c.EndpointDisableAfter = 4
		c.EndpointDisableMinAge = 0
	})
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	rcv.status.Store(http.StatusServiceUnavailable)
	epID, _ := e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "event_types": []string{"*"}})
	e.startWorker()

	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	var ep api.EndpointView
	eventually(t, 10*time.Second, "the endpoint to be disabled", func() bool {
		e.call(key, "GET", "/v1/tenants/acme/endpoints/"+epID, nil).json(t, &ep)
		return ep.Status == "disabled"
	})
	if ep.DisabledReason == nil || !strings.Contains(*ep.DisabledReason, "every attempt failed") || ep.FailureStreak < 4 {
		t.Fatalf("endpoint %+v", ep)
	}
	eventually(t, 5*time.Second, "its backlog in the dead-letter queue", func() bool {
		ds := e.deliveries(key, "acme", "")
		return countStatus(ds, "dead") == 2
	})
	for _, d := range e.deliveries(key, "acme", "") {
		if *d.DeadReason != "endpoint_disabled" {
			t.Fatalf("dead reason %v", *d.DeadReason)
		}
	}
	seen := rcv.count()
	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	time.Sleep(500 * time.Millisecond)
	if rcv.count() != seen || len(e.deliveries(key, "acme", "")) != 2 {
		t.Fatal("a disabled endpoint got a new delivery")
	}
	// Replaying is refused until the endpoint is enabled again.
	d := e.deliveries(key, "acme", "")[0]
	if code, _ := e.call(key, "POST", "/v1/tenants/acme/deliveries/"+d.ID+"/replay", nil).problem(t); code != "endpoint_disabled" {
		t.Fatalf("replay to a disabled endpoint: %s", code)
	}
	rcv.status.Store(http.StatusOK)
	e.call(key, "POST", "/v1/tenants/acme/endpoints/"+epID+"/enable", nil).json(t, &ep)
	if ep.Status != "active" || ep.FailureStreak != 0 {
		t.Fatalf("enabled endpoint %+v", ep)
	}
	e.call(key, "POST", "/v1/tenants/acme/dead-letters/replay", nil)
	eventually(t, 5*time.Second, "the backlog to be delivered", func() bool {
		return countStatus(e.deliveries(key, "acme", ""), "delivered") == 2
	})
}

func TestGoneDisablesTheEndpointAtOnce(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)
	rcv.status.Store(http.StatusGone)
	epID, _ := e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url(), "event_types": []string{"*"}})
	e.startWorker()
	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	eventually(t, 5*time.Second, "a dead delivery", func() bool { return countStatus(e.deliveries(key, "acme", ""), "dead") == 1 })
	d := e.deliveries(key, "acme", "")[0]
	if *d.DeadReason != "endpoint_gone" || d.Attempts != 1 {
		t.Fatalf("delivery %+v", d)
	}
	var ep api.EndpointView
	e.call(key, "GET", "/v1/tenants/acme/endpoints/"+epID, nil).json(t, &ep)
	if ep.Status != "disabled" {
		t.Fatalf("endpoint %+v", ep)
	}
}

func TestIngestRateLimit(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", map[string]any{"ingest_rate": 1, "ingest_burst": 2})
	_, other := e.tenant("globex", nil)
	ev := map[string]any{"type": "job.done", "payload": map[string]any{}}
	statuses := []int{}
	var last resp
	for range 3 {
		last = e.call(key, "POST", "/v1/events", ev)
		statuses = append(statuses, last.Status)
	}
	if fmt.Sprint(statuses) != "[202 202 429]" {
		t.Fatalf("statuses %v", statuses)
	}
	if last.Header.Get("Retry-After") != "1" {
		t.Fatalf("Retry-After %q", last.Header.Get("Retry-After"))
	}
	if r := e.call(other, "POST", "/v1/events", ev); r.Status != 202 {
		t.Fatalf("another tenant was limited too: %d", r.Status)
	}
	time.Sleep(1100 * time.Millisecond)
	if r := e.call(key, "POST", "/v1/events", ev); r.Status != 202 {
		t.Fatalf("after refill: %d", r.Status)
	}
}

func TestSchedulePayloadSurvivesAsJSON(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", nil)
	payload := map[string]any{"nested": map[string]any{"list": []any{1.5, "two", nil, true}}, "unicode": "Jakarta ✓"}
	id := e.send(key, map[string]any{"type": "job.done", "payload": payload,
		"message": map[string]any{"title": "Done", "body": "All good"}, "targets": map[string]any{"users": []string{"u1"}}})
	var ev api.EventView
	e.call(key, "GET", "/v1/tenants/acme/events/"+id, nil).json(t, &ev)
	var back map[string]any
	_ = json.Unmarshal(ev.Payload, &back)
	if fmt.Sprint(back) != fmt.Sprint(payload) || !strings.Contains(string(ev.Message), `"title":"Done"`) || !strings.Contains(string(ev.Targets), `"u1"`) {
		t.Fatalf("event %s %s %s", ev.Payload, ev.Message, ev.Targets)
	}
}

// Concurrent requests share batches in the API's group commit: duplicates of one Idempotency-Key inside a batch
// still end up as one event, and every other request gets its own.
func TestConcurrentIngestBatches(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", map[string]any{"ingest_rate": 1000, "ingest_burst": 1000})
	const n = 60
	ids := make(chan string, 2*n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			r := e.call(key, "POST", "/v1/events", map[string]any{"type": "job.done", "payload": map[string]any{"same": true}},
				"Idempotency-Key", "same-key")
			if r.Status != 202 {
				t.Errorf("duplicate %d: %d %s", i, r.Status, r.Body)
				return
			}
			ids <- "same:" + r.obj(t)["id"].(string)
		})
		wg.Go(func() {
			r := e.call(key, "POST", "/v1/events", map[string]any{"type": "job.done", "payload": map[string]any{"n": i}},
				"Idempotency-Key", fmt.Sprintf("key-%d", i))
			if r.Status != 202 {
				t.Errorf("distinct %d: %d %s", i, r.Status, r.Body)
				return
			}
			ids <- "own:" + r.obj(t)["id"].(string)
		})
	}
	wg.Wait()
	close(ids)
	same, own := map[string]bool{}, map[string]bool{}
	for id := range ids {
		if k, v, _ := strings.Cut(id, ":"); k == "same" {
			same[v] = true
		} else {
			own[v] = true
		}
	}
	if len(same) != 1 || len(own) != n {
		t.Fatalf("one shared event and %d distinct ones expected, got %d and %d", n, len(same), len(own))
	}
	var count int
	if err := e.st.Pool.QueryRow(context.Background(), "SELECT count(*) FROM events").Scan(&count); err != nil || count != n+1 {
		t.Fatalf("%d events stored (%v)", count, err)
	}
}
