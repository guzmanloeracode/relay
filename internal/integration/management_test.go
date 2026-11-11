package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/guzmanloeracode/relay/internal/api"
	"github.com/guzmanloeracode/relay/internal/config"
)

func TestTenantsAndKeys(t *testing.T) {
	e := newEnv(t, nil)
	r := e.call(e.adminKey, "POST", "/v1/tenants", map[string]any{"slug": "acme-air", "name": "Acme Air",
		"limits": map[string]any{"ingest_rate": 500, "max_in_flight": 32}})
	if r.Status != 201 || r.Header.Get("Location") != "/v1/tenants/acme-air" {
		t.Fatalf("create: %d %s", r.Status, r.Body)
	}
	var ten api.TenantView
	r.json(t, &ten)
	if ten.Limits.IngestRate != 500 || ten.Limits.IngestBurst != 200 || ten.Limits.MaxInFlight != 32 || ten.Disabled {
		t.Fatalf("tenant %+v", ten)
	}
	if r := e.call(e.adminKey, "POST", "/v1/tenants", map[string]any{"slug": "acme-air", "name": "Again"}); r.Status != 409 {
		t.Fatalf("duplicate slug: %d", r.Status)
	}
	if r := e.call(e.adminKey, "POST", "/v1/tenants", map[string]any{"slug": "Acme Air", "name": "", "limits": map[string]any{"ingest_rate": 0}}); r.Status != 422 {
		t.Fatalf("invalid tenant: %d", r.Status)
	} else if _, paths := r.problem(t); !sameSet(paths, []string{"slug", "name", "limits.ingest_rate"}) {
		t.Fatalf("paths %v", paths)
	}

	// Pagination: newest first, cursor to the next page.
	for i := range 3 {
		e.call(e.adminKey, "POST", "/v1/tenants", map[string]any{"slug": fmt.Sprintf("t%d", i), "name": "T"})
	}
	var page1, page2 api.List[api.TenantView]
	e.call(e.adminKey, "GET", "/v1/tenants?limit=3", nil).json(t, &page1)
	if len(page1.Data) != 3 || page1.Data[0].Slug != "t2" || page1.NextCursor == nil {
		t.Fatalf("page 1 %+v", page1)
	}
	e.call(e.adminKey, "GET", "/v1/tenants?limit=3&cursor="+*page1.NextCursor, nil).json(t, &page2)
	if len(page2.Data) != 1 || page2.Data[0].Slug != "acme-air" || page2.NextCursor != nil {
		t.Fatalf("page 2 %+v", page2)
	}
	if r := e.call(e.adminKey, "GET", "/v1/tenants?cursor=evt_nope", nil); r.Status != 422 {
		t.Fatalf("bad cursor: %d", r.Status)
	}

	// Keys: the secret is shown once, scopes are enforced, revocation is immediate.
	r = e.call(e.adminKey, "POST", "/v1/tenants/acme-air/keys", map[string]any{"name": "reader", "scopes": []string{"read"}})
	if r.Status != 201 {
		t.Fatalf("key: %d %s", r.Status, r.Body)
	}
	var k api.KeyView
	r.json(t, &k)
	if !strings.HasPrefix(k.Key, k.Prefix) || *k.Tenant != "acme-air" {
		t.Fatalf("key %+v", k)
	}
	if r := e.call(e.adminKey, "POST", "/v1/tenants/acme-air/keys", map[string]any{"name": "x", "scopes": []string{"admin"}}); r.Status != 422 {
		t.Fatalf("admin scope on a tenant key: %d", r.Status)
	}
	var keys api.List[api.KeyView]
	e.call(e.adminKey, "GET", "/v1/tenants/acme-air/keys", nil).json(t, &keys)
	if len(keys.Data) != 1 || keys.Data[0].Key != "" {
		t.Fatalf("listed keys %+v", keys.Data)
	}
	if r := e.call(k.Key, "GET", "/v1/tenants/acme-air", nil); r.Status != 200 {
		t.Fatalf("reader reading its tenant: %d", r.Status)
	}
	if r := e.call(k.Key, "PATCH", "/v1/tenants/acme-air", map[string]any{"name": "X"}); r.Status != 403 {
		t.Fatalf("reader changing its tenant: %d", r.Status)
	}
	if r := e.call(e.adminKey, "DELETE", "/v1/tenants/acme-air/keys/"+k.ID, nil); r.Status != 204 {
		t.Fatalf("revoke: %d", r.Status)
	}
	if r := e.call(k.Key, "GET", "/v1/tenants/acme-air", nil); r.Status != 401 {
		t.Fatalf("revoked key: %d", r.Status)
	}

	// Service keys.
	r = e.call(e.adminKey, "POST", "/v1/keys", map[string]any{"name": "FieldOps worker", "scopes": []string{"ingest", "read", "manage"}})
	var svc api.KeyView
	r.json(t, &svc)
	if r.Status != 201 || svc.Tenant != nil {
		t.Fatalf("service key: %d %+v", r.Status, svc)
	}
	if r := e.call(svc.Key, "GET", "/v1/tenants/t0/endpoints", nil); r.Status != 200 {
		t.Fatalf("service key on any tenant: %d", r.Status)
	}
	if r := e.call(svc.Key, "POST", "/v1/keys", map[string]any{"name": "escalate"}); r.Status != 403 {
		t.Fatalf("service key creating keys: %d", r.Status)
	}
	e.call(e.adminKey, "GET", "/v1/keys", nil).json(t, &keys)
	if len(keys.Data) != 2 { // the admin key and the service key
		t.Fatalf("global keys: %d", len(keys.Data))
	}

	// Disabling a tenant stops its keys and its ingestion.
	_, tkey := e.tenant("globex", nil)
	r = e.call(e.adminKey, "PATCH", "/v1/tenants/globex", map[string]any{"disabled": true, "limits": map[string]any{"delivery_rate": 7}})
	r.json(t, &ten)
	if !ten.Disabled || ten.Limits.DeliveryRate != 7 {
		t.Fatalf("patched %+v", ten)
	}
	time.Sleep(50 * time.Millisecond) // the invalidation travels through Redis
	if r := e.call(tkey, "GET", "/v1/tenants/globex", nil); r.Status != 403 {
		t.Fatalf("disabled tenant's key: %d", r.Status)
	}
	if r := e.call(svc.Key, "POST", "/v1/events", map[string]any{"tenant": "globex", "type": "a.b", "payload": map[string]any{}}); r.Status != 403 {
		t.Fatalf("ingest for a disabled tenant: %d %s", r.Status, r.Body)
	}
	e.call(e.adminKey, "PATCH", "/v1/tenants/globex", map[string]any{"disabled": false})
	time.Sleep(50 * time.Millisecond)
	if r := e.call(tkey, "GET", "/v1/tenants/globex", nil); r.Status != 200 {
		t.Fatalf("re-enabled tenant's key: %d", r.Status)
	}
}

func TestEndpointsAndSubscriptions(t *testing.T) {
	e := newEnv(t, nil)
	_, key := e.tenant("acme", nil)
	rcv := newReceiver(t)

	cases := []struct {
		body  map[string]any
		paths []string
	}{
		{map[string]any{"channel": "fax"}, []string{"channel"}},
		{map[string]any{"channel": "webhook"}, []string{"url"}},
		{map[string]any{"channel": "webhook", "url": "ftp://example.com"}, []string{"url"}},
		{map[string]any{"channel": "webhook", "url": "https://user:pw@example.com/x"}, []string{"url"}},
		{map[string]any{"channel": "email", "url": "https://example.com", "email_to": []string{"x"}}, []string{"url", "email_to[0]"}},
		{map[string]any{"channel": "push", "max_in_flight": 100, "event_types": []string{"Job", "job.*", "*", "job.part.*"}}, []string{"max_in_flight", "event_types[0]"}},
	}
	for _, c := range cases {
		r := e.call(key, "POST", "/v1/tenants/acme/endpoints", c.body)
		if _, paths := r.problem(t); r.Status != 422 || !sameSet(paths, c.paths) {
			t.Errorf("%v: %d %v", c.body, r.Status, paths)
		}
	}

	epID, secret := e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": rcv.url()})
	var ep api.EndpointView
	e.call(key, "GET", "/v1/tenants/acme/endpoints/"+epID, nil).json(t, &ep)
	if ep.Secret != "" || ep.Status != "active" || ep.MaxInFlight != 4 {
		t.Fatalf("endpoint %+v", ep)
	}
	if got := e.call(key, "GET", "/v1/tenants/acme/endpoints/"+epID+"/secret", nil).obj(t)["secret"]; got != secret {
		t.Fatalf("secret %v", got)
	}

	// No subscription yet: nothing is delivered.
	e.startWorker()
	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	r := e.call(key, "POST", "/v1/tenants/acme/subscriptions", map[string]any{"endpoint_id": epID, "event_types": []string{"job.done"}})
	if r.Status != 201 {
		t.Fatalf("subscribe: %d %s", r.Status, r.Body)
	}
	var sub api.SubscriptionView
	r.json(t, &sub)
	if r := e.call(key, "POST", "/v1/tenants/acme/subscriptions", map[string]any{"endpoint_id": "ep_00000000000000000000000000", "event_types": []string{"*"}}); r.Status != 422 {
		t.Fatalf("subscription to a missing endpoint: %d", r.Status)
	}
	time.Sleep(100 * time.Millisecond) // route caches are invalidated through Redis
	rcv.setSecrets(secret)
	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	eventually(t, 5*time.Second, "the subscribed event", func() bool { return rcv.count() == 1 })

	// Rotation: for the grace period both secrets sign, so a receiver holding either accepts.
	var rotated api.EndpointView
	e.call(key, "POST", "/v1/tenants/acme/endpoints/"+epID+"/rotate-secret", nil).json(t, &rotated)
	if rotated.Secret == "" || rotated.Secret == secret {
		t.Fatalf("rotated %+v", rotated)
	}
	rcv.setSecrets(secret, rotated.Secret)
	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	eventually(t, 5*time.Second, "the webhook signed twice", func() bool { return rcv.count() == 2 })
	if sig := rcv.all()[1].Header.Get("webhook-signature"); len(strings.Fields(sig)) != 2 {
		t.Fatalf("signature header %q", sig)
	}

	// Patch, list, filter, delete.
	e.call(key, "PATCH", "/v1/tenants/acme/endpoints/"+epID, map[string]any{"description": "renamed", "max_in_flight": 9}).json(t, &ep)
	if ep.Description != "renamed" || ep.MaxInFlight != 9 || *ep.URL != rcv.url() {
		t.Fatalf("patched %+v", ep)
	}
	if r := e.call(key, "PATCH", "/v1/tenants/acme/endpoints/"+epID, map[string]any{"url": "http://[::1"}); r.Status != 422 {
		t.Fatalf("bad url patch: %d", r.Status)
	}
	e.call(key, "PATCH", "/v1/tenants/acme/subscriptions/"+sub.ID, map[string]any{"event_types": []string{"job.*", "invoice.paid"}}).json(t, &sub)
	if len(sub.EventTypes) != 2 {
		t.Fatalf("sub %+v", sub)
	}
	var subs api.List[api.SubscriptionView]
	e.call(key, "GET", "/v1/tenants/acme/subscriptions?endpoint_id="+epID, nil).json(t, &subs)
	if len(subs.Data) != 1 {
		t.Fatalf("subs %+v", subs)
	}
	if r := e.call(key, "DELETE", "/v1/tenants/acme/subscriptions/"+sub.ID, nil); r.Status != 204 {
		t.Fatalf("delete sub: %d", r.Status)
	}
	if r := e.call(key, "GET", "/v1/tenants/acme/subscriptions/"+sub.ID, nil); r.Status != 404 {
		t.Fatalf("deleted sub: %d", r.Status)
	}
	if r := e.call(key, "POST", "/v1/tenants/acme/endpoints/"+epID+"/disable", nil); r.Status != 200 {
		t.Fatalf("disable: %d", r.Status)
	}
	if r := e.call(key, "DELETE", "/v1/tenants/acme/endpoints/"+epID, nil); r.Status != 204 {
		t.Fatalf("delete endpoint: %d", r.Status)
	}
	if r := e.call(key, "GET", "/v1/tenants/acme/endpoints/"+epID, nil); r.Status != 404 {
		t.Fatalf("deleted endpoint: %d", r.Status)
	}
	if r := e.call(key, "GET", "/v1/tenants/acme/deliveries", nil); len(r.obj(t)["data"].([]any)) != 0 {
		t.Fatal("deliveries of a deleted endpoint remain")
	}
	if r := e.call(key, "GET", "/v1/tenants/acme/endpoints/not-an-id", nil); r.Status != 404 {
		t.Fatalf("malformed id: %d", r.Status)
	}

	// Push tokens.
	r = e.call(key, "PUT", "/v1/tenants/acme/push-tokens", map[string]any{"user": "u1", "token": "ExpoPushToken[abc]", "platform": "ios"})
	ptk := r.obj(t)["id"].(string)
	if r := e.call(key, "PUT", "/v1/tenants/acme/push-tokens", map[string]any{"user": "u2", "token": "ExpoPushToken[abc]"}); r.obj(t)["user"] != "u2" {
		t.Fatal("re-registering a token should move it to the new user")
	}
	if r := e.call(key, "DELETE", "/v1/tenants/acme/push-tokens/"+ptk, nil); r.Status != 204 {
		t.Fatalf("delete token: %d", r.Status)
	}
	if r := e.call(key, "DELETE", "/v1/tenants/acme/push-tokens/"+ptk, nil); r.Status != 404 {
		t.Fatalf("delete token twice: %d", r.Status)
	}
}

func TestPrivateAddressesRefused(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.AllowPrivateNetworks = false })
	_, key := e.tenant("acme", nil)
	for _, u := range []string{"http://127.0.0.1:8080/x", "http://localhost/x", "http://10.0.0.5/x", "http://[::1]/x", "http://169.254.169.254/latest", "http://metadata.internal/x"} {
		if r := e.call(key, "POST", "/v1/tenants/acme/endpoints", map[string]any{"channel": "webhook", "url": u}); r.Status != 422 {
			t.Errorf("%s accepted: %d", u, r.Status)
		}
	}
	// A public name that resolves to a private address passes validation but is refused when dialed.
	rcv := newReceiver(t)
	port := rcv.srv.URL[strings.LastIndex(rcv.srv.URL, ":"):]
	epID, _ := e.endpoint(key, "acme", map[string]any{"channel": "webhook", "url": "http://localtest.me" + port + "/x", "event_types": []string{"*"}})
	e.startWorker()
	e.send(key, map[string]any{"type": "job.done", "payload": map[string]any{}})
	eventually(t, 10*time.Second, "the attempt to be refused", func() bool {
		ds := e.deliveries(key, "acme", "&endpoint_id="+epID)
		return len(ds) == 1 && ds[0].LastError != nil
	})
	d := e.deliveries(key, "acme", "")[0]
	if !strings.Contains(*d.LastError, "private") && !strings.Contains(*d.LastError, "DNS") {
		t.Fatalf("last error %q", *d.LastError)
	}
	if rcv.count() != 0 {
		t.Fatal("the private receiver was called")
	}
}

func TestServicePages(t *testing.T) {
	e := newEnv(t, nil)
	for path, want := range map[string]string{
		"/":             "Relay",
		"/docs":         "api-reference",
		"/openapi.yaml": "openapi: 3.1.0",
	} {
		res, err := http.Get(e.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(string(body), want) {
			t.Errorf("%s: %d", path, res.StatusCode)
		}
	}
	if h := e.call("", "GET", "/healthz", nil).obj(t); h["status"] != "ok" || h["commit"] != "test" {
		t.Fatalf("healthz %v", h)
	}
	if h := e.call("", "GET", "/readyz", nil).obj(t); h["checks"].(map[string]any)["redis"] != "ok" {
		t.Fatalf("readyz %v", h)
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+"/v1/nope", nil)
	req.Header.Set("Authorization", "Bearer "+e.adminKey)
	if res, err := http.DefaultClient.Do(req); err != nil || res.StatusCode != http.StatusNotFound || res.Header.Get("Content-Type") != "application/problem+json" {
		t.Fatalf("unknown route: %v %v", res, err)
	}
}
