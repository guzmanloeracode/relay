package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/channels"
	pkg "github.com/guzmanloeracode/relay/pkg/webhook"
)

func job(url string, secrets ...string) *channels.Job {
	return &channels.Job{
		DeliveryID: uuid.New(), EventID: uuid.New(), TenantSlug: "acme", EventType: "job.done",
		EventCreatedAt: time.Unix(1700000000, 0), Payload: json.RawMessage(`{"a":1}`), Attempt: 2, URL: url, Secrets: secrets,
	}
}

func TestSendOutcomes(t *testing.T) {
	secret := pkg.NewSecret()
	var status int
	var retryAfter string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := pkg.Verify(secret, r.Header, body, 0); err != nil {
			t.Errorf("verify: %v", err)
		}
		if r.Header.Get("Relay-Attempt") != "2" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("headers %v", r.Header)
		}
		var env Envelope
		if err := json.Unmarshal(body, &env); err != nil || env.Tenant != "acme" || string(env.Data) != `{"a":1}` {
			t.Errorf("envelope %s", body)
		}
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		if status == http.StatusFound {
			w.Header().Set("Location", "https://elsewhere.example")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(strings.Repeat("x", 3000)))
	}))
	defer srv.Close()
	s := New(2*time.Second, true, "Relay/test")

	cases := []struct {
		status    int
		retry     string
		outcome   channels.Outcome
		failure   bool
		disable   bool
		retryWait time.Duration
	}{
		{200, "", channels.Delivered, false, false, 0},
		{204, "", channels.Delivered, false, false, 0},
		{302, "", channels.Retry, true, false, 0}, // redirects are not followed
		{400, "", channels.Retry, true, false, 0},
		{410, "", channels.Dead, false, true, 0},
		{429, "7", channels.Retry, true, false, 7 * time.Second},
		{503, "", channels.Retry, true, false, 0},
	}
	for _, c := range cases {
		status, retryAfter = c.status, c.retry
		res := s.Send(context.Background(), job(srv.URL, secret))
		if res.Outcome != c.outcome || res.StatusCode != c.status || res.EndpointFailure != c.failure ||
			(res.DisableEndpoint != "") != c.disable || res.RetryAfter != c.retryWait {
			t.Errorf("%d: %+v", c.status, res)
		}
		if c.status != 204 && len(res.ResponseBody) != maxResponseKept {
			t.Errorf("%d: kept %d bytes of the response", c.status, len(res.ResponseBody))
		}
	}
}

func TestSendTimeoutAndRefused(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-block }))
	defer srv.Close()
	defer close(block)
	s := New(200*time.Millisecond, true, "Relay/test")
	start := time.Now()
	res := s.Send(context.Background(), job(srv.URL, pkg.NewSecret()))
	if res.Outcome != channels.Retry || res.Error != "timed out" || !res.EndpointFailure || time.Since(start) > time.Second {
		t.Fatalf("timeout: %+v after %v", res, time.Since(start))
	}
	res = s.Send(context.Background(), job("http://127.0.0.1:1/", pkg.NewSecret()))
	if res.Outcome != channels.Retry || res.Error != "connection refused" {
		t.Fatalf("refused: %+v", res)
	}
	// Without AllowPrivateNetworks the dialer refuses loopback even when the URL got past validation.
	guarded := New(time.Second, false, "Relay/test")
	res = guarded.Send(context.Background(), job(srv.URL, pkg.NewSecret()))
	if res.Outcome != channels.Retry || !strings.Contains(res.Error, "private") {
		t.Fatalf("guarded: %+v", res)
	}
	if res := s.Send(context.Background(), job(srv.URL, "whsec_!!!")); res.Outcome != channels.Dead {
		t.Fatalf("bad secret: %+v", res)
	}
}

func TestIsPublic(t *testing.T) {
	for addr, want := range map[string]bool{
		"8.8.8.8": true, "2606:4700::1111": true, "104.16.0.1": true,
		"127.0.0.1": false, "10.1.2.3": false, "172.16.0.1": false, "192.168.1.1": false, "169.254.169.254": false,
		"100.64.0.1": false, "0.0.0.0": false, "::1": false, "fc00::1": false, "fe80::1": false, "::ffff:10.0.0.1": false,
		"64:ff9b::a00:1": false, "224.0.0.1": false, "198.18.0.1": false,
	} {
		if got := IsPublic(netip.MustParseAddr(addr)); got != want {
			t.Errorf("IsPublic(%s) = %v", addr, got)
		}
	}
}

func TestValidateURL(t *testing.T) {
	ok := []string{"https://example.com/hooks", "http://example.com:8080/x?y=1"}
	bad := []string{"", "example.com", "ftp://example.com", "https://u:p@example.com", "https://example.com/#x",
		"http://localhost:3000", "http://api.localhost", "http://10.0.0.1", "http://[::1]/", "http://db.internal/x",
		"https://" + strings.Repeat("a", 2100) + ".com"}
	for _, u := range ok {
		if err := ValidateURL(u, false); err != nil {
			t.Errorf("%s: %v", u, err)
		}
	}
	for _, u := range bad {
		if err := ValidateURL(u, false); err == nil {
			t.Errorf("%s accepted", u)
		}
	}
	if err := ValidateURL("http://127.0.0.1:9000/x", true); err != nil {
		t.Errorf("private allowed: %v", err)
	}
}

func TestRetryAfter(t *testing.T) {
	if retryAfter("") != 0 || retryAfter("abc") != 0 || retryAfter("-5") != 0 || retryAfter("30") != 30*time.Second {
		t.Fatal("seconds form")
	}
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if d := retryAfter(future); d < 80*time.Second || d > 91*time.Second {
		t.Fatalf("date form: %v", d)
	}
}
