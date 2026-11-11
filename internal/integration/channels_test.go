package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/channels/email"
	"github.com/guzmanloeracode/relay/internal/config"
)

type captureMailer struct {
	mu   sync.Mutex
	sent []email.Mail
	fail map[string]error
}

func (c *captureMailer) Name() string { return "capture" }
func (c *captureMailer) Send(_ context.Context, m email.Mail) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.fail[m.To]; err != nil {
		return 550, err
	}
	c.sent = append(c.sent, m)
	return 250, nil
}
func (c *captureMailer) to() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, m := range c.sent {
		out = append(out, m.To)
	}
	return out
}

func TestEmailChannel(t *testing.T) {
	e := newEnv(t, nil)
	mailer := &captureMailer{fail: map[string]error{
		"bounce@example.com": &email.PermanentError{Code: 550, Err: errors.New("mailbox does not exist")},
	}}
	e.opts.Senders = map[string]channels.Sender{channels.Email: email.NewSender(mailer, "Relay <relay@example.com>")}
	_, key := e.tenant("acme", nil)
	e.endpoint(key, "acme", map[string]any{"channel": "email", "email_to": []string{"ops@example.com", "boss@example.com"}, "event_types": []string{"invoice.*"}})
	e.startWorker()

	msg := map[string]any{"title": "Invoice INV-7 paid", "body": "Paid in full.\n\nThanks!", "url": "https://app.example.com/invoices/7"}
	e.send(key, map[string]any{"type": "invoice.paid", "payload": map[string]any{}, "message": msg})
	eventually(t, 5*time.Second, "two emails to the default recipients", func() bool { return len(mailer.to()) == 2 })
	m := mailer.sent[0]
	if m.Subject != "Invoice INV-7 paid" || !strings.Contains(m.Text, "Paid in full.") || !strings.Contains(m.Text, "https://app.example.com/invoices/7") ||
		!strings.Contains(m.HTML, "<p>Paid in full.</p>") || !strings.HasPrefix(m.IdempotencyKey, "dlv_") {
		t.Fatalf("mail %+v", m)
	}

	// Recipients named in the event replace the default list; a permanent rejection is dead at once.
	e.send(key, map[string]any{"type": "invoice.paid", "payload": map[string]any{}, "message": msg,
		"targets": map[string]any{"emails": []string{"customer@example.com", "bounce@example.com"}}})
	eventually(t, 5*time.Second, "the third email", func() bool { return len(mailer.to()) == 3 })
	eventually(t, 5*time.Second, "the bounce dead-lettered", func() bool { return countStatus(e.deliveries(key, "acme", ""), "dead") == 1 })
	for _, d := range e.deliveries(key, "acme", "&status=dead") {
		if *d.Recipient != "bounce@example.com" || d.Attempts != 1 || *d.DeadReason != "rejected" {
			t.Fatalf("bounce %+v", d)
		}
	}
	if got := mailer.to()[2]; got != "customer@example.com" {
		t.Fatalf("third email to %s", got)
	}

	// No message: the email route is skipped at routing time.
	id := e.send(key, map[string]any{"type": "invoice.paid", "payload": map[string]any{}})
	time.Sleep(300 * time.Millisecond)
	r := e.call(key, "GET", "/v1/tenants/acme/events/"+id, nil).obj(t)
	if r["delivery_count"].(float64) != 0 {
		t.Fatalf("an event without a message became emails: %v", r)
	}
}

// fakeExpo answers like the Expo Push API: tokens containing "gone" are DeviceNotRegistered.
type fakeExpo struct {
	mu       sync.Mutex
	sent     []map[string]any
	receipts map[string]map[string]any
	srv      *httptest.Server
}

func newFakeExpo(t *testing.T) *fakeExpo {
	f := &fakeExpo{receipts: map[string]map[string]any{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/send":
			var msgs []map[string]any
			_ = json.Unmarshal(body, &msgs)
			var tickets []map[string]any
			for i, m := range msgs {
				f.sent = append(f.sent, m)
				to := m["to"].(string)
				if strings.Contains(to, "gone") {
					tickets = append(tickets, map[string]any{"status": "error", "message": "not registered", "details": map[string]any{"error": "DeviceNotRegistered"}})
					continue
				}
				id := "ticket-" + to + "-" + string(rune('a'+i))
				tickets = append(tickets, map[string]any{"status": "ok", "id": id})
				if strings.Contains(to, "uninstalled") {
					f.receipts[id] = map[string]any{"status": "error", "details": map[string]any{"error": "DeviceNotRegistered"}}
				} else {
					f.receipts[id] = map[string]any{"status": "ok"}
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": tickets})
		case "/receipts":
			var in struct{ IDs []string }
			_ = json.Unmarshal(body, &in)
			out := map[string]any{}
			for _, id := range in.IDs {
				if r, ok := f.receipts[id]; ok {
					out[id] = r
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": out})
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func TestPushChannel(t *testing.T) {
	expo := newFakeExpo(t)
	e := newEnv(t, func(c *config.Config) {
		c.ExpoPushURL = expo.srv.URL + "/send"
		c.ExpoReceiptURL = expo.srv.URL + "/receipts"
	})
	_, key := e.tenant("acme", nil)
	e.endpoint(key, "acme", map[string]any{"channel": "push", "event_types": []string{"job.assigned"}})
	for _, tok := range []string{"ExponentPushToken[phone-ok]", "ExponentPushToken[tablet-gone]", "ExponentPushToken[old-uninstalled]"} {
		if r := e.call(key, "PUT", "/v1/tenants/acme/push-tokens", map[string]any{"user": "tom", "token": tok, "platform": "android"}); r.Status != 200 {
			t.Fatalf("register: %d %s", r.Status, r.Body)
		}
	}
	if r := e.call(key, "PUT", "/v1/tenants/acme/push-tokens", map[string]any{"user": "tom", "token": "not-a-token"}); r.Status != 422 {
		t.Fatalf("bad token: %d", r.Status)
	}
	e.startWorker()

	e.send(key, map[string]any{"type": "job.assigned", "payload": map[string]any{"job": "J-9"},
		"message": map[string]any{"title": "New job J-9", "body": "12 Oak St, 10:00", "url": "fieldops://jobs/J-9"},
		"targets": map[string]any{"users": []string{"tom", "nobody"}}})
	eventually(t, 5*time.Second, "both deliveries to finish", func() bool {
		ds := e.deliveries(key, "acme", "")
		return countStatus(ds, "delivered") == 1 && countStatus(ds, "skipped") == 1
	})
	for _, d := range e.deliveries(key, "acme", "") {
		if *d.Recipient == "nobody" && *d.DeadReason != "no_devices" {
			t.Fatalf("user without devices: %+v", d)
		}
	}
	expo.mu.Lock()
	first := expo.sent[0]
	n := len(expo.sent)
	expo.mu.Unlock()
	if n != 3 || first["title"] != "New job J-9" || first["data"].(map[string]any)["url"] != "fieldops://jobs/J-9" {
		t.Fatalf("expo got %d messages, first %v", n, first)
	}

	// The DeviceNotRegistered ticket removed its token right away; the receipt check removes the other.
	tokens := func() int {
		r := e.call(key, "GET", "/v1/tenants/acme/push-tokens?user=tom", nil).obj(t)
		return len(r["data"].([]any))
	}
	if got := tokens(); got != 2 {
		t.Fatalf("after the send: %d tokens", got)
	}
	e.newWorker().CheckReceipts(context.Background())
	if got := tokens(); got != 1 {
		t.Fatalf("after the receipts: %d tokens", got)
	}
	var left []string
	if err := e.st.Pool.QueryRow(context.Background(), "SELECT array_agg(token) FROM push_tokens").Scan(&left); err != nil || left[0] != "ExponentPushToken[phone-ok]" {
		t.Fatalf("remaining %v %v", left, err)
	}
	var checked int
	_ = e.st.Pool.QueryRow(context.Background(), "SELECT count(*) FROM push_tickets WHERE checked_at IS NOT NULL").Scan(&checked)
	if checked != 2 {
		t.Fatalf("checked tickets %d", checked)
	}
}
