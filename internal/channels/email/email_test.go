package email

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/channels"
)

func testJob() *channels.Job {
	return &channels.Job{DeliveryID: uuid.New(), Recipient: "ops@example.com",
		Message: &channels.Message{Title: "Job J-1 done ✓", Body: "Line one\nline two\n\n<b>Second</b> paragraph", URL: "https://x.example/j/1"}}
}

func TestResend(t *testing.T) {
	var got map[string]any
	var hdr http.Header
	status := 200
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr = r.Header.Clone()
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		if r.URL.Path != "/emails" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"id":"re_1","message":"nope"}`))
	}))
	defer srv.Close()
	s := NewSender(Resend{APIKey: "re_key", BaseURL: srv.URL + "/"}, "Relay <relay@example.com>")

	res := s.Send(context.Background(), testJob())
	if res.Outcome != channels.Delivered || hdr.Get("Authorization") != "Bearer re_key" || !strings.HasPrefix(hdr.Get("Idempotency-Key"), "dlv_") {
		t.Fatalf("send: %+v %v", res, hdr)
	}
	if got["subject"] != "Job J-1 done ✓" || got["to"].([]any)[0] != "ops@example.com" || !strings.Contains(got["html"].(string), "&lt;b&gt;Second&lt;/b&gt;") ||
		!strings.Contains(got["html"].(string), "Line one<br>line two") || !strings.HasSuffix(got["text"].(string), "\n\nhttps://x.example/j/1") {
		t.Fatalf("payload %v", got)
	}
	for code, want := range map[int]channels.Outcome{422: channels.Dead, 400: channels.Dead, 429: channels.Retry, 500: channels.Retry, 401: channels.Retry} {
		status = code
		if res := s.Send(context.Background(), testJob()); res.Outcome != want || res.StatusCode != code {
			t.Errorf("%d: %+v", code, res)
		}
	}
	j := testJob()
	j.Message = nil
	if res := s.Send(context.Background(), j); res.Outcome != channels.Skipped {
		t.Fatalf("no message: %+v", res)
	}
	j = testJob()
	j.Message.HTML = "<h1>Custom</h1>"
	status = 200
	s.Send(context.Background(), j)
	if got["html"] != "<h1>Custom</h1>" {
		t.Fatalf("custom html %v", got["html"])
	}
}

// fakeSMTP is just enough of an SMTP server: it records one message and can reject recipients.
type fakeSMTP struct {
	ln     net.Listener
	mu     sync.Mutex
	data   string
	user   string
	reject string
}

func newFakeSMTP(t *testing.T) *fakeSMTP {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSMTP{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakeSMTP) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	say := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	say("220 fake ESMTP")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(cmd, "EHLO"):
			say("250-fake")
			say("250 AUTH PLAIN")
		case strings.HasPrefix(cmd, "AUTH PLAIN"):
			f.mu.Lock()
			f.user = strings.TrimSpace(line[len("AUTH PLAIN"):])
			f.mu.Unlock()
			say("235 ok")
		case strings.HasPrefix(cmd, "MAIL FROM"):
			say("250 ok")
		case strings.HasPrefix(cmd, "RCPT TO"):
			if f.reject != "" && strings.Contains(line, f.reject) {
				say("550 no such user")
			} else {
				say("250 ok")
			}
		case cmd == "DATA":
			say("354 go")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil || l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			f.mu.Lock()
			f.data = b.String()
			f.mu.Unlock()
			say("250 queued")
		case cmd == "QUIT":
			say("221 bye")
			return
		default:
			say("250 ok")
		}
	}
}

func TestSMTP(t *testing.T) {
	f := newFakeSMTP(t)
	m, err := NewSMTP("smtp://relay:secret@" + f.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// net/smtp refuses PLAIN auth without TLS except on localhost, which this is.
	s := NewSender(m, "Relay <relay@example.com>")
	if res := s.Send(context.Background(), testJob()); res.Outcome != channels.Delivered {
		t.Fatalf("send: %+v", res)
	}
	f.mu.Lock()
	data, user := f.data, f.user
	f.mu.Unlock()
	for _, want := range []string{"To: ops@example.com", "Subject: =?utf-8?q?Job_J-1_done_=E2=9C=93?=", "multipart/alternative", "text/html", "Message-ID: <"} {
		if !strings.Contains(data, want) {
			t.Errorf("message lacks %q:\n%s", want, data)
		}
	}
	if user == "" {
		t.Error("no AUTH PLAIN")
	}
	f.reject = "ops@example.com"
	if res := s.Send(context.Background(), testJob()); res.Outcome != channels.Dead || res.StatusCode != 550 {
		t.Fatalf("rejected recipient: %+v", res)
	}
	if _, err := NewSMTP("http://x"); err == nil {
		t.Fatal("bad scheme accepted")
	}
	down, _ := NewSMTP("smtp://127.0.0.1:1")
	if res := NewSender(down, "relay@example.com").Send(context.Background(), testJob()); res.Outcome != channels.Retry {
		t.Fatalf("server down: %+v", res)
	}
}

func TestLogMailerAndErrors(t *testing.T) {
	s := NewSender(LogMailer{Log: slog.New(slog.DiscardHandler)}, "relay@example.com")
	if res := s.Send(context.Background(), testJob()); res.Outcome != channels.Delivered || res.StatusCode != 250 {
		t.Fatalf("log mailer: %+v", res)
	}
	perm := &PermanentError{Code: 550, Err: errors.New("x")}
	if !errors.Is(perm, perm.Err) || perm.Error() != "x" {
		t.Fatal("PermanentError")
	}
	if code, err := smtpResult(errors.New("plain")); code != 0 || err == nil {
		t.Fatal("non-SMTP error")
	}
}
