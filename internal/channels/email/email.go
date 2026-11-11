// Package email delivers event messages by email through a provider adapter: Resend's HTTP API, any SMTP
// server, or a log-only mailer that sends nothing (the public demo).
package email

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/ids"
)

// Mail is one message to one recipient.
type Mail struct {
	From, To, Subject, Text, HTML string
	// IdempotencyKey is the delivery ID: providers that support it drop a resend of the same attempt.
	IdempotencyKey string
}

// PermanentError means the provider rejected the message for good (an invalid address, say); retrying the
// same message can't succeed.
type PermanentError struct {
	Code int
	Err  error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Mailer is a provider adapter.
type Mailer interface {
	Name() string
	Send(ctx context.Context, m Mail) (code int, err error)
}

// Sender turns jobs into mail.
type Sender struct {
	mailer Mailer
	from   string
}

// NewSender returns a Sender that sends from the given address.
func NewSender(m Mailer, from string) *Sender { return &Sender{mailer: m, from: from} }

// Send emails the job's message to its recipient.
func (s *Sender) Send(ctx context.Context, j *channels.Job) channels.Result {
	if j.Message == nil {
		return channels.Result{Outcome: channels.Skipped, Reason: "no_message", Error: "the event has no message"}
	}
	m := Mail{
		From: s.from, To: j.Recipient, Subject: j.Message.Title,
		Text:           textBody(j.Message),
		HTML:           htmlBody(j.Message),
		IdempotencyKey: ids.Encode(ids.Delivery, j.DeliveryID),
	}
	code, err := s.mailer.Send(ctx, m)
	if err == nil {
		return channels.Result{Outcome: channels.Delivered, StatusCode: code}
	}
	var perm *PermanentError
	if errors.As(err, &perm) {
		return channels.Result{Outcome: channels.Dead, StatusCode: perm.Code, Error: err.Error(), Reason: "rejected"}
	}
	return channels.Result{Outcome: channels.Retry, StatusCode: code, Error: err.Error()}
}

func textBody(m *channels.Message) string {
	var b strings.Builder
	b.WriteString(m.Body)
	if m.URL != "" {
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(m.URL)
	}
	return b.String()
}

func htmlBody(m *channels.Message) string {
	if m.HTML != "" {
		return m.HTML
	}
	var b strings.Builder
	b.WriteString(`<!doctype html><html><body style="font-family:system-ui,sans-serif;line-height:1.5">`)
	for p := range strings.SplitSeq(m.Body, "\n\n") {
		if p = strings.TrimSpace(p); p != "" {
			b.WriteString("<p>" + strings.ReplaceAll(html.EscapeString(p), "\n", "<br>") + "</p>")
		}
	}
	if m.URL != "" {
		b.WriteString(`<p><a href="` + html.EscapeString(m.URL) + `">` + html.EscapeString(m.URL) + "</a></p>")
	}
	b.WriteString("</body></html>")
	return b.String()
}

// LogMailer sends nothing: it logs that a message would have gone out. The public demo uses it.
type LogMailer struct{ Log *slog.Logger }

// Name implements Mailer.
func (LogMailer) Name() string { return "log" }

// Send implements Mailer.
func (l LogMailer) Send(_ context.Context, m Mail) (int, error) {
	domain := m.To[strings.LastIndexByte(m.To, '@')+1:]
	l.Log.Info("email captured (EMAIL_PROVIDER=log)", "to_domain", domain, "subject_len", len(m.Subject),
		"delivery_id", m.IdempotencyKey)
	return 250, nil
}

// Resend sends through the Resend API.
type Resend struct {
	APIKey, BaseURL string
	Client          *http.Client
}

// Name implements Mailer.
func (Resend) Name() string { return "resend" }

// Send implements Mailer.
func (r Resend) Send(ctx context.Context, m Mail) (int, error) {
	body, _ := json.Marshal(map[string]any{
		"from": m.From, "to": []string{m.To}, "subject": m.Subject, "text": m.Text, "html": m.HTML,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(r.BaseURL, "/")+"/emails", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+r.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.IdempotencyKey)
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return resp.StatusCode, nil
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity:
		return resp.StatusCode, &PermanentError{Code: resp.StatusCode, Err: fmt.Errorf("resend rejected the message: %s", bytes.TrimSpace(msg))}
	default: // 401/403 are Relay's own configuration, 429 and 5xx are temporary: all worth retrying
		return resp.StatusCode, fmt.Errorf("resend answered %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
}

// SMTP sends through an SMTP server given as smtp://user:pass@host:587 (STARTTLS when offered) or
// smtps://user:pass@host:465 (implicit TLS).
type SMTP struct {
	URL     *url.URL
	Timeout time.Duration
}

// NewSMTP parses an SMTP URL.
func NewSMTP(raw string) (*SMTP, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "smtp" && u.Scheme != "smtps") || u.Hostname() == "" {
		return nil, errors.New("SMTP_URL must look like smtp://user:pass@host:587 or smtps://user:pass@host:465")
	}
	return &SMTP{URL: u, Timeout: 20 * time.Second}, nil
}

// Name implements Mailer.
func (*SMTP) Name() string { return "smtp" }

// Send implements Mailer.
func (s *SMTP) Send(ctx context.Context, m Mail) (int, error) {
	host := s.URL.Hostname()
	port := s.URL.Port()
	if port == "" {
		port = map[string]string{"smtp": "587", "smtps": "465"}[s.URL.Scheme]
	}
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return 0, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if s.URL.Scheme == "smtps" {
		conn = tls.Client(conn, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
	}
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		conn.Close()
		return 0, err
	}
	defer c.Close()
	if ok, _ := c.Extension("STARTTLS"); ok && s.URL.Scheme == "smtp" {
		if err := c.StartTLS(&tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}); err != nil {
			return 0, err
		}
	}
	if user := s.URL.User; user != nil {
		pass, _ := user.Password()
		if err := c.Auth(smtp.PlainAuth("", user.Username(), pass, host)); err != nil {
			return smtpResult(err)
		}
	}
	from, err := mail.ParseAddress(m.From)
	if err != nil {
		return 0, &PermanentError{Err: fmt.Errorf("invalid sender: %w", err)}
	}
	if err := c.Mail(from.Address); err != nil {
		return smtpResult(err)
	}
	if err := c.Rcpt(m.To); err != nil {
		return smtpResult(err)
	}
	w, err := c.Data()
	if err != nil {
		return smtpResult(err)
	}
	if _, err := w.Write(buildMIME(m, from)); err != nil {
		return 0, err
	}
	if err := w.Close(); err != nil {
		return smtpResult(err)
	}
	_ = c.Quit()
	return 250, nil
}

// smtpResult maps an SMTP reply: 5xx is permanent, 4xx is temporary.
func smtpResult(err error) (int, error) {
	var te *textproto.Error
	if errors.As(err, &te) {
		if te.Code >= 500 {
			return te.Code, &PermanentError{Code: te.Code, Err: err}
		}
		return te.Code, err
	}
	return 0, err
}

func buildMIME(m Mail, from *mail.Address) []byte {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	hdr := func(k, v string) { fmt.Fprintf(&buf, "%s: %s\r\n", k, v) }
	hdr("From", from.String())
	hdr("To", m.To)
	hdr("Subject", mime.QEncoding.Encode("utf-8", m.Subject))
	hdr("Date", time.Now().UTC().Format(time.RFC1123Z))
	hdr("Message-ID", "<"+randomID()+"@"+domainOf(from.Address)+">")
	hdr("MIME-Version", "1.0")
	hdr("Content-Type", `multipart/alternative; boundary="`+mw.Boundary()+`"`)
	buf.WriteString("\r\n")
	for _, part := range []struct{ ctype, body string }{{"text/plain", m.Text}, {"text/html", m.HTML}} {
		pw, _ := mw.CreatePart(textproto.MIMEHeader{
			"Content-Type":              {part.ctype + "; charset=utf-8"},
			"Content-Transfer-Encoding": {"quoted-printable"},
		})
		qp := quotedprintable.NewWriter(pw)
		_, _ = qp.Write([]byte(part.body))
		_ = qp.Close()
	}
	_ = mw.Close()
	return buf.Bytes()
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func domainOf(addr string) string {
	if i := strings.LastIndexByte(addr, '@'); i >= 0 {
		return addr[i+1:]
	}
	return "relay"
}
