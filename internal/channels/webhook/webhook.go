// Package webhook delivers events as signed HTTP POST requests.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/pkg/webhook"
)

const (
	maxResponseKept = 1024
	maxURLLength    = 2048
)

// Sender posts webhooks with a fixed timeout and refuses private network addresses unless allowed.
type Sender struct {
	client    *http.Client
	userAgent string
}

// New returns a Sender. timeout covers the whole request, from dial to the end of the response headers and body.
func New(timeout time.Duration, allowPrivate bool, userAgent string) *Sender {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	if !allowPrivate {
		dialer.Control = refusePrivate
	}
	tr := &http.Transport{
		Proxy:                 nil, // never route customer webhooks through an environment proxy
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: time.Second,
	}
	return &Sender{
		client: &http.Client{
			Transport: tr,
			Timeout:   timeout,
			// A redirect is an answer, not a success: following it could send the payload somewhere unexpected.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		userAgent: userAgent,
	}
}

// Envelope is the JSON body of every webhook.
type Envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Tenant    string          `json:"tenant"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// Send posts one webhook.
func (s *Sender) Send(ctx context.Context, j *channels.Job) channels.Result {
	eventID := ids.Encode(ids.Event, j.EventID)
	body, err := json.Marshal(Envelope{
		ID: eventID, Type: j.EventType, Tenant: j.TenantSlug, CreatedAt: j.EventCreatedAt.UTC(), Data: j.Payload,
	})
	if err != nil {
		return channels.Result{Outcome: channels.Dead, Error: err.Error(), Reason: "invalid_payload"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.URL, bytes.NewReader(body))
	if err != nil {
		return channels.Result{Outcome: channels.Retry, Error: err.Error(), EndpointFailure: true}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set("Relay-Delivery-Id", ids.Encode(ids.Delivery, j.DeliveryID))
	req.Header.Set("Relay-Attempt", strconv.Itoa(j.Attempt))
	if err := webhook.SetHeaders(req.Header, j.Secrets, eventID, time.Now(), body); err != nil {
		return channels.Result{Outcome: channels.Dead, Error: err.Error(), Reason: "invalid_secret"}
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return channels.Result{Outcome: channels.Retry, Error: describe(err), EndpointFailure: true}
	}
	defer resp.Body.Close()
	kept, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseKept))
	_, _ = io.CopyN(io.Discard, resp.Body, 64<<10) // let the connection be reused
	res := channels.Result{StatusCode: resp.StatusCode, ResponseBody: strings.ToValidUTF8(string(kept), "")}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		res.Outcome = channels.Delivered
	case resp.StatusCode == http.StatusGone:
		res.Outcome = channels.Dead
		res.Reason = "endpoint_gone"
		res.Error = "the endpoint answered 410 Gone"
		res.DisableEndpoint = "the endpoint answered 410 Gone"
	default:
		res.Outcome = channels.Retry
		res.Error = fmt.Sprintf("the endpoint answered %d", resp.StatusCode)
		res.EndpointFailure = true
		res.RetryAfter = retryAfter(resp.Header.Get("Retry-After"))
	}
	return res
}

func retryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// describe turns transport errors into short, stable messages for the delivery log.
func describe(err error) string {
	var nerr net.Error
	switch {
	case errors.Is(err, errPrivateAddress):
		return "the URL resolves to a private or reserved address, which Relay does not call"
	case errors.As(err, &nerr) && nerr.Timeout():
		return "timed out"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "DNS lookup failed: " + dnsErr.Err
	}
	msg := err.Error()
	if len(msg) > 300 {
		msg = msg[:300]
	}
	return msg
}

var errPrivateAddress = errors.New("private address")

// refusePrivate runs after DNS resolution, on the address actually dialed, so DNS rebinding can't get around it.
func refusePrivate(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return err
	}
	if !IsPublic(ip) {
		return errPrivateAddress
	}
	return nil
}

var reserved = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"), // NAT64 can reach private IPv4
	netip.MustParsePrefix("2001:db8::/32"),
}

// IsPublic reports whether ip is a globally routable unicast address.
func IsPublic(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
		return false
	}
	for _, p := range reserved {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// ValidateURL checks a webhook URL when an endpoint is saved. Addresses are checked again at every dial.
func ValidateURL(raw string, allowPrivate bool) error {
	if len(raw) > maxURLLength {
		return fmt.Errorf("must be at most %d characters", maxURLLength)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("must be an absolute http or https URL")
	}
	if u.User != nil {
		return errors.New("must not contain credentials; use the signature to authenticate Relay")
	}
	if u.Fragment != "" {
		return errors.New("must not contain a fragment")
	}
	if allowPrivate {
		return nil
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".internal") ||
		strings.HasSuffix(host, ".local") {
		return errors.New("must be reachable on the public internet")
	}
	if ip, err := netip.ParseAddr(host); err == nil && !IsPublic(ip) {
		return errors.New("must be reachable on the public internet")
	}
	return nil
}
