package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guzmanloeracode/relay/pkg/webhook"
)

// receiver is a webhook endpoint for tests: it verifies signatures, records requests and answers as told.
type receiver struct {
	t       *testing.T
	srv     *httptest.Server
	mu      sync.Mutex
	secrets []string
	got     []received
	byID    map[string]int
	status  atomic.Int32
	delay   atomic.Int64 // nanoseconds
	active  atomic.Int32
	peak    atomic.Int32
	badSigs atomic.Int32
}

type received struct {
	Header   http.Header
	Envelope struct {
		ID        string          `json:"id"`
		Type      string          `json:"type"`
		Tenant    string          `json:"tenant"`
		CreatedAt time.Time       `json:"created_at"`
		Data      json.RawMessage `json:"data"`
	}
	At time.Time
}

func newReceiver(t *testing.T) *receiver {
	r := &receiver{t: t, byID: map[string]int{}}
	r.status.Store(http.StatusOK)
	r.srv = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) url() string { return r.srv.URL + "/hooks" }

func (r *receiver) setSecrets(s ...string) {
	r.mu.Lock()
	r.secrets = s
	r.mu.Unlock()
}

func (r *receiver) serve(w http.ResponseWriter, req *http.Request) {
	n := r.active.Add(1)
	defer r.active.Add(-1)
	for {
		p := r.peak.Load()
		if n <= p || r.peak.CompareAndSwap(p, n) {
			break
		}
	}
	body, _ := io.ReadAll(req.Body)
	if d := time.Duration(r.delay.Load()); d > 0 {
		select {
		case <-time.After(d):
		case <-req.Context().Done():
			return
		}
	}
	r.mu.Lock()
	secrets := r.secrets
	r.mu.Unlock()
	for _, s := range secrets {
		if err := webhook.Verify(s, req.Header, body, webhook.DefaultTolerance); err != nil {
			r.badSigs.Add(1)
			r.t.Errorf("signature with secret %d: %v", len(s), err)
		}
	}
	var rec received
	rec.Header = req.Header.Clone()
	rec.At = time.Now()
	if err := json.Unmarshal(body, &rec.Envelope); err != nil {
		r.t.Errorf("webhook body is not the envelope: %s", body)
	}
	r.mu.Lock()
	r.got = append(r.got, rec)
	r.byID[rec.Envelope.ID]++
	r.mu.Unlock()
	w.WriteHeader(int(r.status.Load()))
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func (r *receiver) unique() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byID)
}

func (r *receiver) all() []received {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]received(nil), r.got...)
}

func (r *receiver) duplicates() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := 0
	for _, n := range r.byID {
		d += n - 1
	}
	return d
}
