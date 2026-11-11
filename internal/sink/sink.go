// Package sink is a webhook receiver for load tests and release smoke tests (`relay sink`): it verifies each
// signature, counts unique event IDs and measures the lag from an event's creation to its arrival.
package sink

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/guzmanloeracode/relay/pkg/webhook"
)

// Sink is the receiver's state.
type Sink struct {
	mu       sync.Mutex
	secret   string
	failRate float64
	seen     map[string]int
	lags     []time.Duration
	received int
	badSig   int
	first    time.Time
	last     time.Time
}

// New returns an empty sink.
func New() *Sink { return &Sink{seen: map[string]int{}} }

// Stats is the body of GET /stats.
type Stats struct {
	Received      int     `json:"received"`
	Unique        int     `json:"unique"`
	Duplicates    int     `json:"duplicates"`
	BadSignatures int     `json:"bad_signatures"`
	LagP50Ms      float64 `json:"lag_p50_ms"`
	LagP95Ms      float64 `json:"lag_p95_ms"`
	LagP99Ms      float64 `json:"lag_p99_ms"`
	LagMaxMs      float64 `json:"lag_max_ms"`
	FirstAt       string  `json:"first_at,omitempty"`
	LastAt        string  `json:"last_at,omitempty"`
	PerSecond     float64 `json:"per_second"`
}

// Handler serves POST /hook (webhooks), PUT /config ({"secret", "fail_rate"}), GET /stats and POST /reset.
func (s *Sink) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hook", s.hook)
	mux.HandleFunc("PUT /config", func(w http.ResponseWriter, r *http.Request) {
		var c struct {
			Secret   string  `json:"secret"`
			FailRate float64 `json:"fail_rate"`
		}
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.secret, s.failRate = c.Secret, c.FailRate
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Stats())
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		s.seen, s.lags, s.received, s.badSig = map[string]int{}, nil, 0, 0
		s.first, s.last = time.Time{}, time.Time{}
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (s *Sink) hook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read", http.StatusBadRequest)
		return
	}
	now := time.Now()
	s.mu.Lock()
	secret, failRate := s.secret, s.failRate
	s.mu.Unlock()
	if failRate > 0 && rand.Float64() < failRate { //nolint:gosec // load-test chaos, not security
		http.Error(w, "planned failure", http.StatusServiceUnavailable)
		return
	}
	ok := secret == "" || webhook.Verify(secret, r.Header, body, webhook.DefaultTolerance) == nil
	var env struct {
		CreatedAt time.Time `json:"created_at"`
	}
	_ = json.Unmarshal(body, &env)

	s.mu.Lock()
	s.received++
	if !ok {
		s.badSig++
	}
	id := r.Header.Get(webhook.HeaderID)
	s.seen[id]++
	if s.seen[id] == 1 && !env.CreatedAt.IsZero() {
		s.lags = append(s.lags, now.Sub(env.CreatedAt))
	}
	if s.first.IsZero() {
		s.first = now
	}
	s.last = now
	s.mu.Unlock()
	if !ok {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Stats summarizes what arrived so far.
func (s *Sink) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{Received: s.received, Unique: len(s.seen), BadSignatures: s.badSig}
	st.Duplicates = st.Received - st.Unique
	if len(s.lags) > 0 {
		l := slices.Clone(s.lags)
		slices.Sort(l)
		q := func(p float64) float64 { return float64(l[int(p*float64(len(l)-1))].Microseconds()) / 1000 }
		st.LagP50Ms, st.LagP95Ms, st.LagP99Ms, st.LagMaxMs = q(.5), q(.95), q(.99), q(1)
	}
	if !s.first.IsZero() {
		st.FirstAt, st.LastAt = s.first.Format(time.RFC3339Nano), s.last.Format(time.RFC3339Nano)
		if d := s.last.Sub(s.first).Seconds(); d > 0 {
			st.PerSecond = float64(st.Unique) / d
		}
	}
	return st
}
