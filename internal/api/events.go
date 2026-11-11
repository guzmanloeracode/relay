package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/router"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

const maxPayload = 256 << 10

var (
	eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$`)
	idemKeyPattern   = regexp.MustCompile(`^[\x21-\x7e]{1,255}$`)
)

// EventIn is the body of POST /v1/events.
type EventIn struct {
	Type    string            `json:"type"`
	Tenant  string            `json:"tenant"`
	Payload json.RawMessage   `json:"payload"`
	Message *channels.Message `json:"message"`
	Targets *router.Targets   `json:"targets"`
}

// Accepted is the 202 answer to POST /v1/events.
type Accepted struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

func validateEvent(in *EventIn) []FieldError {
	var errs []FieldError
	add := func(path, msg string) { errs = append(errs, FieldError{Path: path, Message: msg}) }
	switch {
	case in.Type == "":
		add("type", "is required")
	case len(in.Type) > 128 || !eventTypePattern.MatchString(in.Type):
		add("type", "must be lower-case words separated by dots, such as job.completed (at most 128 characters)")
	case len(in.Type) > 6 && in.Type[:6] == "relay.":
		add("type", "the relay. prefix is reserved")
	}
	p := bytes.TrimSpace(in.Payload)
	switch {
	case len(p) == 0 || string(p) == "null":
		add("payload", "is required")
	case p[0] != '{':
		add("payload", "must be a JSON object")
	case len(p) > maxPayload:
		add("payload", fmt.Sprintf("must be at most %d bytes", maxPayload))
	}
	if m := in.Message; m != nil {
		if n := utf8.RuneCountInString(m.Title); n < 1 || n > 200 {
			add("message.title", "must be 1 to 200 characters")
		}
		if utf8.RuneCountInString(m.Body) > 4000 {
			add("message.body", "must be at most 4000 characters")
		}
		if m.URL != "" && !validLink(m.URL) {
			add("message.url", "must be an absolute URL: https://… or an app link such as myapp://jobs/42")
		}
		if len(m.HTML) > 100<<10 {
			add("message.html", "must be at most 100 KB")
		}
	}
	if t := in.Targets; t != nil {
		if len(t.Users) > router.MaxUsers {
			add("targets.users", fmt.Sprintf("must have at most %d users", router.MaxUsers))
		}
		for i, u := range t.Users {
			if u == "" || len(u) > 128 {
				add(fmt.Sprintf("targets.users[%d]", i), "must be 1 to 128 characters")
			}
		}
		if len(t.Emails) > router.MaxEmails {
			add("targets.emails", fmt.Sprintf("must have at most %d addresses", router.MaxEmails))
		}
		for i, e := range t.Emails {
			if !validEmail(e) {
				add(fmt.Sprintf("targets.emails[%d]", i), "must be an email address")
			}
		}
		if len(t.Endpoints) > router.MaxEndpoints {
			add("targets.endpoints", fmt.Sprintf("must have at most %d endpoints", router.MaxEndpoints))
		}
		for i, e := range t.Endpoints {
			if _, err := ids.Decode(ids.Endpoint, e); err != nil {
				add(fmt.Sprintf("targets.endpoints[%d]", i), "must be an endpoint ID (ep_…)")
			}
		}
	}
	return errs
}

// validLink accepts web URLs and app deep links, never script or data URLs.
func validLink(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || !linkScheme.MatchString(u.Scheme) || blockedSchemes[u.Scheme] {
		return false
	}
	if u.Scheme == "http" || u.Scheme == "https" {
		return u.Host != ""
	}
	return u.Host != "" || u.Opaque != "" || u.Path != ""
}

var (
	linkScheme     = regexp.MustCompile(`^[a-z][a-z0-9+.-]{1,31}$`)
	blockedSchemes = map[string]bool{"javascript": true, "data": true, "vbscript": true, "file": true, "blob": true}
)

func validEmail(s string) bool {
	a, err := mail.ParseAddress(s)
	return err == nil && a.Address == s && len(s) <= 254
}

// ingest is POST /v1/events.
func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	result := "error"
	defer func() {
		s.m.EventsIngested.WithLabelValues(result).Inc()
		if result == "accepted" || result == "replayed" {
			s.m.IngestDuration.Observe(time.Since(start).Seconds())
		}
	}()
	ctx := r.Context()
	p := principalFrom(ctx)

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		result = "invalid"
		problem(w, http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("The body must be at most %d bytes.", maxBody))
		return
	}
	var in EventIn
	if !decodeBytes(w, body, &in) {
		result = "invalid"
		return
	}
	errs := validateEvent(&in)
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey != "" && !idemKeyPattern.MatchString(idemKey) {
		errs = append(errs, FieldError{Path: "Idempotency-Key", Message: "must be 1 to 255 visible ASCII characters"})
	}

	// The tenant: a tenant-bound key's own tenant, or the one named in the body.
	var tenant db.Tenant
	switch {
	case p.TenantID != nil:
		if in.Tenant != "" && in.Tenant != p.TenantSlug {
			errs = append(errs, FieldError{Path: "tenant", Message: "must be this key's tenant (or left out)"})
		} else {
			tenant, err = s.tenantBySlug(ctx, p.TenantSlug)
		}
	case in.Tenant == "":
		errs = append(errs, FieldError{Path: "tenant", Message: "is required for a key that is not bound to a tenant"})
	default:
		tenant, err = s.tenantBySlug(ctx, in.Tenant)
		if store.IsNotFound(err) {
			errs = append(errs, FieldError{Path: "tenant", Message: "is not a known tenant"})
			err = nil
		}
	}
	if err != nil {
		s.log.Error("loading tenant", "err", err)
		internal(w)
		return
	}
	if len(errs) > 0 {
		result = "invalid"
		invalid(w, errs)
		return
	}
	if tenant.DisabledAt != nil {
		result = "invalid"
		problem(w, http.StatusForbidden, "tenant_disabled", "This tenant is disabled.")
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("relay.tenant", tenant.Slug), attribute.String("relay.event_type", in.Type))

	ok, wait, err := s.q.Allow(ctx, "ingest", tenant.ID, queue.Bucket{Rate: int(tenant.IngestRate), Burst: int(tenant.IngestBurst)})
	if err != nil {
		s.log.Warn("rate limiter unavailable; accepting the event", "err", err) // the event is durable in PostgreSQL
	} else if !ok {
		result = "rate_limited"
		s.m.RateLimited.WithLabelValues("ingest").Inc()
		tooMany(w, wait, fmt.Sprintf("This tenant may send %d events a second (bursts of %d).", tenant.IngestRate, tenant.IngestBurst))
		return
	}

	hash := sha256.Sum256(body)
	params := db.InsertEventParams{
		ID: ids.New(), TenantID: tenant.ID, Type: in.Type, Payload: bytes.TrimSpace(in.Payload), RequestHash: hash[:],
	}
	if in.Message != nil {
		params.Message, _ = json.Marshal(in.Message)
	}
	if in.Targets != nil {
		params.Targets, _ = json.Marshal(in.Targets)
	}
	if idemKey != "" {
		params.IdempotencyKey = &idemKey
	}
	row, err := s.store.InsertEvent(ctx, params)
	if store.IsNotFound(err) { // the Idempotency-Key was used before
		prev, err := s.store.GetEventByIdempotencyKey(ctx, db.GetEventByIdempotencyKeyParams{TenantID: tenant.ID, IdempotencyKey: &idemKey})
		if err != nil {
			s.log.Error("loading idempotent event", "err", err)
			internal(w)
			return
		}
		if !bytes.Equal(prev.RequestHash, hash[:]) {
			result = "invalid"
			problem(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
				"This Idempotency-Key was used with a different body in the last 24 hours.")
			return
		}
		result = "replayed"
		w.Header().Set("Idempotent-Replayed", "true")
		s.accepted(w, tenant.Slug, in.Type, prev.ID, prev.CreatedAt)
		return
	}
	if err != nil {
		s.log.Error("storing event", "err", err)
		internal(w)
		return
	}
	if err := s.q.EnqueueEvents(ctx, queue.EventRef{Event: row.ID, Tenant: tenant.ID}); err != nil {
		// Stored but not queued: the worker's sweeper routes it within a minute.
		s.log.Warn("queueing event failed; the sweeper will route it", "err", err)
	}
	result = "accepted"
	s.accepted(w, tenant.Slug, in.Type, row.ID, row.CreatedAt)
}

func (s *Server) accepted(w http.ResponseWriter, tenant, typ string, id [16]byte, created time.Time) {
	eid := ids.Encode(ids.Event, id)
	w.Header().Set("Location", "/v1/tenants/"+tenant+"/events/"+eid)
	writeJSON(w, http.StatusAccepted, Accepted{ID: eid, Tenant: tenant, Type: typ, Status: "accepted", CreatedAt: created})
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	pg, ok := parsePage(w, r, ids.Event)
	if !ok {
		return
	}
	t := tenantFrom(r.Context())
	params := db.ListEventsParams{TenantID: t.ID, Before: pg.before, Lim: pg.limit}
	if typ := r.URL.Query().Get("type"); typ != "" {
		params.Type = &typ
	}
	rows, err := s.store.ListEvents(r.Context(), params)
	if err != nil {
		s.log.Error("listing events", "err", err)
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, listOf(rows, pg.limit,
		func(e db.Event) EventView { return eventView(e, t.Slug) },
		func(e db.Event) string { return ids.Encode(ids.Event, e.ID) }))
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, chi.URLParam(r, "id"), ids.Event, "Event")
	if !ok {
		return
	}
	t := tenantFrom(r.Context())
	ev, err := s.store.GetEvent(r.Context(), db.GetEventParams{TenantID: t.ID, ID: id})
	if store.IsNotFound(err) {
		notFound(w, "Event")
		return
	}
	if err != nil {
		internal(w)
		return
	}
	dels, err := s.store.ListEventDeliveries(r.Context(), db.ListEventDeliveriesParams{TenantID: t.ID, EventID: id})
	if err != nil {
		internal(w)
		return
	}
	v := eventView(ev, t.Slug)
	v.Deliveries = make([]DeliveryView, len(dels))
	for i, d := range dels {
		v.Deliveries[i] = deliveryView(d)
	}
	writeJSON(w, http.StatusOK, v)
}
