package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/channels"
	whchan "github.com/guzmanloeracode/relay/internal/channels/webhook"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
	"github.com/guzmanloeracode/relay/pkg/webhook"
)

// EndpointIn is the body of POST /endpoints. EventTypes, when given, also creates a subscription.
type EndpointIn struct {
	Channel     string   `json:"channel"`
	Description string   `json:"description"`
	URL         string   `json:"url"`
	EmailTo     []string `json:"email_to"`
	MaxInFlight *int32   `json:"max_in_flight"`
	EventTypes  []string `json:"event_types"`
}

// EndpointPatch is the body of PATCH /endpoints/{id}.
type EndpointPatch struct {
	Description *string   `json:"description"`
	URL         *string   `json:"url"`
	EmailTo     *[]string `json:"email_to"`
	MaxInFlight *int32    `json:"max_in_flight"`
}

func validatePatterns(path string, patterns []string) []FieldError {
	var errs []FieldError
	if len(patterns) == 0 || len(patterns) > 50 {
		return []FieldError{{Path: path, Message: "must list 1 to 50 event types"}}
	}
	for i, p := range patterns {
		if !validPattern(p) {
			errs = append(errs, FieldError{Path: fmt.Sprintf("%s[%d]", path, i),
				Message: `must be an event type (job.completed), a prefix (job.*) or "*"`})
		}
	}
	return errs
}

// validPattern accepts "*", an event type, or a prefix of one followed by ".*" ("job.*", "job.part.*").
func validPattern(p string) bool {
	if p == "*" {
		return true
	}
	if len(p) > 128 {
		return false
	}
	if prefix, ok := strings.CutSuffix(p, ".*"); ok {
		return prefix != "" && eventTypePattern.MatchString(prefix+".x")
	}
	return eventTypePattern.MatchString(p)
}

func (s *Server) validateEndpointFields(channel, url string, emailTo []string, maxInFlight *int32) []FieldError {
	var errs []FieldError
	switch channel {
	case channels.Webhook:
		if url == "" {
			errs = append(errs, FieldError{Path: "url", Message: "is required for a webhook endpoint"})
		} else if err := whchan.ValidateURL(url, s.cfg.AllowPrivateNetworks); err != nil {
			errs = append(errs, FieldError{Path: "url", Message: err.Error()})
		}
	case channels.Email, channels.Push:
		if url != "" {
			errs = append(errs, FieldError{Path: "url", Message: "only webhook endpoints have a URL"})
		}
	default:
		errs = append(errs, FieldError{Path: "channel", Message: "must be webhook, email or push"})
	}
	if len(emailTo) > 0 && channel != channels.Email {
		errs = append(errs, FieldError{Path: "email_to", Message: "only email endpoints have recipients"})
	}
	if len(emailTo) > 50 {
		errs = append(errs, FieldError{Path: "email_to", Message: "must have at most 50 addresses"})
	}
	for i, e := range emailTo {
		if !validEmail(e) {
			errs = append(errs, FieldError{Path: fmt.Sprintf("email_to[%d]", i), Message: "must be an email address"})
		}
	}
	if maxInFlight != nil && (*maxInFlight < 1 || *maxInFlight > 64) {
		errs = append(errs, FieldError{Path: "max_in_flight", Message: "must be between 1 and 64"})
	}
	return errs
}

func (s *Server) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var in EndpointIn
	if !decode(w, r, &in) {
		return
	}
	errs := s.validateEndpointFields(in.Channel, in.URL, in.EmailTo, in.MaxInFlight)
	if len(in.Description) > 500 {
		errs = append(errs, FieldError{Path: "description", Message: "must be at most 500 characters"})
	}
	if in.EventTypes != nil {
		errs = append(errs, validatePatterns("event_types", in.EventTypes)...)
	}
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	ctx := r.Context()
	t := tenantFrom(ctx)
	p := db.CreateEndpointParams{
		ID: ids.New(), TenantID: t.ID, Channel: in.Channel, Description: in.Description,
		EmailTo: in.EmailTo, MaxInFlight: 4,
	}
	if p.EmailTo == nil {
		p.EmailTo = []string{}
	}
	if in.MaxInFlight != nil {
		p.MaxInFlight = *in.MaxInFlight
	}
	var secret string
	if in.Channel == channels.Webhook {
		secret = webhook.NewSecret()
		p.Url = &in.URL
		p.SecretEnc = s.box.Seal([]byte(secret))
	}
	var ep db.Endpoint
	err := s.store.Tx(ctx, func(q *db.Queries) error {
		var err error
		if ep, err = q.CreateEndpoint(ctx, p); err != nil {
			return err
		}
		if in.EventTypes != nil {
			_, err = q.CreateSubscription(ctx, db.CreateSubscriptionParams{ID: ids.New(), TenantID: t.ID, EndpointID: ep.ID, EventTypes: in.EventTypes})
		}
		return err
	})
	if err != nil {
		s.log.Error("creating endpoint", "err", err)
		internal(w)
		return
	}
	s.invalidate(ctx, "routes:"+t.ID.String())
	v := endpointView(ep)
	v.Secret = secret
	w.Header().Set("Location", r.URL.Path+"/"+v.ID)
	writeJSON(w, http.StatusCreated, v)
}

func (s *Server) loadEndpoint(w http.ResponseWriter, r *http.Request) (db.Endpoint, bool) {
	id, ok := pathID(w, chi.URLParam(r, "id"), ids.Endpoint, "Endpoint")
	if !ok {
		return db.Endpoint{}, false
	}
	ep, err := s.store.GetEndpoint(r.Context(), db.GetEndpointParams{TenantID: tenantFrom(r.Context()).ID, ID: id})
	if store.IsNotFound(err) {
		notFound(w, "Endpoint")
		return ep, false
	}
	if err != nil {
		internal(w)
		return ep, false
	}
	return ep, true
}

func (s *Server) listEndpoints(w http.ResponseWriter, r *http.Request) {
	pg, ok := parsePage(w, r, ids.Endpoint)
	if !ok {
		return
	}
	rows, err := s.store.ListEndpoints(r.Context(), db.ListEndpointsParams{TenantID: tenantFrom(r.Context()).ID, Before: pg.before, Lim: pg.limit})
	if err != nil {
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, listOf(rows, pg.limit, endpointView, func(e db.Endpoint) string { return ids.Encode(ids.Endpoint, e.ID) }))
}

func (s *Server) getEndpoint(w http.ResponseWriter, r *http.Request) {
	if ep, ok := s.loadEndpoint(w, r); ok {
		writeJSON(w, http.StatusOK, endpointView(ep))
	}
}

func (s *Server) updateEndpoint(w http.ResponseWriter, r *http.Request) {
	ep, ok := s.loadEndpoint(w, r)
	if !ok {
		return
	}
	var in EndpointPatch
	if !decode(w, r, &in) {
		return
	}
	url := ""
	if in.URL != nil {
		url = *in.URL
		if ep.Channel == channels.Webhook && url == "" {
			invalid(w, []FieldError{{Path: "url", Message: "cannot be empty"}})
			return
		}
	} else if ep.Url != nil {
		url = *ep.Url
	}
	var emailTo []string
	if in.EmailTo != nil {
		emailTo = *in.EmailTo
	}
	errs := s.validateEndpointFields(ep.Channel, url, emailTo, in.MaxInFlight)
	if in.Description != nil && len(*in.Description) > 500 {
		errs = append(errs, FieldError{Path: "description", Message: "must be at most 500 characters"})
	}
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	p := db.UpdateEndpointParams{TenantID: ep.TenantID, ID: ep.ID, Description: in.Description, MaxInFlight: in.MaxInFlight}
	if in.URL != nil {
		p.Url = in.URL
	}
	if in.EmailTo != nil {
		p.EmailTo = emailTo
		if p.EmailTo == nil {
			p.EmailTo = []string{}
		}
	}
	updated, err := s.store.UpdateEndpoint(r.Context(), p)
	if err != nil {
		internal(w)
		return
	}
	s.endpointChanged(r.Context(), updated)
	writeJSON(w, http.StatusOK, endpointView(updated))
}

func (s *Server) endpointChanged(ctx context.Context, ep db.Endpoint) {
	s.invalidate(ctx, "routes:"+ep.TenantID.String())
	s.invalidate(ctx, "endpoint:"+ep.ID.String())
}

func (s *Server) deleteEndpoint(w http.ResponseWriter, r *http.Request) {
	ep, ok := s.loadEndpoint(w, r)
	if !ok {
		return
	}
	if _, err := s.store.DeleteEndpoint(r.Context(), db.DeleteEndpointParams{TenantID: ep.TenantID, ID: ep.ID}); err != nil {
		internal(w)
		return
	}
	s.endpointChanged(r.Context(), ep)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getEndpointSecret(w http.ResponseWriter, r *http.Request) {
	ep, ok := s.loadEndpoint(w, r)
	if !ok {
		return
	}
	if ep.Channel != channels.Webhook {
		problem(w, http.StatusConflict, "not_a_webhook", "Only webhook endpoints have a signing secret.")
		return
	}
	secret, err := s.box.Open(ep.SecretEnc)
	if err != nil {
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"secret": string(secret)})
}

// rotateEndpointSecret issues a new secret. The old one keeps signing too (a second signature) for
// SECRET_ROTATION_GRACE, so receivers can switch without dropping requests.
func (s *Server) rotateEndpointSecret(w http.ResponseWriter, r *http.Request) {
	ep, ok := s.loadEndpoint(w, r)
	if !ok {
		return
	}
	if ep.Channel != channels.Webhook {
		problem(w, http.StatusConflict, "not_a_webhook", "Only webhook endpoints have a signing secret.")
		return
	}
	secret := webhook.NewSecret()
	until := time.Now().Add(s.cfg.SecretRotationGrace)
	updated, err := s.store.RotateEndpointSecret(r.Context(), db.RotateEndpointSecretParams{
		TenantID: ep.TenantID, ID: ep.ID, SecretEnc: s.box.Seal([]byte(secret)), PreviousExpiresAt: &until,
	})
	if err != nil {
		internal(w)
		return
	}
	v := endpointView(updated)
	v.Secret = secret
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) enableEndpoint(w http.ResponseWriter, r *http.Request) {
	ep, ok := s.loadEndpoint(w, r)
	if !ok {
		return
	}
	updated, err := s.store.EnableEndpoint(r.Context(), db.EnableEndpointParams{TenantID: ep.TenantID, ID: ep.ID})
	if err != nil {
		internal(w)
		return
	}
	s.endpointChanged(r.Context(), updated)
	writeJSON(w, http.StatusOK, endpointView(updated))
}

func (s *Server) disableEndpoint(w http.ResponseWriter, r *http.Request) {
	ep, ok := s.loadEndpoint(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	reason := "disabled through the API"
	updated, err := s.store.DisableEndpoint(ctx, db.DisableEndpointParams{ID: ep.ID, Reason: &reason})
	if store.IsNotFound(err) { // already disabled
		writeJSON(w, http.StatusOK, endpointView(ep))
		return
	}
	if err != nil {
		internal(w)
		return
	}
	n, _ := s.store.DeadLetterEndpointBacklog(ctx, db.DeadLetterEndpointBacklogParams{EndpointID: ep.ID, Reason: new("endpoint_disabled")})
	s.m.DeadLettered.WithLabelValues("endpoint_disabled").Add(float64(n))
	s.endpointChanged(ctx, updated)
	writeJSON(w, http.StatusOK, endpointView(updated))
}

// SubscriptionIn is the body of POST /subscriptions and PATCH /subscriptions/{id}.
type SubscriptionIn struct {
	EndpointID string   `json:"endpoint_id"`
	EventTypes []string `json:"event_types"`
}

func (s *Server) createSubscription(w http.ResponseWriter, r *http.Request) {
	var in SubscriptionIn
	if !decode(w, r, &in) {
		return
	}
	errs := validatePatterns("event_types", in.EventTypes)
	epID, err := ids.Decode(ids.Endpoint, in.EndpointID)
	if err != nil {
		errs = append(errs, FieldError{Path: "endpoint_id", Message: "must be an endpoint ID (ep_…)"})
	}
	ctx := r.Context()
	t := tenantFrom(ctx)
	if err == nil {
		if _, err := s.store.GetEndpoint(ctx, db.GetEndpointParams{TenantID: t.ID, ID: epID}); store.IsNotFound(err) {
			errs = append(errs, FieldError{Path: "endpoint_id", Message: "is not an endpoint of this tenant"})
		} else if err != nil {
			internal(w)
			return
		}
	}
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	sub, err := s.store.CreateSubscription(ctx, db.CreateSubscriptionParams{ID: ids.New(), TenantID: t.ID, EndpointID: epID, EventTypes: in.EventTypes})
	if err != nil {
		internal(w)
		return
	}
	s.invalidate(ctx, "routes:"+t.ID.String())
	v := subscriptionView(sub)
	w.Header().Set("Location", r.URL.Path+"/"+v.ID)
	writeJSON(w, http.StatusCreated, v)
}

func (s *Server) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	pg, ok := parsePage(w, r, ids.Subscription)
	if !ok {
		return
	}
	params := db.ListSubscriptionsParams{TenantID: tenantFrom(r.Context()).ID, Before: pg.before, Lim: pg.limit}
	if v := r.URL.Query().Get("endpoint_id"); v != "" {
		id, err := ids.Decode(ids.Endpoint, v)
		if err != nil {
			invalid(w, []FieldError{{Path: "endpoint_id", Message: "must be an endpoint ID (ep_…)"}})
			return
		}
		params.EndpointID = &id
	}
	rows, err := s.store.ListSubscriptions(r.Context(), params)
	if err != nil {
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, listOf(rows, pg.limit, subscriptionView, func(s db.Subscription) string { return ids.Encode(ids.Subscription, s.ID) }))
}

func (s *Server) subscriptionID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	return pathID(w, chi.URLParam(r, "id"), ids.Subscription, "Subscription")
}

func (s *Server) getSubscription(w http.ResponseWriter, r *http.Request) {
	id, ok := s.subscriptionID(w, r)
	if !ok {
		return
	}
	sub, err := s.store.GetSubscription(r.Context(), db.GetSubscriptionParams{TenantID: tenantFrom(r.Context()).ID, ID: id})
	if store.IsNotFound(err) {
		notFound(w, "Subscription")
		return
	}
	if err != nil {
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, subscriptionView(sub))
}

func (s *Server) updateSubscription(w http.ResponseWriter, r *http.Request) {
	id, ok := s.subscriptionID(w, r)
	if !ok {
		return
	}
	var in struct {
		EventTypes []string `json:"event_types"`
	}
	if !decode(w, r, &in) {
		return
	}
	if errs := validatePatterns("event_types", in.EventTypes); len(errs) > 0 {
		invalid(w, errs)
		return
	}
	t := tenantFrom(r.Context())
	sub, err := s.store.UpdateSubscription(r.Context(), db.UpdateSubscriptionParams{TenantID: t.ID, ID: id, EventTypes: in.EventTypes})
	if store.IsNotFound(err) {
		notFound(w, "Subscription")
		return
	}
	if err != nil {
		internal(w)
		return
	}
	s.invalidate(r.Context(), "routes:"+t.ID.String())
	writeJSON(w, http.StatusOK, subscriptionView(sub))
}

func (s *Server) deleteSubscription(w http.ResponseWriter, r *http.Request) {
	id, ok := s.subscriptionID(w, r)
	if !ok {
		return
	}
	t := tenantFrom(r.Context())
	n, err := s.store.DeleteSubscription(r.Context(), db.DeleteSubscriptionParams{TenantID: t.ID, ID: id})
	if err != nil {
		internal(w)
		return
	}
	if n == 0 {
		notFound(w, "Subscription")
		return
	}
	s.invalidate(r.Context(), "routes:"+t.ID.String())
	w.WriteHeader(http.StatusNoContent)
}
