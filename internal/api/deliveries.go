package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

var deliveryStatuses = map[string]bool{"pending": true, "delivered": true, "skipped": true, "dead": true}

func (s *Server) listDeliveries(w http.ResponseWriter, r *http.Request) {
	pg, ok := parsePage(w, r, ids.Delivery)
	if !ok {
		return
	}
	t := tenantFrom(r.Context())
	q := r.URL.Query()
	params := db.ListDeliveriesParams{TenantID: t.ID, Before: pg.before, Lim: pg.limit}
	var errs []FieldError
	if v := q.Get("event_id"); v != "" {
		if id, err := ids.Decode(ids.Event, v); err == nil {
			params.EventID = &id
		} else {
			errs = append(errs, FieldError{Path: "event_id", Message: "must be an event ID (evt_…)"})
		}
	}
	if v := q.Get("endpoint_id"); v != "" {
		if id, err := ids.Decode(ids.Endpoint, v); err == nil {
			params.EndpointID = &id
		} else {
			errs = append(errs, FieldError{Path: "endpoint_id", Message: "must be an endpoint ID (ep_…)"})
		}
	}
	if v := q.Get("status"); v != "" {
		if deliveryStatuses[v] {
			params.Status = &v
		} else {
			errs = append(errs, FieldError{Path: "status", Message: "must be pending, delivered, skipped or dead"})
		}
	}
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	rows, err := s.store.ListDeliveries(r.Context(), params)
	if err != nil {
		s.log.Error("listing deliveries", "err", err)
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, listOf(rows, pg.limit, deliveryView, func(d db.Delivery) string { return ids.Encode(ids.Delivery, d.ID) }))
}

func (s *Server) getDelivery(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, chi.URLParam(r, "id"), ids.Delivery, "Delivery")
	if !ok {
		return
	}
	t := tenantFrom(r.Context())
	d, err := s.store.GetDelivery(r.Context(), db.GetDeliveryParams{TenantID: t.ID, ID: id})
	if store.IsNotFound(err) {
		notFound(w, "Delivery")
		return
	}
	if err != nil {
		internal(w)
		return
	}
	attempts, err := s.store.ListAttempts(r.Context(), id)
	if err != nil {
		internal(w)
		return
	}
	v := deliveryView(d)
	v.AttemptLog = make([]AttemptView, len(attempts))
	for i, a := range attempts {
		v.AttemptLog[i] = attemptView(a)
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	pg, ok := parsePage(w, r, ids.Delivery)
	if !ok {
		return
	}
	t := tenantFrom(r.Context())
	params := db.ListDeadLettersParams{TenantID: t.ID, Before: pg.before, Lim: pg.limit}
	if v := r.URL.Query().Get("endpoint_id"); v != "" {
		id, err := ids.Decode(ids.Endpoint, v)
		if err != nil {
			invalid(w, []FieldError{{Path: "endpoint_id", Message: "must be an endpoint ID (ep_…)"}})
			return
		}
		params.EndpointID = &id
	}
	rows, err := s.store.ListDeadLetters(r.Context(), params)
	if err != nil {
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, listOf(rows, pg.limit, deliveryView, func(d db.Delivery) string { return ids.Encode(ids.Delivery, d.ID) }))
}

// replayDelivery sends one dead delivery again, as a new delivery with a fresh attempt budget.
func (s *Server) replayDelivery(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, chi.URLParam(r, "id"), ids.Delivery, "Delivery")
	if !ok {
		return
	}
	ctx := r.Context()
	t := tenantFrom(ctx)
	d, err := s.store.GetDelivery(ctx, db.GetDeliveryParams{TenantID: t.ID, ID: id})
	if store.IsNotFound(err) {
		notFound(w, "Delivery")
		return
	}
	if err != nil {
		internal(w)
		return
	}
	switch {
	case d.Status != "dead":
		problem(w, http.StatusConflict, "not_dead", "Only deliveries in the dead-letter queue can be replayed.")
		return
	case d.ReplayedAt != nil:
		problem(w, http.StatusConflict, "already_replayed", "This delivery was already replayed.")
		return
	}
	nd, err := s.store.ReplayDelivery(ctx, db.ReplayDeliveryParams{
		NewID: ids.New(), MaxAttempts: int32(s.cfg.RetryMaxAttempts), TenantID: t.ID, ID: id,
	})
	if store.IsNotFound(err) {
		problem(w, http.StatusConflict, "endpoint_disabled", "The endpoint is disabled. Enable it first, then replay.")
		return
	}
	if err != nil {
		s.log.Error("replaying delivery", "err", err)
		internal(w)
		return
	}
	s.m.DeliveriesRouted.WithLabelValues(nd.Channel, "replay").Inc()
	if err := s.q.EnqueueDeliveries(ctx, queue.Item{Delivery: nd.ID, Tenant: nd.TenantID, Endpoint: nd.EndpointID}); err != nil {
		s.log.Warn("queueing replay failed; the sweeper will pick it up", "err", err)
	}
	writeJSON(w, http.StatusAccepted, deliveryView(nd))
}

// ReplayIn is the body of POST /dead-letters/replay.
type ReplayIn struct {
	EndpointID string `json:"endpoint_id"`
	Limit      int    `json:"limit"`
}

func (s *Server) replayDeadLetters(w http.ResponseWriter, r *http.Request) {
	var in ReplayIn
	if r.ContentLength != 0 && !decode(w, r, &in) {
		return
	}
	ctx := r.Context()
	t := tenantFrom(ctx)
	params := db.ReplayDeadLettersParams{TenantID: t.ID, MaxAttempts: int32(s.cfg.RetryMaxAttempts), Lim: 100}
	var errs []FieldError
	if in.Limit != 0 {
		if in.Limit < 1 || in.Limit > 1000 {
			errs = append(errs, FieldError{Path: "limit", Message: "must be between 1 and 1000"})
		}
		params.Lim = int32(in.Limit)
	}
	if in.EndpointID != "" {
		id, err := ids.Decode(ids.Endpoint, in.EndpointID)
		if err != nil {
			errs = append(errs, FieldError{Path: "endpoint_id", Message: "must be an endpoint ID (ep_…)"})
		}
		params.EndpointID = &id
	}
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	rows, err := s.store.ReplayDeadLetters(ctx, params)
	if err != nil {
		s.log.Error("replaying dead letters", "err", err)
		internal(w)
		return
	}
	items := make([]queue.Item, len(rows))
	for i, d := range rows {
		items[i] = queue.Item{Delivery: d.ID, Tenant: d.TenantID, Endpoint: d.EndpointID}
	}
	if err := s.q.EnqueueDeliveries(ctx, items...); err != nil {
		s.log.Warn("queueing replays failed; the sweeper will pick them up", "err", err)
	}
	writeJSON(w, http.StatusAccepted, map[string]int{"replayed": len(rows)})
}
