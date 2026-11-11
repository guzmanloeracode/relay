package api

import (
	"encoding/hex"
	"net/http"
	"regexp"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/auth"
	"github.com/guzmanloeracode/relay/internal/crypto"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// LimitsIn sets some or all of a tenant's limits.
type LimitsIn struct {
	IngestRate   *int32 `json:"ingest_rate"`
	IngestBurst  *int32 `json:"ingest_burst"`
	DeliveryRate *int32 `json:"delivery_rate"`
	MaxInFlight  *int32 `json:"max_in_flight"`
}

func (l *LimitsIn) validate() []FieldError {
	if l == nil {
		return nil
	}
	var errs []FieldError
	check := func(path string, v *int32, hi int32) {
		if v != nil && (*v < 1 || *v > hi) {
			errs = append(errs, FieldError{Path: "limits." + path, Message: "must be between 1 and " + strconv.Itoa(int(hi))})
		}
	}
	check("ingest_rate", l.IngestRate, 100_000)
	check("ingest_burst", l.IngestBurst, 100_000)
	check("delivery_rate", l.DeliveryRate, 10_000)
	check("max_in_flight", l.MaxInFlight, 1_000)
	return errs
}

// TenantIn is the body of POST /v1/tenants.
type TenantIn struct {
	Slug   string    `json:"slug"`
	Name   string    `json:"name"`
	Limits *LimitsIn `json:"limits"`
}

// TenantPatch is the body of PATCH /v1/tenants/{tenant}.
type TenantPatch struct {
	Name     *string   `json:"name"`
	Limits   *LimitsIn `json:"limits"`
	Disabled *bool     `json:"disabled"`
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	var in TenantIn
	if !decode(w, r, &in) {
		return
	}
	var errs []FieldError
	if !slugPattern.MatchString(in.Slug) {
		errs = append(errs, FieldError{Path: "slug", Message: "must be 1 to 63 lower-case letters, digits, - or _"})
	}
	if in.Name == "" || len(in.Name) > 200 {
		errs = append(errs, FieldError{Path: "name", Message: "must be 1 to 200 characters"})
	}
	errs = append(errs, in.Limits.validate()...)
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	p := db.CreateTenantParams{ID: ids.New(), Slug: in.Slug, Name: in.Name, IngestRate: 100, IngestBurst: 200, DeliveryRate: 50, MaxInFlight: 16}
	if l := in.Limits; l != nil {
		p.IngestRate = deref(l.IngestRate, p.IngestRate)
		p.IngestBurst = deref(l.IngestBurst, p.IngestBurst)
		p.DeliveryRate = deref(l.DeliveryRate, p.DeliveryRate)
		p.MaxInFlight = deref(l.MaxInFlight, p.MaxInFlight)
	}
	t, err := s.store.CreateTenant(r.Context(), p)
	if _, dup := store.IsUniqueViolation(err); dup {
		problem(w, http.StatusConflict, "slug_taken", "A tenant with this slug already exists.")
		return
	}
	if err != nil {
		s.log.Error("creating tenant", "err", err)
		internal(w)
		return
	}
	w.Header().Set("Location", "/v1/tenants/"+t.Slug)
	writeJSON(w, http.StatusCreated, tenantView(t))
}

func deref[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	pg, ok := parsePage(w, r, ids.Tenant)
	if !ok {
		return
	}
	rows, err := s.store.ListTenants(r.Context(), db.ListTenantsParams{Before: pg.before, Lim: pg.limit})
	if err != nil {
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, listOf(rows, pg.limit, tenantView, func(t db.Tenant) string { return ids.Encode(ids.Tenant, t.ID) }))
}

func (s *Server) getTenant(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, tenantView(tenantFrom(r.Context())))
}

func (s *Server) updateTenant(w http.ResponseWriter, r *http.Request) {
	var in TenantPatch
	if !decode(w, r, &in) {
		return
	}
	errs := in.Limits.validate()
	if in.Name != nil && (*in.Name == "" || len(*in.Name) > 200) {
		errs = append(errs, FieldError{Path: "name", Message: "must be 1 to 200 characters"})
	}
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	t := tenantFrom(r.Context())
	p := db.UpdateTenantParams{ID: t.ID, Name: in.Name, Disabled: in.Disabled}
	if l := in.Limits; l != nil {
		p.IngestRate, p.IngestBurst, p.DeliveryRate, p.MaxInFlight = l.IngestRate, l.IngestBurst, l.DeliveryRate, l.MaxInFlight
	}
	updated, err := s.store.UpdateTenant(r.Context(), p)
	if err != nil {
		internal(w)
		return
	}
	s.tenants.Delete(t.Slug)
	s.invalidate(r.Context(), "tenant:"+t.ID.String())
	writeJSON(w, http.StatusOK, tenantView(updated))
}

// KeyIn is the body of POST /keys.
type KeyIn struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request, tenant *db.Tenant) {
	var in KeyIn
	if !decode(w, r, &in) {
		return
	}
	var errs []FieldError
	if in.Name == "" || len(in.Name) > 100 {
		errs = append(errs, FieldError{Path: "name", Message: "must be 1 to 100 characters"})
	}
	if in.Scopes == nil {
		in.Scopes = auth.TenantScopes
	}
	if !auth.ValidScopes(in.Scopes, tenant != nil) {
		msg := "must be some of ingest, read, manage and admin"
		if tenant != nil {
			msg = "must be some of ingest, read and manage (admin keys are not bound to a tenant)"
		}
		errs = append(errs, FieldError{Path: "scopes", Message: msg})
	}
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	key, prefix := auth.NewKey()
	p := db.CreateAPIKeyParams{ID: ids.New(), Name: in.Name, Prefix: prefix, Hash: crypto.HashKey(key), Scopes: in.Scopes}
	var slug *string
	if tenant != nil {
		p.TenantID = &tenant.ID
		slug = &tenant.Slug
	}
	k, err := s.store.CreateAPIKey(r.Context(), p)
	if err != nil {
		s.log.Error("creating api key", "err", err)
		internal(w)
		return
	}
	v := keyView(k, slug)
	v.Key = key
	writeJSON(w, http.StatusCreated, v)
}

func (s *Server) createGlobalKey(w http.ResponseWriter, r *http.Request) { s.createKey(w, r, nil) }

func (s *Server) createTenantKey(w http.ResponseWriter, r *http.Request) {
	t := tenantFrom(r.Context())
	s.createKey(w, r, &t)
}

func (s *Server) listGlobalKeys(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.ListGlobalAPIKeys(r.Context())
	if err != nil {
		internal(w)
		return
	}
	out := List[KeyView]{Data: make([]KeyView, len(rows))}
	for i, k := range rows {
		out.Data[i] = keyView(k, nil)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listTenantKeys(w http.ResponseWriter, r *http.Request) {
	t := tenantFrom(r.Context())
	rows, err := s.store.ListTenantAPIKeys(r.Context(), &t.ID)
	if err != nil {
		internal(w)
		return
	}
	out := List[KeyView]{Data: make([]KeyView, len(rows))}
	for i, k := range rows {
		out.Data[i] = keyView(k, &t.Slug)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) revokeKey(w http.ResponseWriter, r *http.Request, tenant *uuid.UUID) {
	id, ok := pathID(w, chi.URLParam(r, "id"), ids.APIKey, "API key")
	if !ok {
		return
	}
	k, err := s.store.RevokeAPIKey(r.Context(), db.RevokeAPIKeyParams{ID: id, TenantID: tenant})
	if store.IsNotFound(err) {
		notFound(w, "API key")
		return
	}
	if err != nil {
		internal(w)
		return
	}
	hash := hex.EncodeToString(k.Hash)
	s.keys.Delete(hash)
	s.invalidate(r.Context(), "key:"+hash)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) revokeGlobalKey(w http.ResponseWriter, r *http.Request) { s.revokeKey(w, r, nil) }

func (s *Server) revokeTenantKey(w http.ResponseWriter, r *http.Request) {
	t := tenantFrom(r.Context())
	s.revokeKey(w, r, &t.ID)
}
