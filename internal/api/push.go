package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/guzmanloeracode/relay/internal/channels/push"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// PushTokenIn is the body of PUT /push-tokens: register (or move) a device.
type PushTokenIn struct {
	User     string `json:"user"`
	Token    string `json:"token"`
	Platform string `json:"platform"`
}

var platforms = map[string]bool{"": true, "ios": true, "android": true, "web": true}

func (s *Server) putPushToken(w http.ResponseWriter, r *http.Request) {
	var in PushTokenIn
	if !decode(w, r, &in) {
		return
	}
	var errs []FieldError
	if in.User == "" || len(in.User) > 128 {
		errs = append(errs, FieldError{Path: "user", Message: "must be 1 to 128 characters"})
	}
	if !push.TokenPattern.MatchString(in.Token) {
		errs = append(errs, FieldError{Path: "token", Message: "must be an Expo push token (ExponentPushToken[…])"})
	}
	if !platforms[in.Platform] {
		errs = append(errs, FieldError{Path: "platform", Message: "must be ios, android or web"})
	}
	if len(errs) > 0 {
		invalid(w, errs)
		return
	}
	t := tenantFrom(r.Context())
	tok, err := s.store.UpsertPushToken(r.Context(), db.UpsertPushTokenParams{
		ID: ids.New(), TenantID: t.ID, UserID: in.User, Token: in.Token, Platform: in.Platform,
	})
	if err != nil {
		s.log.Error("saving push token", "err", err)
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, pushTokenView(tok))
}

func (s *Server) listPushTokens(w http.ResponseWriter, r *http.Request) {
	pg, ok := parsePage(w, r, ids.PushToken)
	if !ok {
		return
	}
	params := db.ListPushTokensParams{TenantID: tenantFrom(r.Context()).ID, Before: pg.before, Lim: pg.limit}
	if u := r.URL.Query().Get("user"); u != "" {
		params.UserID = &u
	}
	rows, err := s.store.ListPushTokens(r.Context(), params)
	if err != nil {
		internal(w)
		return
	}
	writeJSON(w, http.StatusOK, listOf(rows, pg.limit, pushTokenView, func(p db.PushToken) string { return ids.Encode(ids.PushToken, p.ID) }))
}

func (s *Server) deletePushToken(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, chi.URLParam(r, "id"), ids.PushToken, "Push token")
	if !ok {
		return
	}
	n, err := s.store.DeletePushToken(r.Context(), db.DeletePushTokenParams{TenantID: tenantFrom(r.Context()).ID, ID: id})
	if err != nil {
		internal(w)
		return
	}
	if n == 0 {
		notFound(w, "Push token")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
