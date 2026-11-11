package api

import (
	"context"
	_ "embed"
	"html/template"
	"net/http"
	"time"

	apispec "github.com/guzmanloeracode/relay/api"
)

var (
	//go:embed web/landing.html
	landingSrc string
	//go:embed web/docs.html
	docsHTML     []byte
	landingTmpl  = template.Must(template.New("landing").Parse(landingSrc))
	pageSecurity = "default-src 'none'; style-src 'unsafe-inline'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
	docsSecurity = "default-src 'none'; script-src https://cdn.jsdelivr.net 'unsafe-inline'; style-src https://cdn.jsdelivr.net https://fonts.googleapis.com 'unsafe-inline'; " +
		"font-src https://fonts.gstatic.com https://cdn.jsdelivr.net data:; img-src 'self' data: https:; connect-src 'self'; worker-src blob:; base-uri 'none'; frame-ancestors 'none'"
)

func (s *Server) landing(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		notFound(w, "Page")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", pageSecurity)
	w.Header().Set("Cache-Control", "public, max-age=300")
	_ = landingTmpl.Execute(w, map[string]string{"Version": s.cfg.Version, "Commit": s.cfg.Commit, "URL": s.cfg.PublicURL})
}

func (s *Server) docs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", docsSecurity)
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write(docsHTML)
}

func (s *Server) openapi(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write(apispec.Spec)
}

// Health is the body of /healthz and /readyz.
type Health struct {
	Status  string            `json:"status"`
	Version string            `json:"version"`
	Commit  string            `json:"commit"`
	Checks  map[string]string `json:"checks,omitempty"`
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, Health{Status: "ok", Version: s.cfg.Version, Commit: s.cfg.Commit})
}

// readyz checks PostgreSQL and Redis.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	h := Health{Status: "ok", Version: s.cfg.Version, Commit: s.cfg.Commit, Checks: map[string]string{"postgres": "ok", "redis": "ok"}}
	code := http.StatusOK
	if err := s.store.Pool.Ping(ctx); err != nil {
		h.Checks["postgres"], h.Status, code = "unreachable", "degraded", http.StatusServiceUnavailable
	}
	if err := s.q.Client().Ping(ctx).Err(); err != nil {
		h.Checks["redis"], h.Status, code = "unreachable", "degraded", http.StatusServiceUnavailable
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, code, h)
}
