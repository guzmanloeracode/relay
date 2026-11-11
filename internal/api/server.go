// Package api is Relay's HTTP API: event ingestion, delivery logs, dead letters and management.
package api

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/guzmanloeracode/relay/internal/auth"
	"github.com/guzmanloeracode/relay/internal/cache"
	"github.com/guzmanloeracode/relay/internal/config"
	"github.com/guzmanloeracode/relay/internal/crypto"
	"github.com/guzmanloeracode/relay/internal/observability"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// Server holds the API's dependencies.
type Server struct {
	cfg   config.Config
	store *store.Store
	q     *queue.Queue
	log   *slog.Logger
	m     *observability.Metrics
	box   *crypto.Box

	keys    *cache.Cache[string, cachedKey]
	tenants *cache.Cache[string, db.Tenant] // by slug
	touched *cache.Cache[uuid.UUID, bool]   // keys whose last_used_at was written recently
}

type cachedKey struct {
	principal      auth.Principal
	revoked        bool
	tenantDisabled bool
}

// New returns a Server.
func New(cfg config.Config, st *store.Store, q *queue.Queue, log *slog.Logger, m *observability.Metrics) (*Server, error) {
	box, err := crypto.NewBox(cfg.SecretKey)
	if err != nil {
		return nil, err
	}
	return &Server{
		cfg: cfg, store: st, q: q, log: log, m: m, box: box,
		keys:    cache.New[string, cachedKey](30*time.Second, 100_000),
		tenants: cache.New[string, db.Tenant](30*time.Second, 100_000),
		touched: cache.New[uuid.UUID, bool](5*time.Minute, 100_000),
	}, nil
}

// ListenInvalidations drops cache entries that other processes changed, until ctx ends.
func (s *Server) ListenInvalidations(ctx context.Context) {
	for msg := range s.q.Invalidations(ctx) {
		kind, v, _ := strings.Cut(msg, ":")
		switch kind {
		case "key":
			s.keys.Delete(v)
		case "tenant":
			id, err := uuid.Parse(v)
			if err != nil {
				continue
			}
			s.tenants.DeleteFunc(func(_ string, t db.Tenant) bool { return t.ID == id })
			s.keys.DeleteFunc(func(_ string, k cachedKey) bool {
				return k.principal.TenantID != nil && *k.principal.TenantID == id
			})
		}
	}
}

// Handler returns the full HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, s.requestLog, middleware.Recoverer, securityHeaders)

	r.Get("/", s.landing)
	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)
	r.Get("/openapi.yaml", s.openapi)
	r.Get("/docs", s.docs)

	r.Route("/v1", func(r chi.Router) {
		r.Use(noStore, s.authenticate, s.apiRateLimit)
		r.With(s.require(auth.ScopeIngest)).Post("/events", s.ingest)

		r.With(s.require(auth.ScopeAdmin)).Group(func(r chi.Router) {
			r.Get("/tenants", s.listTenants)
			r.Post("/tenants", s.createTenant)
			r.Get("/keys", s.listGlobalKeys)
			r.Post("/keys", s.createGlobalKey)
			r.Delete("/keys/{id}", s.revokeGlobalKey)
		})

		r.Route("/tenants/{tenant}", func(r chi.Router) {
			r.Use(s.tenantScope)
			r.With(s.require(auth.ScopeRead)).Get("/", s.getTenant)
			r.With(s.require(auth.ScopeAdmin)).Patch("/", s.updateTenant)
			r.With(s.require(auth.ScopeAdmin)).Get("/keys", s.listTenantKeys)
			r.With(s.require(auth.ScopeAdmin)).Post("/keys", s.createTenantKey)
			r.With(s.require(auth.ScopeAdmin)).Delete("/keys/{id}", s.revokeTenantKey)

			r.Group(func(r chi.Router) {
				r.Use(s.require(auth.ScopeRead))
				r.Get("/events", s.listEvents)
				r.Get("/events/{id}", s.getEvent)
				r.Get("/deliveries", s.listDeliveries)
				r.Get("/deliveries/{id}", s.getDelivery)
				r.Get("/dead-letters", s.listDeadLetters)
				r.Get("/endpoints", s.listEndpoints)
				r.Get("/endpoints/{id}", s.getEndpoint)
				r.Get("/subscriptions", s.listSubscriptions)
				r.Get("/subscriptions/{id}", s.getSubscription)
				r.Get("/push-tokens", s.listPushTokens)
			})
			r.Group(func(r chi.Router) {
				r.Use(s.require(auth.ScopeManage))
				r.Post("/deliveries/{id}/replay", s.replayDelivery)
				r.Post("/dead-letters/replay", s.replayDeadLetters)
				r.Post("/endpoints", s.createEndpoint)
				r.Patch("/endpoints/{id}", s.updateEndpoint)
				r.Delete("/endpoints/{id}", s.deleteEndpoint)
				r.Get("/endpoints/{id}/secret", s.getEndpointSecret)
				r.Post("/endpoints/{id}/rotate-secret", s.rotateEndpointSecret)
				r.Post("/endpoints/{id}/enable", s.enableEndpoint)
				r.Post("/endpoints/{id}/disable", s.disableEndpoint)
				r.Post("/subscriptions", s.createSubscription)
				r.Patch("/subscriptions/{id}", s.updateSubscription)
				r.Delete("/subscriptions/{id}", s.deleteSubscription)
				r.Put("/push-tokens", s.putPushToken)
				r.Delete("/push-tokens/{id}", s.deletePushToken)
			})
		})
	})
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) { notFound(w, "Route") })
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		problem(w, http.StatusMethodNotAllowed, "method_not_allowed", "This route does not accept that method.")
	})

	return otelhttp.NewHandler(r, "relay.api", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		return r.Method + " " + routePattern(r)
	}))
}

func routePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil {
		if p := rc.RoutePattern(); p != "" {
			return p
		}
	}
	return "unmatched"
}

// requestLog records metrics for every request and logs errors and slow requests.
func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		took := time.Since(start)
		route := routePattern(r)
		status := ww.Status()
		if status == 0 {
			status = http.StatusOK
		}
		s.m.HTTPRequests.WithLabelValues(route, r.Method, fmt.Sprintf("%dxx", status/100)).Inc()
		s.m.HTTPDuration.WithLabelValues(route, r.Method).Observe(took.Seconds())
		if status >= 500 || took > time.Second {
			s.log.Warn("request", "method", r.Method, "route", route, "status", status,
				"ms", took.Milliseconds(), "request_id", middleware.GetReqID(r.Context()))
		}
	})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}

func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

type ctxKey int

const (
	principalKey ctxKey = iota
	tenantKey
)

func principalFrom(ctx context.Context) *auth.Principal {
	p, _ := ctx.Value(principalKey).(*auth.Principal)
	return p
}

func tenantFrom(ctx context.Context) db.Tenant {
	t, _ := ctx.Value(tenantKey).(db.Tenant)
	return t
}

// authenticate resolves "Authorization: Bearer <key>" to a principal.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || raw == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="relay"`)
			problem(w, http.StatusUnauthorized, "unauthenticated", "Send an API key as Authorization: Bearer <key>.")
			return
		}
		hash := crypto.HashKey(strings.TrimSpace(raw))
		hashHex := hex.EncodeToString(hash)
		k, err := s.keys.GetOrLoad(hashHex, func() (cachedKey, error) {
			row, err := s.store.GetAPIKeyByHash(r.Context(), hash)
			if store.IsNotFound(err) {
				return cachedKey{revoked: true}, nil // cache unknown keys too, so guessing costs no queries
			}
			if err != nil {
				return cachedKey{}, err
			}
			p := auth.Principal{KeyID: row.ID, TenantID: row.TenantID, Scopes: row.Scopes}
			if row.TenantSlug != nil {
				p.TenantSlug = *row.TenantSlug
			}
			return cachedKey{principal: p, revoked: row.RevokedAt != nil, tenantDisabled: row.TenantDisabledAt != nil}, nil
		})
		if err != nil {
			s.log.Error("loading api key", "err", err)
			internal(w)
			return
		}
		if k.revoked {
			w.Header().Set("WWW-Authenticate", `Bearer realm="relay", error="invalid_token"`)
			problem(w, http.StatusUnauthorized, "unauthenticated", "This API key is not valid.")
			return
		}
		if k.tenantDisabled {
			problem(w, http.StatusForbidden, "tenant_disabled", "This key's tenant is disabled.")
			return
		}
		if _, recent := s.touched.Get(k.principal.KeyID); !recent {
			s.touched.Set(k.principal.KeyID, true)
			go func(id uuid.UUID) {
				// Detached from the request: the response doesn't wait for this bookkeeping write.
				ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
				defer cancel()
				_ = s.store.TouchAPIKey(ctx, id)
			}(k.principal.KeyID)
		}
		p := k.principal
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey, &p)))
	})
}

// apiRateLimit caps every key at 50 requests a second (burst 100), except ingestion, which has the tenant's
// own bucket.
func (s *Server) apiRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/events" {
			next.ServeHTTP(w, r)
			return
		}
		p := principalFrom(r.Context())
		ok, wait, err := s.q.Allow(r.Context(), "api", p.KeyID, queue.Bucket{Rate: 50, Burst: 100})
		if err == nil && !ok {
			s.m.RateLimited.WithLabelValues("api").Inc()
			tooMany(w, wait, "Slow down: this key is limited to 50 requests a second.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func tooMany(w http.ResponseWriter, wait time.Duration, detail string) {
	secs := int((wait + time.Second - 1) / time.Second)
	w.Header().Set("Retry-After", fmt.Sprint(max(secs, 1)))
	problem(w, http.StatusTooManyRequests, "rate_limited", detail)
}

func (s *Server) require(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !principalFrom(r.Context()).Has(scope) {
				problem(w, http.StatusForbidden, "forbidden", fmt.Sprintf("This API key lacks the %q scope.", scope))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// tenantScope loads the tenant named in the path. Keys of another tenant get a 404, as if it didn't exist.
func (s *Server) tenantScope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slug := chi.URLParam(r, "tenant")
		t, err := s.tenantBySlug(r.Context(), slug)
		if store.IsNotFound(err) || (err == nil && !principalFrom(r.Context()).CanAccess(t.ID)) {
			notFound(w, "Tenant")
			return
		}
		if err != nil {
			s.log.Error("loading tenant", "err", err)
			internal(w)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantKey, t)))
	})
}

func (s *Server) tenantBySlug(ctx context.Context, slug string) (db.Tenant, error) {
	return s.tenants.GetOrLoad(slug, func() (db.Tenant, error) { return s.store.GetTenantBySlug(ctx, slug) })
}

// invalidate drops a cache entry here and in every other process.
func (s *Server) invalidate(ctx context.Context, what string) {
	if err := s.q.Invalidate(ctx, what); err != nil {
		s.log.Warn("publishing cache invalidation", "err", err, "what", what)
	}
}
