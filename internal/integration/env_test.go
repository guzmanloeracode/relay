// Package integration runs Relay end to end against real PostgreSQL 18 and Redis 8 containers
// (testcontainers-go): the API on an httptest server, workers in-process, receivers as httptest servers.
// Every API call made through env.call is also checked against api/openapi.yaml (contract tests).
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pb33f/libopenapi"
	validator "github.com/pb33f/libopenapi-validator"
	"github.com/pb33f/libopenapi-validator/config"
	verrors "github.com/pb33f/libopenapi-validator/errors"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	apispec "github.com/guzmanloeracode/relay/api"
	"github.com/guzmanloeracode/relay/internal/api"
	"github.com/guzmanloeracode/relay/internal/auth"
	relayconfig "github.com/guzmanloeracode/relay/internal/config"
	"github.com/guzmanloeracode/relay/internal/crypto"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/observability"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
	"github.com/guzmanloeracode/relay/internal/worker"
)

var (
	pgBase    string // postgres://…/ without a database name
	redisAddr string
	redisDBs  = make(chan int, 15)
	contract  validator.Validator
	dbCounter atomic.Int64
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Println("integration tests skipped (-short)")
		os.Exit(0)
	}
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithUsername("relay"), tcpostgres.WithPassword("relay"), tcpostgres.WithDatabase("relay_template"),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmdArgs("-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "max_connections=400"),
	)
	if err != nil {
		fmt.Println("starting postgres:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(pg) }()
	rd, err := tcredis.Run(ctx, "redis:8.10-alpine")
	if err != nil {
		fmt.Println("starting redis:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(rd) }()

	tmplURL, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Println(err)
		return 1
	}
	pgBase = tmplURL[:strings.LastIndex(tmplURL, "/")+1]
	if err := store.Migrate(ctx, tmplURL); err != nil {
		fmt.Println("migrating:", err)
		return 1
	}
	redisURL, err := rd.ConnectionString(ctx)
	if err != nil {
		fmt.Println(err)
		return 1
	}
	redisAddr = strings.TrimPrefix(redisURL, "redis://")
	for i := 1; i <= 15; i++ {
		redisDBs <- i
	}

	doc, err := libopenapi.NewDocument(apispec.Spec)
	if err != nil {
		fmt.Println("parsing openapi.yaml:", err)
		return 1
	}
	v, errs := validator.NewValidator(doc, config.WithoutSecurityValidation())
	if len(errs) > 0 {
		fmt.Println("building validator:", errs)
		return 1
	}
	if ok, verrs := v.ValidateDocument(); !ok {
		for _, e := range verrs {
			fmt.Println("openapi.yaml:", e.Message, e.Reason)
		}
		return 1
	}
	contract = v
	return m.Run()
}

// env is one isolated Relay: its own database (copied from the migrated template) and Redis database.
type env struct {
	t        *testing.T
	cfg      relayconfig.Config
	st       *store.Store
	rdb      *redis.Client
	q        *queue.Queue
	srv      *httptest.Server
	adminKey string
	opts     worker.Options

	mu      sync.Mutex
	workers []*runningWorker
}

type runningWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// newEnv starts an API (no worker yet: call startWorker). tweak adjusts the configuration first.
func newEnv(t *testing.T, tweak func(*relayconfig.Config)) *env {
	t.Helper()
	t.Parallel()
	ctx := context.Background()

	name := fmt.Sprintf("relay_t%d", dbCounter.Add(1))
	admin, err := pgx.Connect(ctx, pgBase+"postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	// A template can't have other connections while it is copied: retry briefly.
	for i := 0; ; i++ {
		_, err = admin.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE relay_template")
		if err == nil || i == 50 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	admin.Close(ctx)
	if err != nil {
		t.Fatal("create database:", err)
	}
	dbURL := pgBase + name + "?sslmode=disable"

	cfg := relayconfig.Config{
		Addr: ":0", PublicURL: "http://relay.test", DatabaseURL: dbURL, LogLevel: "warn",
		SecretKey:         bytes.Repeat([]byte{42}, 32),
		WorkerConcurrency: 16, WebhookTimeout: time.Second, LeaseDuration: 8 * time.Second,
		RetryMaxAttempts: 3, RetryBase: 100 * time.Millisecond, RetryMaxDelay: time.Second,
		EndpointDisableAfter: 1000, EndpointDisableMinAge: time.Hour, AllowPrivateNetworks: true,
		SecretRotationGrace: time.Hour, SweepInterval: time.Hour, SweepOverdue: 2 * time.Second,
		SweepUnrouted: 0, Retention: 30 * 24 * time.Hour, IdempotencyTTL: 24 * time.Hour, ShutdownTimeout: 10 * time.Second,
		EmailProvider: "log", EmailFrom: "Relay <relay@example.com>", PushReceiptAge: 0,
		ExpoPushURL: "http://127.0.0.1:1/send", ExpoReceiptURL: "http://127.0.0.1:1/receipts",
		Version: "test", Commit: "test",
	}
	if tweak != nil {
		tweak(&cfg)
	}

	st, err := store.Open(ctx, dbURL, store.Options{MaxConns: 40})
	if err != nil {
		t.Fatal(err)
	}
	dbn := <-redisDBs
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: dbn, PoolSize: 64})
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	q := queue.New(rdb)
	if err := q.Setup(ctx); err != nil {
		t.Fatal(err)
	}

	log := slog.New(testHandler{t: t, done: &atomic.Bool{}})
	s, err := api.New(cfg, st, q, log, observability.NewMetrics())
	if err != nil {
		t.Fatal(err)
	}
	apiCtx, stopAPI := context.WithCancel(ctx)
	go s.ListenInvalidations(apiCtx)
	srv := httptest.NewServer(s.Handler())

	e := &env{t: t, cfg: cfg, st: st, rdb: rdb, q: q, srv: srv}
	e.adminKey = e.createKey(nil, []string{auth.ScopeAdmin})

	t.Cleanup(func() {
		e.stopWorkers()
		stopAPI()
		srv.Close()
		st.Close()
		_ = rdb.FlushDB(context.Background()).Err()
		_ = rdb.Close()
		redisDBs <- dbn
		if c, err := pgx.Connect(context.Background(), pgBase+"postgres?sslmode=disable"); err == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			c.Close(context.Background())
		}
	})
	return e
}

// createKey inserts a key directly (as `relay keys create` does).
func (e *env) createKey(tenant *db.Tenant, scopes []string) string {
	key, prefix := auth.NewKey()
	p := db.CreateAPIKeyParams{ID: ids.New(), Name: "test", Prefix: prefix, Hash: crypto.HashKey(key), Scopes: scopes}
	if tenant != nil {
		p.TenantID = &tenant.ID
	}
	if _, err := e.st.CreateAPIKey(context.Background(), p); err != nil {
		e.t.Fatal(err)
	}
	return key
}

// startWorker runs one more worker process in the background.
func (e *env) startWorker() *runningWorker {
	e.t.Helper()
	w, err := worker.New(e.cfg, e.st, e.q, slog.New(testHandler{t: e.t, done: &atomic.Bool{}}), observability.NewMetrics(), e.opts)
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rw := &runningWorker{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rw.done)
		if err := w.Run(ctx); err != nil {
			e.t.Errorf("worker: %v", err)
		}
	}()
	e.mu.Lock()
	e.workers = append(e.workers, rw)
	e.mu.Unlock()
	return rw
}

// newWorker builds a worker without running it (to call Sweep, CheckReceipts…).
func (e *env) newWorker() *worker.Worker {
	w, err := worker.New(e.cfg, e.st, e.q, slog.New(testHandler{t: e.t, done: &atomic.Bool{}}), observability.NewMetrics(), e.opts)
	if err != nil {
		e.t.Fatal(err)
	}
	return w
}

func (rw *runningWorker) stop() {
	rw.cancel()
	<-rw.done
}

func (e *env) stopWorkers() {
	e.mu.Lock()
	ws := e.workers
	e.workers = nil
	e.mu.Unlock()
	for _, w := range ws {
		w.stop()
	}
}

// resp is a decoded API response.
type resp struct {
	Status int
	Header http.Header
	Body   []byte
}

func (r resp) json(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		t.Fatalf("decoding %s: %v", r.Body, err)
	}
}

func (r resp) obj(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	r.json(t, &m)
	return m
}

// problem returns the error code and the field paths of a problem response.
func (r resp) problem(t *testing.T) (string, []string) {
	t.Helper()
	var p api.Problem
	r.json(t, &p)
	var paths []string
	for _, e := range p.Errors {
		paths = append(paths, e.Path)
	}
	return p.Code, paths
}

// call makes an API request with key and checks the exchange against the OpenAPI document.
func (e *env) call(key, method, path string, body any, headers ...string) resp {
	e.t.Helper()
	var raw []byte
	switch b := body.(type) {
	case nil:
	case string:
		raw = []byte(b)
	case []byte:
		raw = b
	default:
		var err error
		if raw, err = json.Marshal(b); err != nil {
			e.t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, e.srv.URL+path, bytes.NewReader(raw))
	if err != nil {
		e.t.Fatal(err)
	}
	if raw != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	got, _ := io.ReadAll(res.Body)
	out := resp{Status: res.StatusCode, Header: res.Header, Body: got}
	e.checkContract(method, path, raw, req.Header, res, got)
	return out
}

func (e *env) checkContract(method, path string, reqBody []byte, reqHeader http.Header, res *http.Response, resBody []byte) {
	e.t.Helper()
	if !strings.HasPrefix(path, "/v1/") && path != "/healthz" && path != "/readyz" {
		return
	}
	creq, _ := http.NewRequest(method, "https://relay.tinggalpake.com"+path, bytes.NewReader(reqBody))
	creq.Header = reqHeader.Clone()
	cres := &http.Response{StatusCode: res.StatusCode, Header: res.Header.Clone(), Body: io.NopCloser(bytes.NewReader(resBody)), Request: creq}
	var ok bool
	var verrs []*verrors.ValidationError
	if res.StatusCode < 300 {
		ok, verrs = contract.ValidateHttpRequestResponse(creq, cres)
	} else {
		ok, verrs = contract.ValidateHttpResponse(creq, cres)
	}
	if !ok {
		for _, er := range verrs {
			msg := er.Message + ": " + er.Reason
			for _, f := range er.SchemaValidationErrors {
				msg += fmt.Sprintf("; %s at %s", f.Reason, f.FieldPath)
			}
			e.t.Errorf("contract: %s %s → %d: %s", method, path, res.StatusCode, msg)
		}
	}
}

// eventually waits until cond is true.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// tenant creates a tenant through the API and returns it with a tenant key holding every tenant scope.
func (e *env) tenant(slug string, limits map[string]any) (db.Tenant, string) {
	e.t.Helper()
	body := map[string]any{"slug": slug, "name": strings.ToUpper(slug[:1]) + slug[1:]}
	if limits != nil {
		body["limits"] = limits
	}
	r := e.call(e.adminKey, "POST", "/v1/tenants", body)
	if r.Status != http.StatusCreated {
		e.t.Fatalf("create tenant: %d %s", r.Status, r.Body)
	}
	t, err := e.st.GetTenantBySlug(context.Background(), slug)
	if err != nil {
		e.t.Fatal(err)
	}
	r = e.call(e.adminKey, "POST", "/v1/tenants/"+slug+"/keys", map[string]any{"name": "app"})
	if r.Status != http.StatusCreated {
		e.t.Fatalf("create key: %d %s", r.Status, r.Body)
	}
	return t, r.obj(e.t)["key"].(string)
}

// endpoint creates an endpoint and returns its ID and (for webhooks) secret.
func (e *env) endpoint(key, tenant string, body map[string]any) (string, string) {
	e.t.Helper()
	r := e.call(key, "POST", "/v1/tenants/"+tenant+"/endpoints", body)
	if r.Status != http.StatusCreated {
		e.t.Fatalf("create endpoint: %d %s", r.Status, r.Body)
	}
	m := r.obj(e.t)
	secret, _ := m["secret"].(string)
	return m["id"].(string), secret
}

// send ingests an event and returns its ID.
func (e *env) send(key string, body map[string]any, headers ...string) string {
	e.t.Helper()
	r := e.call(key, "POST", "/v1/events", body, headers...)
	if r.Status != http.StatusAccepted {
		e.t.Fatalf("ingest: %d %s", r.Status, r.Body)
	}
	return r.obj(e.t)["id"].(string)
}

// deliveries lists a tenant's deliveries (one page of up to 100).
func (e *env) deliveries(key, tenant, query string) []api.DeliveryView {
	e.t.Helper()
	r := e.call(key, "GET", "/v1/tenants/"+tenant+"/deliveries?limit=100"+query, nil)
	if r.Status != http.StatusOK {
		e.t.Fatalf("list deliveries: %d %s", r.Status, r.Body)
	}
	var l api.List[api.DeliveryView]
	r.json(e.t, &l)
	return l.Data
}

func countStatus(ds []api.DeliveryView, status string) int {
	n := 0
	for _, d := range ds {
		if d.Status == status {
			n++
		}
	}
	return n
}

// testHandler sends warnings and errors to the test log while the test runs.
type testHandler struct {
	t     *testing.T
	done  *atomic.Bool
	attrs []slog.Attr
}

func (h testHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= logLevel }
func (h testHandler) Handle(_ context.Context, r slog.Record) error {
	if h.done.Load() {
		return nil
	}
	var b strings.Builder
	b.WriteString(r.Level.String() + " " + r.Message)
	r.Attrs(func(a slog.Attr) bool { b.WriteString(" " + a.String()); return true })
	defer func() { _ = recover() }() // the test may have finished
	h.t.Log(b.String())
	return nil
}
func (h testHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return testHandler{t: h.t, done: h.done, attrs: append(h.attrs, as...)}
}
func (h testHandler) WithGroup(string) slog.Handler { return h }

var logLevel = func() slog.Level {
	if os.Getenv("RELAY_TEST_DEBUG") != "" {
		return slog.LevelDebug
	}
	return slog.LevelWarn
}()
