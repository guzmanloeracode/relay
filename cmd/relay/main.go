// Command relay runs Relay: the API, the worker, migrations and key management.
//
//	relay api                 serve the HTTP API
//	relay worker              route and deliver events
//	relay all                 both in one process (small installs, local development)
//	relay migrate [status]    apply database migrations, or list them
//	relay keys create …       create an API key (the first admin key, typically)
//	relay healthcheck [url]   exit 0 when /healthz answers (for container health checks)
//	relay sink [addr]         a webhook receiver for load and smoke tests (verifies signatures, counts events)
//	relay version             print the version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/guzmanloeracode/relay/internal/api"
	"github.com/guzmanloeracode/relay/internal/auth"
	"github.com/guzmanloeracode/relay/internal/config"
	"github.com/guzmanloeracode/relay/internal/crypto"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/observability"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/sink"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
	"github.com/guzmanloeracode/relay/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "api", "worker", "all":
		err = serve(cmd)
	case "migrate":
		err = migrate(args)
	case "keys":
		err = keys(args)
	case "healthcheck":
		err = healthcheck(args)
	case "sink":
		err = runSink(args)
	case "version":
		fmt.Printf("relay %s (%s)\n", config.Version, config.Commit)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: relay api | worker | all | migrate [status] | keys create [flags] | healthcheck [url] | sink [addr] | version")
}

func runSink(args []string) error {
	addr := ":8090"
	if len(args) > 0 {
		addr = args[0]
	}
	srv := &http.Server{Addr: addr, Handler: sink.New().Handler(), ReadHeaderTimeout: 5 * time.Second}
	fmt.Fprintln(os.Stderr, "sink listening on", addr)
	return srv.ListenAndServe()
}

func serve(mode string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := observability.NewLogger(cfg.LogLevel).With("service", "relay-"+mode, "version", cfg.Version)
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, cfg.OTLPEndpoint, "relay-"+mode, cfg.Version)
	if err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(c)
	}()

	// Separate pools: the API commits synchronously, the worker asynchronously (see store.Options).
	var apiStore, workerStore *store.Store
	if mode != "worker" {
		if apiStore, err = store.Open(ctx, cfg.DatabaseURL, store.Options{MaxConns: int32(cfg.APIDBConns), Name: "relay-api"}); err != nil {
			return err
		}
		defer apiStore.Close()
	}
	if mode != "api" {
		if workerStore, err = store.Open(ctx, cfg.DatabaseURL, store.Options{
			MaxConns: int32(cfg.WorkerDBConns), AsyncCommit: true, Name: "relay-worker",
		}); err != nil {
			return err
		}
		defer workerStore.Close()
	}
	ropts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("REDIS_URL: %w", err)
	}
	ropts.PoolSize = max(ropts.PoolSize, cfg.WorkerConcurrency+16)
	rdb := redis.NewClient(ropts)
	defer rdb.Close()
	if cfg.OTLPEndpoint != "" {
		_ = redisotel.InstrumentTracing(rdb)
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	q := queue.New(rdb)
	m := observability.NewMetrics()
	observability.ServeMetrics(ctx, cfg.MetricsAddr, m, log)

	errc := make(chan error, 2)
	running := 0
	if mode == "api" || mode == "all" {
		running++
		go func() { errc <- runAPI(ctx, cfg, apiStore, q, log, m) }()
	}
	if mode == "worker" || mode == "all" {
		w, err := worker.New(cfg, workerStore, q, log, m, worker.Options{})
		if err != nil {
			return err
		}
		running++
		go func() { errc <- w.Run(ctx) }()
	}
	var first error
	for range running {
		if err := <-errc; err != nil && first == nil {
			first = err
			stop() // one part failed: stop the other too
		}
	}
	log.Info("stopped")
	return first
}

func runAPI(ctx context.Context, cfg config.Config, st *store.Store, q *queue.Queue, log *slog.Logger, m *observability.Metrics) error {
	s, err := api.New(cfg, st, q, log, m)
	if err != nil {
		return err
	}
	go s.ListenInvalidations(ctx)
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		// Stop accepting, finish the requests in progress.
		c, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	log.Info("api listening", "addr", cfg.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func databaseURL() (string, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return "", errors.New("DATABASE_URL is required")
	}
	return url, nil
}

func migrate(args []string) error {
	url, err := databaseURL()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if len(args) > 0 && args[0] == "status" {
		list, err := store.MigrationStatus(ctx, url)
		if err != nil {
			return err
		}
		for _, m := range list {
			fmt.Printf("%-8s %s\n", m.State, m.Source.Path)
		}
		return nil
	}
	if err := store.Migrate(ctx, url); err != nil {
		return err
	}
	fmt.Println("migrations applied")
	return nil
}

func keys(args []string) error {
	if len(args) == 0 || args[0] != "create" {
		return errors.New("usage: relay keys create --name NAME [--tenant SLUG] [--scopes ingest,read,manage | --admin]")
	}
	fs := flag.NewFlagSet("keys create", flag.ContinueOnError)
	name := fs.String("name", "", "a name to recognize the key")
	tenant := fs.String("tenant", "", "bind the key to this tenant (slug)")
	scopes := fs.String("scopes", "ingest,read,manage", "comma-separated scopes")
	admin := fs.Bool("admin", false, "an admin key (every scope, every tenant)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	list := strings.Split(*scopes, ",")
	if *admin {
		list = []string{auth.ScopeAdmin}
	}
	if !auth.ValidScopes(list, *tenant != "") {
		return fmt.Errorf("invalid scopes %v for this key", list)
	}
	url, err := databaseURL()
	if err != nil {
		return err
	}
	ctx := context.Background()
	st, err := store.Open(ctx, url, store.Options{MaxConns: 2, Name: "relay-cli"})
	if err != nil {
		return err
	}
	defer st.Close()
	key, prefix := auth.NewKey()
	p := db.CreateAPIKeyParams{ID: ids.New(), Name: *name, Prefix: prefix, Hash: crypto.HashKey(key), Scopes: list}
	if *tenant != "" {
		t, err := st.GetTenantBySlug(ctx, *tenant)
		if err != nil {
			return fmt.Errorf("tenant %q: %w", *tenant, err)
		}
		p.TenantID = &t.ID
	}
	k, err := st.CreateAPIKey(ctx, p)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "created %s (%s), scopes %s. The key is shown once:\n", ids.Encode(ids.APIKey, k.ID), *name, strings.Join(list, ","))
	fmt.Println(key)
	return nil
}

func healthcheck(args []string) error {
	url := "http://127.0.0.1:8080/healthz"
	if len(args) > 0 {
		url = args[0]
	} else if addr := os.Getenv("RELAY_ADDR"); strings.HasPrefix(addr, ":") {
		url = "http://127.0.0.1" + addr + "/healthz"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // see below
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // the operator chooses the URL (a container health check)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s answered %d", url, resp.StatusCode)
	}
	return nil
}
