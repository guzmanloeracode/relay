// Package store opens PostgreSQL, runs the migrations and exposes the sqlc queries.
package store

import (
	"cmp"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/guzmanloeracode/relay/internal/store/db"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Store is the connection pool plus the generated queries.
type Store struct {
	*db.Queries
	Pool *pgxpool.Pool
}

// Options size the pool and choose the commit mode.
type Options struct {
	MaxConns int32 // 0 keeps pgx's default
	// AsyncCommit turns synchronous_commit off for this pool. The worker uses it: a database crash can lose
	// the last few hundred milliseconds of its writes, which only means those deliveries are routed or
	// attempted again (at least once). The API keeps synchronous commits, so a 202 means the event is on disk.
	AsyncCommit bool
	Name        string // application_name, shown in pg_stat_activity
}

// Open connects to PostgreSQL.
func Open(ctx context.Context, url string, o Options) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if o.MaxConns > 0 {
		cfg.MaxConns = o.MaxConns
	}
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.ConnConfig.RuntimeParams["application_name"] = cmp.Or(o.Name, "relay")
	if o.AsyncCommit {
		cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{Queries: db.New(pool), Pool: pool}, nil
}

// Close closes the pool.
func (s *Store) Close() { s.Pool.Close() }

// Tx runs fn in a transaction with queries bound to it.
func (s *Store) Tx(ctx context.Context, fn func(q *db.Queries) error) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error { return fn(s.WithTx(tx)) })
}

// Migrate applies every pending migration. A PostgreSQL advisory lock (taken by goose) keeps two processes
// from migrating at once.
func Migrate(ctx context.Context, url string) error {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return err
	}
	sqlDB := stdlib.OpenDB(*cfg)
	defer sqlDB.Close()
	return migrate(ctx, sqlDB)
}

func migrate(ctx context.Context, sqlDB *sql.DB) error {
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, mustSub(), goose.WithSessionLocker(mustLocker()))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

// MigrationStatus lists the migrations and whether each one is applied.
func MigrationStatus(ctx context.Context, url string) ([]*goose.MigrationStatus, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	sqlDB := stdlib.OpenDB(*cfg)
	defer sqlDB.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, mustSub())
	if err != nil {
		return nil, err
	}
	return provider.Status(ctx)
}

// IsNotFound reports whether err means the query matched no row.
func IsNotFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// IsUniqueViolation reports a unique constraint failure, and which constraint.
func IsUniqueViolation(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return pgErr.ConstraintName, true
	}
	return "", false
}
