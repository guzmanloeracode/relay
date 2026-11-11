// Package config reads Relay's settings from the environment. Every variable is listed in .env.example.
package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/mail"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every setting. Durations accept Go syntax ("10s", "24h").
type Config struct {
	Addr        string // API listen address
	MetricsAddr string // Prometheus /metrics listen address ("" disables)
	PublicURL   string // shown on the landing page and in Location headers' docs
	DatabaseURL string
	RedisURL    string
	SecretKey   []byte // 32 bytes, encrypts webhook signing secrets at rest
	LogLevel    string

	WorkerConcurrency int
	APIDBConns        int // PostgreSQL connections of an API process
	WorkerDBConns     int // PostgreSQL connections of a worker process
	WebhookTimeout    time.Duration
	LeaseDuration     time.Duration
	RetryMaxAttempts  int
	RetryBase         time.Duration // first retry delay; each later one is 3× longer, with jitter
	RetryMaxDelay     time.Duration // cap on a single delay, also on a Retry-After from the receiver
	// An endpoint is disabled once it has failed DisableAfter attempts in a row for at least DisableMinAge.
	EndpointDisableAfter  int
	EndpointDisableMinAge time.Duration
	AllowPrivateNetworks  bool // allow webhook URLs that resolve to loopback or private addresses (tests, local)
	SecretRotationGrace   time.Duration

	SweepInterval   time.Duration
	SweepOverdue    time.Duration // a due delivery this late is queued again
	SweepUnrouted   time.Duration // an event not routed after this long is queued again
	Retention       time.Duration
	IdempotencyTTL  time.Duration
	ShutdownTimeout time.Duration

	EmailProvider  string // log, resend or smtp
	EmailFrom      string
	ResendAPIKey   string
	ResendURL      string
	SMTPURL        string
	ExpoPushURL    string
	ExpoReceiptURL string
	ExpoToken      string
	PushReceiptAge time.Duration // how long to wait before asking Expo for a receipt

	OTLPEndpoint string // OTEL_EXPORTER_OTLP_ENDPOINT; tracing is off when empty
	Version      string
	Commit       string
}

// Load reads the environment. It returns every problem at once.
func Load() (Config, error) {
	e := &env{}
	c := Config{
		Addr:        e.str("RELAY_ADDR", ":8080"),
		MetricsAddr: e.str("RELAY_METRICS_ADDR", ":9091"),
		PublicURL:   strings.TrimRight(e.str("RELAY_PUBLIC_URL", "http://localhost:8080"), "/"),
		DatabaseURL: e.str("DATABASE_URL", ""),
		RedisURL:    e.str("REDIS_URL", "redis://localhost:6379/0"),
		LogLevel:    e.str("LOG_LEVEL", "info"),

		WorkerConcurrency:     e.int("WORKER_CONCURRENCY", 64),
		APIDBConns:            e.int("API_DB_CONNS", 16),
		WorkerDBConns:         e.int("WORKER_DB_CONNS", 24),
		WebhookTimeout:        e.dur("WEBHOOK_TIMEOUT", 10*time.Second),
		LeaseDuration:         e.dur("DELIVERY_LEASE", 60*time.Second),
		RetryMaxAttempts:      e.int("RETRY_MAX_ATTEMPTS", 8),
		RetryBase:             e.dur("RETRY_BASE", 75*time.Second),
		RetryMaxDelay:         e.dur("RETRY_MAX_DELAY", 16*time.Hour),
		EndpointDisableAfter:  e.int("ENDPOINT_DISABLE_AFTER", 10),
		EndpointDisableMinAge: e.dur("ENDPOINT_DISABLE_MIN_AGE", 24*time.Hour),
		AllowPrivateNetworks:  e.bool("ALLOW_PRIVATE_NETWORKS", false),
		SecretRotationGrace:   e.dur("SECRET_ROTATION_GRACE", 24*time.Hour),

		SweepInterval:   e.dur("SWEEP_INTERVAL", 30*time.Second),
		SweepOverdue:    e.dur("SWEEP_OVERDUE", 10*time.Minute),
		SweepUnrouted:   e.dur("SWEEP_UNROUTED", time.Minute),
		Retention:       e.dur("RETENTION", 30*24*time.Hour),
		IdempotencyTTL:  e.dur("IDEMPOTENCY_TTL", 24*time.Hour),
		ShutdownTimeout: e.dur("SHUTDOWN_TIMEOUT", 30*time.Second),

		EmailProvider:  e.str("EMAIL_PROVIDER", "log"),
		EmailFrom:      e.str("EMAIL_FROM", "Relay <relay@example.com>"),
		ResendAPIKey:   e.str("RESEND_API_KEY", ""),
		ResendURL:      e.str("RESEND_API_URL", "https://api.resend.com"),
		SMTPURL:        e.str("SMTP_URL", ""),
		ExpoPushURL:    e.str("EXPO_PUSH_URL", "https://exp.host/--/api/v2/push/send"),
		ExpoReceiptURL: e.str("EXPO_RECEIPTS_URL", "https://exp.host/--/api/v2/push/getReceipts"),
		ExpoToken:      e.str("EXPO_ACCESS_TOKEN", ""),
		PushReceiptAge: e.dur("PUSH_RECEIPT_DELAY", 15*time.Minute),

		OTLPEndpoint: e.str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		Version:      Version,
		Commit:       Commit,
	}

	if c.DatabaseURL == "" {
		e.fail("DATABASE_URL is required")
	}
	if raw := e.str("RELAY_SECRET_KEY", ""); raw == "" {
		e.fail("RELAY_SECRET_KEY is required (32 random bytes in base64: openssl rand -base64 32)")
	} else if key, err := base64.StdEncoding.DecodeString(raw); err != nil || len(key) != 32 {
		e.fail("RELAY_SECRET_KEY must be 32 bytes in base64")
	} else {
		c.SecretKey = key
	}
	if c.WorkerConcurrency < 1 || c.WorkerConcurrency > 1024 {
		e.fail("WORKER_CONCURRENCY must be between 1 and 1024")
	}
	if c.APIDBConns < 1 || c.APIDBConns > 200 || c.WorkerDBConns < 1 || c.WorkerDBConns > 200 {
		e.fail("API_DB_CONNS and WORKER_DB_CONNS must be between 1 and 200")
	}
	if c.RetryMaxAttempts < 1 || c.RetryMaxAttempts > 20 {
		e.fail("RETRY_MAX_ATTEMPTS must be between 1 and 20")
	}
	if c.LeaseDuration <= c.WebhookTimeout {
		e.fail("DELIVERY_LEASE must be longer than WEBHOOK_TIMEOUT")
	}
	switch c.EmailProvider {
	case "log":
	case "resend":
		if c.ResendAPIKey == "" {
			e.fail("EMAIL_PROVIDER=resend needs RESEND_API_KEY")
		}
	case "smtp":
		if c.SMTPURL == "" {
			e.fail("EMAIL_PROVIDER=smtp needs SMTP_URL")
		}
	default:
		e.fail("EMAIL_PROVIDER must be log, resend or smtp")
	}
	if _, err := mail.ParseAddress(c.EmailFrom); err != nil {
		e.fail("EMAIL_FROM is not a valid address")
	}
	return c, errors.Join(e.errs...)
}

// Version and Commit are set at build time with -ldflags.
var (
	Version = "dev"
	Commit  = "none"
)

type env struct{ errs []error }

func (e *env) fail(msg string) { e.errs = append(e.errs, errors.New(msg)) }

func (e *env) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

func (e *env) int(key string, def int) int {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.fail(fmt.Sprintf("%s must be a whole number", key))
	}
	return n
}

func (e *env) dur(key string, def time.Duration) time.Duration {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		e.fail(fmt.Sprintf("%s must be a duration such as 30s or 24h", key))
	}
	return d
}

func (e *env) bool(key string, def bool) bool {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.fail(fmt.Sprintf("%s must be true or false", key))
	}
	return b
}
