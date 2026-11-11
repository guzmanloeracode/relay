package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("RELAY_SECRET_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RetryMaxAttempts != 8 || c.WebhookTimeout != 10*time.Second || c.RetryBase != 75*time.Second ||
		c.EmailProvider != "log" || len(c.SecretKey) != 32 || c.AllowPrivateNetworks {
		t.Fatalf("defaults %+v", c)
	}
}

func TestLoadReportsEveryProblem(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RELAY_SECRET_KEY", "short")
	t.Setenv("WORKER_CONCURRENCY", "lots")
	t.Setenv("WEBHOOK_TIMEOUT", "1 minute")
	t.Setenv("EMAIL_PROVIDER", "resend")
	t.Setenv("ALLOW_PRIVATE_NETWORKS", "maybe")
	_, err := Load()
	if err == nil {
		t.Fatal("no error")
	}
	for _, want := range []string{"DATABASE_URL", "RELAY_SECRET_KEY", "WORKER_CONCURRENCY", "WEBHOOK_TIMEOUT", "RESEND_API_KEY", "ALLOW_PRIVATE_NETWORKS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

func TestLoadChecksRelations(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("RELAY_SECRET_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	t.Setenv("DELIVERY_LEASE", "5s")
	t.Setenv("EMAIL_PROVIDER", "smtp")
	t.Setenv("EMAIL_FROM", "not an address")
	t.Setenv("RETRY_MAX_ATTEMPTS", "50")
	_, err := Load()
	for _, want := range []string{"DELIVERY_LEASE", "SMTP_URL", "EMAIL_FROM", "RETRY_MAX_ATTEMPTS"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s: %v", want, err)
		}
	}
}
