package auth

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestNewKey(t *testing.T) {
	k1, p1 := NewKey()
	k2, _ := NewKey()
	if !strings.HasPrefix(k1, "rk_") || len(k1) != 43 || k1 == k2 || !strings.HasPrefix(k1, p1) || len(p1) != ShownPrefix {
		t.Fatalf("NewKey: %q %q", k1, p1)
	}
}

func TestPrincipal(t *testing.T) {
	tenant := uuid.New()
	p := &Principal{TenantID: &tenant, Scopes: []string{ScopeIngest}}
	if !p.Has(ScopeIngest) || p.Has(ScopeRead) || !p.CanAccess(tenant) || p.CanAccess(uuid.New()) {
		t.Fatal("tenant key")
	}
	admin := &Principal{Scopes: []string{ScopeAdmin}}
	if !admin.Has(ScopeManage) || !admin.CanAccess(uuid.New()) {
		t.Fatal("admin key")
	}
}

func TestValidScopes(t *testing.T) {
	if !ValidScopes([]string{"ingest", "read"}, true) || ValidScopes([]string{"admin"}, true) ||
		!ValidScopes([]string{"admin"}, false) || ValidScopes(nil, false) || ValidScopes([]string{"root"}, false) {
		t.Fatal("ValidScopes")
	}
}
