// Package auth defines API keys, their scopes and the principal a request acts as.
package auth

import (
	"crypto/rand"
	"slices"

	"github.com/google/uuid"
)

// Scopes a key can hold.
const (
	ScopeIngest = "ingest" // POST /v1/events
	ScopeRead   = "read"   // read events, deliveries, endpoints, subscriptions, dead letters, push tokens
	ScopeManage = "manage" // change endpoints, subscriptions and push tokens; replay dead letters
	ScopeAdmin  = "admin"  // tenants and API keys (only on keys that are not bound to a tenant)
)

// TenantScopes are the scopes a tenant-bound key may hold.
var TenantScopes = []string{ScopeIngest, ScopeRead, ScopeManage}

// AllScopes are the scopes a key that isn't bound to a tenant may hold.
var AllScopes = []string{ScopeIngest, ScopeRead, ScopeManage, ScopeAdmin}

const (
	keyPrefix   = "rk_"
	keyAlphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	keyLength   = 40 // characters after the prefix, about 238 bits
	// ShownPrefix is how many characters of a key the API shows to identify it.
	ShownPrefix = 10
)

// NewKey returns a new API key and the prefix shown in listings.
func NewKey() (key, prefix string) {
	b := make([]byte, keyLength)
	out := make([]byte, keyLength)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		// 256 % 62 = 8: the bias is under 0.5% per character and the key still has over 230 bits.
		out[i] = keyAlphabet[int(b[i])%len(keyAlphabet)]
	}
	key = keyPrefix + string(out)
	return key, key[:ShownPrefix]
}

// Principal is who a request acts as.
type Principal struct {
	KeyID      uuid.UUID
	TenantID   *uuid.UUID // nil: every tenant
	TenantSlug string
	Scopes     []string
}

// Has reports whether the principal holds a scope. Admin keys hold every scope.
func (p *Principal) Has(scope string) bool {
	return slices.Contains(p.Scopes, scope) || slices.Contains(p.Scopes, ScopeAdmin)
}

// CanAccess reports whether the principal may act for the tenant.
func (p *Principal) CanAccess(tenant uuid.UUID) bool {
	return p.TenantID == nil || *p.TenantID == tenant
}

// ValidScopes reports whether every scope is allowed for a key bound (or not) to a tenant.
func ValidScopes(scopes []string, tenantBound bool) bool {
	allowed := AllScopes
	if tenantBound {
		allowed = TenantScopes
	}
	if len(scopes) == 0 {
		return false
	}
	for _, s := range scopes {
		if !slices.Contains(allowed, s) {
			return false
		}
	}
	return true
}
