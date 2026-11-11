// Package apispec embeds Relay's OpenAPI 3.1 document, served at /openapi.yaml and used by the contract tests.
package apispec

import _ "embed"

// Spec is api/openapi.yaml.
//
//go:embed openapi.yaml
var Spec []byte
