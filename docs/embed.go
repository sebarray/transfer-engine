// Package docs contains the embedded public API contract.
package docs

import _ "embed"

// OpenAPI is the same contract published in docs/openapi.json.
//
//go:embed openapi.json
var OpenAPI string
