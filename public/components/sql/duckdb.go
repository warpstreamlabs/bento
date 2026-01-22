//go:build ((darwin || linux) && (amd64 || arm64)) || (windows && amd64)

package sql

import (
	// Import only for supported pre-built binaries.
	_ "github.com/duckdb/duckdb-go/v2"
)
