//go:build !x_bento_extra

package sql

func init() {
	driverField = driverField.LintRule(`root = if this == "duckdb" { [ "Cannot use DuckDB driver outside of CGO build. Use a binary built with tag x_bento_extra, or the CGO Docker image" ] }`)
}
