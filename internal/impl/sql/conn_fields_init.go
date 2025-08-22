//go:build !x_bento_extra

package sql

import "github.com/warpstreamlabs/bento/public/service"

func init() {
	driverField = service.NewStringEnumField("driver", "mysql", "postgres", "clickhouse", "mssql", "sqlite", "oracle", "snowflake", "trino", "gocosmos", "spanner").
		Description("A database [driver](#drivers) to use.").LintRule(`root = if this == "duckdb" { [ "Cannot use DuckDB driver outside of CGO build. Use a binary built with tag x_bento_extra, or the CGO Docker image" ] }`)
}
