//go:build x_bento_extra

package sql_test

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service/integration"
	"path/filepath"
	"testing"
)

func TestIntegrationDuckDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create a temp DB file that is automatically cleaned up
	dbFile := filepath.Join(t.TempDir(), "test.duckdb")
	db, err := sql.Open("duckdb", dbFile)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
			"foo" varchar(50) not null,
			"bar" integer not null,
			"baz" varchar(50) not null,
			primary key ("foo")
		)`, name))
		return name, err
	}

	require.NoError(t, db.Ping())
	_, err = createTable("footable")
	require.NoError(t, err)

	testSuite(t, "duckdb", dbFile, createTable)
}
