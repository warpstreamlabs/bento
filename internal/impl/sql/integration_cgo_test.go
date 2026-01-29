//go:build x_bento_extra

package sql_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
	_ "github.com/warpstreamlabs/bento/public/components/sql"
)

func TestIntegrationDuckDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Create a temp DB file that is automatically cleaned up
	dbFile := filepath.Join(t.TempDir(), "test.duckdb")
	db, err := sql.Open("duckdb", dbFile)
	require.NoError(t, err)

	t.Cleanup(func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Errorf("error closing duckdb database: %v", err)
			}
		}
	})

	require.Eventually(t, func() bool {
		return db.Ping() == nil
	}, time.Second, 100*time.Millisecond, "duckdb did not respond to ping in time")

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
			"foo" varchar(50) not null,
			"bar" integer not null,
			"baz" varchar(50) not null,
			primary key ("foo")
		)`, name))
		return name, err
	}
	_, err = createTable("footable")
	require.NoError(t, err)

	testSuite(t, "duckdb", dbFile, createTable)
}
