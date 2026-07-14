//go:build x_bento_extra

package sql_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
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

	require.NoError(t, db.Ping())

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

func TestIntegrationDuckDBAppender(t *testing.T) {
	integration.CheckSkip(t)

	db := filepath.Join(t.TempDir(), "vault.duckdb")

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: |
      root.duck = ["Scrooge McDuck", "Donald Duck", "Daffy Duck"][counter() %% 3]
      root.gold_coins = counter() * 10
    count: 5
    interval: ""

output:
  duckdb_append:
    dsn: %s
    table: vault_deposits
    columns: [duck, gold_coins]
    args_mapping: "root = [this.duck, this.gold_coins]"
    init_statement: |
      CREATE TABLE IF NOT EXISTS vault_deposits (
        duck       VARCHAR,
        gold_coins BIGINT
      )

logger:
  level: none
`, db)))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	require.NoError(t, stream.Run(context.Background()))

	// open a fresh connection to verify the written data
	dbConn, err := sql.Open("duckdb", db)
	require.NoError(t, err)
	defer dbConn.Close()

	var totalDeposits int
	require.NoError(t, dbConn.QueryRow(`SELECT COUNT(*) FROM vault_deposits`).Scan(&totalDeposits))
	assert.Equal(t, 5, totalDeposits)

	var totalCoins int64
	require.NoError(t, dbConn.QueryRow(`SELECT SUM(gold_coins) FROM vault_deposits`).Scan(&totalCoins))
	assert.EqualValues(t, 10+20+30+40+50, totalCoins)
}
