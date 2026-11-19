package crdb_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/lib/pq"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationExploration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	resource := pool.RunT(t, "cockroachdb/cockroach",
		dockertest.WithTag("latest"),
		dockertest.WithCmd([]string{"start-single-node", "--insecure"}),
		dockertest.WithoutReuse(),
	)

	port := resource.GetPort("26257/tcp")
	dsn := fmt.Sprintf("postgres://root@localhost:%v/defaultdb?sslmode=disable", port)

	var pgpool *pgxpool.Pool
	var err error

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		if pgpool == nil {
			if pgpool, err = pgxpool.New(context.Background(), dsn); err != nil {
				return err
			}
		}
		// Enable changefeeds
		if _, err = pgpool.Exec(context.Background(), "SET CLUSTER SETTING kv.rangefeed.enabled = true;"); err != nil {
			return err
		}
		// Create table
		_, err = pgpool.Exec(context.Background(), "CREATE TABLE foo (a INT PRIMARY KEY);")
		return err
	}))
	t.Cleanup(func() {
		pgpool.Close()
	})

	cfdb, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	// Create a backlog of rows
	i := 0
	for ; i < 100; i++ {
		// Insert some rows
		if _, err = pgpool.Exec(context.Background(), fmt.Sprintf("INSERT INTO foo VALUES (%v);", i)); err != nil {
			return
		}
	}

	rowsCtx, done := context.WithCancel(context.Background())

	rows, err := cfdb.QueryContext(rowsCtx, "EXPERIMENTAL CHANGEFEED FOR foo WITH UPDATED")
	require.NoError(t, err)

	var latestCursor string
	for j := range 100 {
		require.True(t, rows.Next())

		var a, b, c []byte
		require.NoError(t, rows.Scan(&a, &b, &c))

		gObj, err := gabs.ParseJSON(c)
		require.NoError(t, err)

		latestCursor, _ = gObj.S("updated").Data().(string)
		assert.Equal(t, float64(j), gObj.S("after", "a").Data(), gObj.String())
	}

	require.NoError(t, rows.Err(), "checking rows.Err()")

	done()

	cfdb.Close()
	rows.Close()

	// Insert some more rows
	for ; i < 150; i++ {
		if _, err = pgpool.Exec(context.Background(), fmt.Sprintf("INSERT INTO foo VALUES (%v);", i)); err != nil {
			t.Error(err)
		}
	}

	// Create a new changefeed with a cursor set to the latest updated value
	cfdb, err = sql.Open("postgres", dsn)
	require.NoError(t, err)

	rowsCtx, done = context.WithCancel(context.Background())

	rows, err = cfdb.QueryContext(rowsCtx, "EXPERIMENTAL CHANGEFEED FOR foo WITH UPDATED, CURSOR=\""+latestCursor+"\"")
	require.NoError(t, err)

	for j := range 50 {
		require.True(t, rows.Next())

		var a, b, c []byte
		require.NoError(t, rows.Scan(&a, &b, &c))

		gObj, err := gabs.ParseJSON(c)
		require.NoError(t, err)

		assert.Equal(t, float64(j+100), gObj.S("after", "a").Data(), gObj.String())
	}

	done()

	require.NoError(t, rows.Err(), "checking rows.Err()")

	cfdb.Close()
	rows.Close()
}
