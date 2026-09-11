package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool(t.Context(), "", dockertest.WithMaxWait(time.Minute))
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	t.Cleanup(func() {
		if err := pool.Close(context.WithoutCancel(t.Context())); err != nil {
			t.Logf("pool.Close() error: %v", err)
		}
	})

	resource := pool.RunT(t, "postgres",
		dockertest.WithContainerConfig(func(c *dockercontainer.Config) {
			c.ExposedPorts = dockernetwork.PortSet{
				dockernetwork.MustParsePort("5432/tcp"): {},
			}
		}),
		dockertest.WithEnv([]string{
			"POSTGRES_USER=testuser",
			"POSTGRES_PASSWORD=testpass",
			"POSTGRES_DB=testdb",
		}),
		dockertest.WithoutReuse(),
	)

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table "%s" (
  "foo" varchar not null,
  "bar" varchar not null,
  primary key ("foo")
)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", resource.GetPort("5432/tcp"))
	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	template := `
cache_resources:
  - label: testcache
    sql:
      driver: postgres
      dsn: $VAR1
      table: $VAR2
      key_column: foo
      value_column: bar
      set_suffix: "ON CONFLICT (foo) DO UPDATE SET bar=excluded.bar"
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
		integration.CacheTestMissingKeyExists(),
		integration.CacheTestExistsAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptVarSet("VAR1", dsn),
		integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
			tableName := strings.ReplaceAll(vars.ID, "-", "_")
			tableName = "table_" + tableName
			vars.General["VAR2"] = tableName
			_, err := createTable(tableName)
			require.NoError(t, err)
		}),
	)
}
