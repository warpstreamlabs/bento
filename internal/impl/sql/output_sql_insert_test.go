package sql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/Jeffail/shutdown"
	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"

	"github.com/warpstreamlabs/bento/public/service"
)

func newTestSQLiteDB(t *testing.T, createStmt string) *sql.DB {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "test_*.db")
	if err != nil {
		t.Fatalf("create temp db file: %v", err)
	}
	f.Close()

	dsn := fmt.Sprintf("file:%s?cache=shared&mode=rwc", f.Name())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(createStmt); err != nil {
		t.Fatalf("create table: %v", err)
	}

	return db
}

func raceBarrier(n int) func() {
	var (
		mu      sync.Mutex
		cond    = sync.NewCond(&mu)
		waiting int
	)
	return func() {
		mu.Lock()
		defer mu.Unlock()
		waiting++
		if waiting == n {
			cond.Broadcast()
		} else {
			for waiting < n {
				cond.Wait()
			}
		}
	}
}

func defaultConnSettings() *connSettings {
	return &connSettings{
		getCredentials: func(dsn, driver string) (string, string, error) {
			return dsn, "", nil
		},
	}
}

func TestSQLInsertOutputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
table: quack
columns: [ foo ]
args_mapping: 'root = [ this.id ]'
`

	spec := sqlInsertOutputConfig()
	env := service.NewEnvironment()

	insertConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	insertOutput, err := newSQLInsertOutputFromConfig(insertConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, insertOutput.Close(context.Background()))
}

func TestSQLInsertOutputWriteBatchWithNilDB(t *testing.T) {
	output := &sqlInsertOutput{
		logger:       service.MockResources().Logger(),
		shutSig:      shutdown.NewSignaller(),
		driver:       "sqlite",
		builder:      squirrel.Insert("things").Columns("foo"),
		connSettings: defaultConnSettings(),
	}

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar"}`)),
	}

	err := output.WriteBatch(context.Background(), batch)
	require.ErrorIs(t, err, service.ErrNotConnected)
}

func TestSQLInsertOutputWriteBatchConcurrentDBClose(t *testing.T) {
	db := newTestSQLiteDB(t, `CREATE TABLE IF NOT EXISTS things (foo TEXT NOT NULL)`)

	output := &sqlInsertOutput{
		logger:       service.MockResources().Logger(),
		shutSig:      shutdown.NewSignaller(),
		driver:       "sqlite",
		builder:      squirrel.Insert("things").Columns("foo"),
		connSettings: defaultConnSettings(),
		db:           db,
	}

	ctx := context.Background()
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar"}`)),
	}

	const workers = 50

	ready := raceBarrier(workers)
	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			ready()

			_ = output.WriteBatch(ctx, batch)

			output.dbMut.Lock()
			if output.db != nil {
				_ = output.db.Close()
				output.db = nil
			}
			output.dbMut.Unlock()

			output.dbMut.Lock()
			output.db = db
			output.dbMut.Unlock()
		})
	}

	wg.Wait()
}
