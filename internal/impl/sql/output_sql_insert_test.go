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

// newTestSQLiteDB opens a real file-backed SQLite database using the modernc
// driver (already a module dependency) and runs createStmt to prepare the
// schema.
//
// A file-backed DSN is used instead of ":memory:" because SQLite in-memory
// databases are per-connection: when the sql.DB pool opens a second connection
// it would see an empty database and return "no such table" errors.
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

// raceBarrier returns a function that blocks each caller until all n callers
// have arrived, then releases them all simultaneously. This maximises
// concurrent overlap and makes the TOCTOU window as wide as possible.
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

// defaultConnSettings returns a minimal connSettings that performs no
// credential rewriting, suitable for tests that inject a *sql.DB directly.
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

// TestSQLInsertOutputWriteBatchBadConnNilRace verifies that a concurrent
// bad-connection nil-reset cannot cause a nil pointer panic inside writeBatch.
//
// Before the fix, writeBatch held only an RLock for its entire duration.
// RWMutex permits concurrent readers, so a writer could legally acquire the
// write lock and zero s.db between the nil check and the subsequent use of
// s.db inside insertBuilder.RunWith — a classic TOCTOU window. The fix
// captures s.db into a local variable immediately after the nil check so that
// both the check and the use refer to the same pointer value.
//
// NOTE: because every access to s.db goes through the mutex, go test -race
// does not flag this. The additional serialisation the race detector imposes
// may also suppress the interleaving needed to trigger the panic. Run without
// -race for the most reliable reproduction of the unfixed behaviour.
func TestSQLInsertOutputWriteBatchBadConnNilRace(t *testing.T) {
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

	// Each goroutine races writeBatch (RLock → nil check → use s.db) against
	// the bad-conn nil-reset (write lock → s.db = nil).
	const workers = 50

	ready := raceBarrier(workers)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready() // hold until all goroutines are lined up

			// Step 1: read path — RLock, nil check, then use s.db.
			_ = output.writeBatch(ctx, batch)

			// Step 2: write path — mirrors WriteBatch's bad-conn error handler.
			output.dbMut.Lock()
			output.db = nil
			output.dbMut.Unlock()

			// Restore so the next goroutine has a live *sql.DB to race against.
			output.dbMut.Lock()
			output.db = db
			output.dbMut.Unlock()
		}()
	}

	wg.Wait()
}

// TestSQLInsertOutputShutdownNilRace verifies that the shutdown goroutine
// zeroing s.db cannot cause a nil pointer panic inside a concurrent writeBatch.
//
// The shutdown goroutine spawned inside Connect acquires the write lock,
// closes the database, and sets s.db = nil — the same write-side operation as
// the bad-conn reset, just from a different call site. The fix (capturing s.db
// into a local variable under the RLock) closes the TOCTOU window for both
// writers identically.
//
// NOTE: go test -race does not flag this. Run without -race to reproduce.
func TestSQLInsertOutputShutdownNilRace(t *testing.T) {
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready()

			// Step 1: read path.
			_ = output.writeBatch(ctx, batch)

			// Step 2: mimic the shutdown goroutine's critical section.
			output.dbMut.Lock()
			if output.db != nil {
				_ = output.db.Close()
				output.db = nil
			}
			output.dbMut.Unlock()

			// Restore for the next goroutine.
			output.dbMut.Lock()
			output.db = db
			output.dbMut.Unlock()
		}()
	}

	wg.Wait()
}
