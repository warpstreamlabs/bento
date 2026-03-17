package sql

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestSQLRawOutputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
query: "INSERT INTO footable (foo) VALUES (?);"
`

	spec := sqlRawOutputConfig()
	env := service.NewEnvironment()

	rawConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	rawOutput, err := newSQLRawOutputFromConfig(rawConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, rawOutput.Close(context.Background()))
}

// TestSQLRawOutputWriteBatchBadConnNilRace verifies that a concurrent
// bad-connection nil-reset cannot cause a nil pointer panic inside writeBatch.
//
// Before the fix, writeBatch held only an RLock and called s.db.ExecContext
// with no nil guard. A writer could legally acquire the write lock and zero
// s.db between the start of writeBatch and the ExecContext call — a classic
// TOCTOU window. The fix captures s.db into a local variable under the RLock
// and adds a nil guard, so both the check and all subsequent uses refer to the
// same pointer value.
//
// NOTE: because every access to s.db goes through the mutex, go test -race
// does not flag this. The additional serialisation the race detector imposes
// may also suppress the interleaving needed to trigger the panic. Run without
// -race for the most reliable reproduction of the unfixed behaviour.
func TestSQLRawOutputWriteBatchBadConnNilRace(t *testing.T) {
	db := newTestSQLiteDB(t, `CREATE TABLE IF NOT EXISTS things (foo TEXT NOT NULL)`)

	output := newSQLRawOutput(
		service.MockResources().Logger(),
		"sqlite",
		"", // dsn unused; we inject db directly below
		"INSERT INTO things (foo) VALUES (?)",
		nil, // no dynamic query
		nil, // no args mapping
		defaultConnSettings(),
		aws.Config{},
	)
	output.db = db

	ctx := context.Background()
	batch := service.MessageBatch{
		service.NewMessage([]byte(`hello`)),
	}

	// Each goroutine races writeBatch (RLock → nil check → ExecContext) against
	// the bad-conn nil-reset (write lock → s.db = nil).
	const workers = 50

	ready := raceBarrier(workers)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready() // hold until all goroutines are lined up

			// Step 1: read path — RLock, nil check, then db.ExecContext.
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

// TestSQLRawOutputShutdownNilRace verifies that the shutdown goroutine zeroing
// s.db cannot cause a nil pointer panic inside a concurrent writeBatch.
//
// The shutdown goroutine spawned inside Connect acquires the write lock, closes
// the database, and sets s.db = nil — the same write-side operation as the
// bad-conn reset, just from a different call site. The fix (capturing s.db into
// a local variable under the RLock) closes the TOCTOU window for both writers
// identically.
//
// NOTE: go test -race does not flag this. Run without -race to reproduce.
func TestSQLRawOutputShutdownNilRace(t *testing.T) {
	db := newTestSQLiteDB(t, `CREATE TABLE IF NOT EXISTS things (foo TEXT NOT NULL)`)

	output := newSQLRawOutput(
		service.MockResources().Logger(),
		"sqlite",
		"",
		"INSERT INTO things (foo) VALUES (?)",
		nil,
		nil,
		defaultConnSettings(),
		aws.Config{},
	)
	output.db = db

	ctx := context.Background()
	batch := service.MessageBatch{
		service.NewMessage([]byte(`hello`)),
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

// TestSQLRawOutputShutdownNilsDB verifies that writeBatch returns
// service.ErrNotConnected after the shutdown goroutine closes and nils s.db.
//
// Before the fix, the shutdown goroutine in Connect closed s.db but never set
// s.db = nil. writeBatch would then find a non-nil but closed *sql.DB, call
// ExecContext, and receive a generic driver error. Because that error does not
// match driver.ErrBadConn, WriteBatch's reconnect branch was never taken and
// the output remained permanently broken until the process restarted.
//
// The fix adds s.db = nil to the shutdown goroutine after Close, consistent
// with the sql_insert shutdown goroutine. writeBatch then sees a nil db, returns
// service.ErrNotConnected, and WriteBatch's caller can trigger a reconnect.
func TestSQLRawOutputShutdownNilsDB(t *testing.T) {
	db := newTestSQLiteDB(t, `CREATE TABLE IF NOT EXISTS things (foo TEXT NOT NULL)`)

	output := newSQLRawOutput(
		service.MockResources().Logger(),
		"sqlite",
		"",
		"INSERT INTO things (foo) VALUES (?)",
		nil,
		nil,
		defaultConnSettings(),
		aws.Config{},
	)
	output.db = db

	// Reproduce what the fixed shutdown goroutine does: close and nil s.db.
	output.dbMut.Lock()
	_ = output.db.Close()
	output.db = nil
	output.dbMut.Unlock()

	ctx := context.Background()
	batch := service.MessageBatch{
		service.NewMessage([]byte(`hello`)),
	}

	err := output.writeBatch(ctx, batch)
	require.ErrorIs(t, err, service.ErrNotConnected)
}
