package sql

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"

	"github.com/warpstreamlabs/bento/public/service"
)

// TestSQLRawOutputWriteBatchNilDB exercises the trivial path: writeBatch
// must return ErrNotConnected when s.db is nil, not panic.
func TestSQLRawOutputWriteBatchNilDB(t *testing.T) {
	o := &sqlRawOutput{
		driver:      "sqlite",
		queryStatic: "SELECT 1",
		logger:      service.MockResources().Logger(),
		shutSig:     shutdown.NewSignaller(),
	}
	// s.db is nil — simulating mid-recovery after a bad-conn nil-out.

	err := o.WriteBatch(context.Background(), service.MessageBatch{service.NewMessage(nil)})
	require.ErrorIs(t, err, service.ErrNotConnected)
}

// TestSQLRawOutputConcurrentNilOutRace stresses the writeBatch path while
// another goroutine nils and restores s.db, the way WriteBatch does on
// ErrBadConn. Without the nil-check fix this panics with a nil-pointer
// deref under `go test -race`.
func TestSQLRawOutputConcurrentNilOutRace(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	o := &sqlRawOutput{
		driver:      "sqlite",
		queryStatic: "SELECT 1",
		db:          db,
		logger:      service.MockResources().Logger(),
		shutSig:     shutdown.NewSignaller(),
	}

	batch := service.MessageBatch{service.NewMessage(nil)}

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)

	for range 64 {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("writeBatch panicked: %v", r)
				}
			}()
			for !stop.Load() {
				err := o.WriteBatch(context.Background(), batch)
				if err != nil && !errors.Is(err, service.ErrNotConnected) {
					t.Errorf("unexpected writeBatch error: %v", err)
					return
				}
			}
		})
	}

	// Split critical sections so readers can observe db == nil between them —
	// merging into one lock would never let the nil branch fire.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		o.dbMut.Lock()
		o.db = nil
		o.dbMut.Unlock()
		o.dbMut.Lock()
		o.db = db
		o.dbMut.Unlock()
	}

	stop.Store(true)
	wg.Wait()
}

func TestSQLRawOutputConnectNoGoroutineLeak(t *testing.T) {
	conf := `
driver: sqlite
dsn: ":memory:"
query: "SELECT 1"
`
	parsed, err := sqlRawOutputConfig().ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)

	o, err := newSQLRawOutputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	assertNoConnectGoroutineLeak(t, o, func() {
		o.dbMut.Lock()
		if o.db != nil {
			_ = o.db.Close()
			o.db = nil
		}
		o.dbMut.Unlock()
	})
}

// connectCloser is satisfied by *sqlRawOutput and *sqlInsertOutput.
type connectCloser interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
}

// assertNoConnectGoroutineLeak runs repeated reconnect cycles and asserts the
// goroutine count does not scale with iteration count — guards against the
// pre-fix regression where Connect spawned a new watcher each call.
func assertNoConnectGoroutineLeak(t *testing.T, o connectCloser, nilDB func()) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, o.Connect(ctx))

	// Warm up so cgo/sqlite/finalizer goroutines settle before measuring.
	for range 5 {
		nilDB()
		require.NoError(t, o.Connect(ctx))
	}

	before := runtime.NumGoroutine()
	const iterations = 10
	for range iterations {
		nilDB()
		require.NoError(t, o.Connect(ctx))
	}

	// A real per-Connect leak would push the delta to ~iterations.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine()-before <= 2
	}, time.Second, 5*time.Millisecond,
		"Connect leaks goroutines: before=%d current=%d", before, runtime.NumGoroutine())

	require.NoError(t, o.Close(ctx))
}
