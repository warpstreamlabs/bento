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
		wg       sync.WaitGroup
		stop     atomic.Bool
		panicked atomic.Bool
	)

	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
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
		}()
	}

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
	require.False(t, panicked.Load(), "writeBatch must not panic on nil s.db")
}

// TestSQLRawOutputConnectNoGoroutineLeak verifies that repeated
// connect/disconnect cycles (as caused by IAM token rotation in prod)
// don't accumulate watcher goroutines.
func TestSQLRawOutputConnectNoGoroutineLeak(t *testing.T) {
	conf := `
driver: sqlite
dsn: ":memory:"
query: "SELECT 1"
`
	spec := sqlRawOutputConfig()
	parsed, err := spec.ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)

	o, err := newSQLRawOutputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, o.Connect(ctx))

	for i := 0; i < 50; i++ {
		o.dbMut.Lock()
		if o.db != nil {
			_ = o.db.Close()
			o.db = nil
		}
		o.dbMut.Unlock()
		require.NoError(t, o.Connect(ctx))
	}

	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		o.dbMut.Lock()
		if o.db != nil {
			_ = o.db.Close()
			o.db = nil
		}
		o.dbMut.Unlock()
		require.NoError(t, o.Connect(ctx))
	}
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	require.LessOrEqual(t, after-before, 2, "Connect leaks goroutines: before=%d after=%d", before, after)

	require.NoError(t, o.Close(ctx))
}
