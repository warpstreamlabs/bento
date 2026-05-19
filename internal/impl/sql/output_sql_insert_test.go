package sql

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"

	"github.com/warpstreamlabs/bento/public/service"
)

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

func TestSQLInsertOutputConnectNoGoroutineLeak(t *testing.T) {
	conf := `
driver: sqlite
dsn: ":memory:"
table: foo
columns: [ id ]
args_mapping: 'root = [ this.id ]'
`
	spec := sqlInsertOutputConfig()
	parsed, err := spec.ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)

	o, err := newSQLInsertOutputFromConfig(parsed, service.MockResources())
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
