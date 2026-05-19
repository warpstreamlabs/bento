package sql

import (
	"context"
	"testing"

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
	parsed, err := sqlInsertOutputConfig().ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)

	o, err := newSQLInsertOutputFromConfig(parsed, service.MockResources())
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
