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

func TestSQLRawOutputWriteBatchWithNilDB(t *testing.T) {
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

	batch := service.MessageBatch{
		service.NewMessage([]byte(`hello`)),
	}

	err := output.WriteBatch(context.Background(), batch)
	require.ErrorIs(t, err, service.ErrNotConnected)
}

func TestSQLRawOutputWriteBatchConcurrentDBClose(t *testing.T) {
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
