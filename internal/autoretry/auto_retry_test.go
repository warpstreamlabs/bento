package autoretry_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func TestAutoRetryStreamStopsDuringBackoff(t *testing.T) {
	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetYAML(`
input:
  generate:
    mapping: root = "the future is bright"
    count: 1
    batch_size: 10

output:
  reject: "processing failed"
`))

	strm, err := builder.Build()
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- strm.Run(context.Background())
	}()

	// Let the rejected batch retry long enough to reach the backoff.
	time.Sleep(500 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, strm.Stop(stopCtx))

	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not stop")
	}
}
