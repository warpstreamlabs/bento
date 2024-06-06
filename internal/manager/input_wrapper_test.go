package manager_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/warpstreamlabs/bento/v1/internal/component/testutil"
	"github.com/warpstreamlabs/bento/v1/internal/manager"
	bmock "github.com/warpstreamlabs/bento/v1/internal/manager/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInputWrapperSwap(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	conf, err := testutil.InputFromYAML(`
generate:
  interval: 10ms
  mapping: 'root.name = "from root generate"'
`)
	require.NoError(t, err)

	bMgr := bmock.NewManager()

	iWrapped, err := bMgr.NewInput(conf)
	require.NoError(t, err)

	iWrapper := manager.WrapInput(iWrapped)
	select {
	case tran, open := <-iWrapper.TransactionChan():
		require.True(t, open)
		assert.Equal(t, `{"name":"from root generate"}`, string(tran.Payload.Get(0).AsBytes()))
		assert.NoError(t, tran.Ack(ctx, nil))
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	for i := 0; i < 5; i++ {
		conf, err := testutil.InputFromYAML(fmt.Sprintf(`
generate:
  interval: 10ms
  mapping: 'root.name = "from generate %v"'
`, i))
		require.NoError(t, err)

		go func() {
			assert.NoError(t, iWrapper.CloseExistingInput(ctx, true))

			iWrapped, err = bMgr.NewInput(conf)
			assert.NoError(t, err)

			iWrapper.SwapInput(iWrapped)
		}()

		expected := fmt.Sprintf(`{"name":"from generate %v"}`, i)
	consumeLoop:
		for {
			select {
			case tran, open := <-iWrapper.TransactionChan():
				require.True(t, open, i)

				actual := string(tran.Payload.Get(0).AsBytes())
				assert.NoError(t, tran.Ack(ctx, nil), i)
				if expected == actual {
					break consumeLoop
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err(), i)
			}
		}
	}

	iWrapper.TriggerStopConsuming()
	require.NoError(t, iWrapper.WaitForClose(ctx))
}
