package pure_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/v1/internal/component/testutil"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
	"github.com/warpstreamlabs/bento/v1/internal/message"
)

func TestSleep(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
sleep:
  duration: 1ns
`)
	require.NoError(t, err)

	slp, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch([][]byte{[]byte("hello world")})
	msgsOut, err := slp.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Len(t, msgsOut, 1)
	require.Len(t, msgsOut[0], 1)
	assert.Equal(t, "hello world", string(msgsOut[0][0].AsBytes()))
}

func TestSleepExit(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
sleep:
  duration: 10s
`)
	require.NoError(t, err)

	slp, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	doneChan := make(chan struct{})
	go func() {
		_, _ = slp.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("hello world")}))
		close(doneChan)
	}()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()
	assert.NoError(t, slp.Close(ctx))

	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Error("took too long")
	}
}

func TestSleep200Millisecond(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
sleep:
  duration: 200ms
`)
	require.NoError(t, err)

	slp, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	tBefore := time.Now()
	batches, err := slp.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("hello world")}))
	tAfter := time.Now()
	require.NoError(t, err)
	require.Len(t, batches, 1)

	if dur := tAfter.Sub(tBefore); dur < (time.Millisecond * 200) {
		t.Errorf("Message didn't take long enough")
	}
}

func TestSleepInterpolated(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
sleep:
  duration: '${!json("foo")}ms'
`)
	require.NoError(t, err)

	slp, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	tBefore := time.Now()
	batches, err := slp.ProcessBatch(context.Background(), message.QuickBatch([][]byte{
		[]byte(`{"foo":200}`),
	}))
	tAfter := time.Now()
	require.NoError(t, err)
	require.Len(t, batches, 1)

	if dur := tAfter.Sub(tBefore); dur < (time.Millisecond * 200) {
		t.Errorf("Message didn't take long enough")
	}
}
