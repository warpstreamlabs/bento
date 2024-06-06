package pure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
)

func testGenReader(t testing.TB, confStr string, args ...any) *generateReader {
	pConf, err := genInputSpec().ParseYAML(fmt.Sprintf(confStr, args...), nil)
	require.NoError(t, err)

	r, err := newGenerateReaderFromParsed(pConf, mock.NewManager())
	require.NoError(t, err)

	return r
}

func TestBloblangInterval(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*80)
	defer done()

	b := testGenReader(t, `
mapping: 'root = "hello world"'
interval: 50ms
`)

	err := b.Connect(ctx)
	require.NoError(t, err)

	// First read is immediate.
	m, _, err := b.ReadBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).AsBytes()))

	// Second takes 50ms.
	m, _, err = b.ReadBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).AsBytes()))

	// Third takes another 50ms and therefore times out.
	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "action timed out")

	require.NoError(t, b.Close(context.Background()))
}

func TestBloblangZeroInterval(t *testing.T) {
	_ = testGenReader(t, `
mapping: 'root = "hello world"'
interval: 0s
`)
}

func TestBloblangCron(t *testing.T) {
	t.Skip()

	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*1100)
	defer done()

	b := testGenReader(t, `
mapping: 'root = "hello world"'
interval: '@every 1s'
`)

	assert.NotNil(t, b.schedule)

	err := b.Connect(ctx)
	require.NoError(t, err)

	// First takes 1s so.
	m, _, err := b.ReadBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).AsBytes()))

	// Second takes another 1s and therefore times out.
	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "action timed out")

	require.NoError(t, b.Close(context.Background()))
}

func TestBloblangMapping(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	b := testGenReader(t, `
mapping: |
  root = {
    "id": count("docs")
  }
interval: 1ms
`)

	err := b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, fmt.Sprintf(`{"id":%v}`, i+1), string(m.Get(0).AsBytes()))
	}
}

func TestBloblangRemaining(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	b := testGenReader(t, `
mapping: 'root = "foobar"'
interval: 1ms
count: 10
`)

	err := b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).AsBytes()))
	}

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")
}

func TestBloblangRemainingBatched(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	b := testGenReader(t, `
mapping: 'root = "foobar"'
interval: 1ms
count: 9
batch_size: 2
`)

	err := b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		if i == 4 {
			require.Equal(t, 1, m.Len())
		} else {
			require.Equal(t, 2, m.Len())
			assert.Equal(t, "foobar", string(m[1].AsBytes()))
		}
		assert.Equal(t, "foobar", string(m[0].AsBytes()))
	}

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")
}

func TestBloblangUnbounded(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	b := testGenReader(t, `
mapping: 'root = "foobar"'
interval: 0s
`)

	err := b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).AsBytes()))
	}

	require.NoError(t, b.Close(context.Background()))
}

func TestBloblangUnboundedEmpty(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	b := testGenReader(t, `
mapping: 'root = "foobar"'
interval: ""
`)

	err := b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).AsBytes()))
	}

	require.NoError(t, b.Close(context.Background()))
}
