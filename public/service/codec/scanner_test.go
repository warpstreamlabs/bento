package codec_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/codec"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestInteropCodecOldStyle(t *testing.T) {
	confSpec := service.NewConfigSpec().Fields(codec.DeprecatedCodecFields("lines")...)
	pConf, err := confSpec.ParseYAML(`
codec: lines
max_buffer: 1000000
`, nil)
	require.NoError(t, err)

	rdr, err := codec.DeprecatedCodecFromParsed(pConf)
	require.NoError(t, err)

	buf := bytes.NewReader([]byte(`first
second
third`))
	var acked bool
	strm, err := rdr.Create(io.NopCloser(buf), func(ctx context.Context, err error) error {
		acked = true
		return nil
	}, service.NewScannerSourceDetails())
	require.NoError(t, err)

	for _, s := range []string{
		"first", "second", "third",
	} {
		m, aFn, err := strm.NextBatch(context.Background())
		require.NoError(t, err)
		require.Len(t, m, 1)
		mBytes, err := m[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, s, string(mBytes))
		require.NoError(t, aFn(context.Background(), nil))
		assert.False(t, acked)
	}

	_, _, err = strm.NextBatch(context.Background())
	require.Equal(t, io.EOF, err)

	require.NoError(t, strm.Close(context.Background()))
	assert.True(t, acked)
}

func TestInteropCodecNewStyle(t *testing.T) {
	confSpec := service.NewConfigSpec().Fields(codec.DeprecatedCodecFields("lines")...)
	pConf, err := confSpec.ParseYAML(`
scanner:
  lines:
    custom_delimiter: 'X'
    max_buffer_size: 200
`, nil)
	require.NoError(t, err)

	rdr, err := codec.DeprecatedCodecFromParsed(pConf)
	require.NoError(t, err)

	buf := bytes.NewReader([]byte(`firstXsecondXthird`))
	var acked bool
	strm, err := rdr.Create(io.NopCloser(buf), func(ctx context.Context, err error) error {
		acked = true
		return nil
	}, service.NewScannerSourceDetails())
	require.NoError(t, err)

	for _, s := range []string{
		"first", "second", "third",
	} {
		m, aFn, err := strm.NextBatch(context.Background())
		require.NoError(t, err)
		require.Len(t, m, 1)
		mBytes, err := m[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, s, string(mBytes))
		require.NoError(t, aFn(context.Background(), nil))
		assert.False(t, acked)
	}

	_, _, err = strm.NextBatch(context.Background())
	require.Equal(t, io.EOF, err)

	require.NoError(t, strm.Close(context.Background()))
	assert.True(t, acked)
}

func TestInteropCodecDefault(t *testing.T) {
	confSpec := service.NewConfigSpec().Fields(codec.DeprecatedCodecFields("lines")...)
	pConf, err := confSpec.ParseYAML(`{}`, nil)
	require.NoError(t, err)

	rdr, err := codec.DeprecatedCodecFromParsed(pConf)
	require.NoError(t, err)

	buf := bytes.NewReader([]byte("first\nsecond\nthird"))
	var acked bool
	strm, err := rdr.Create(io.NopCloser(buf), func(ctx context.Context, err error) error {
		acked = true
		return nil
	}, service.NewScannerSourceDetails())
	require.NoError(t, err)

	for _, s := range []string{
		"first", "second", "third",
	} {
		m, aFn, err := strm.NextBatch(context.Background())
		require.NoError(t, err)
		require.Len(t, m, 1)
		mBytes, err := m[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, s, string(mBytes))
		require.NoError(t, aFn(context.Background(), nil))
		assert.False(t, acked)
	}

	_, _, err = strm.NextBatch(context.Background())
	require.Equal(t, io.EOF, err)

	require.NoError(t, strm.Close(context.Background()))
	assert.True(t, acked)
}
