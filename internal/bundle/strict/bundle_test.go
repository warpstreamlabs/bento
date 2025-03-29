package strict_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/bundle/strict"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/internal/impl/io"
	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func TestStrictBundleProcessor(t *testing.T) {
	senv := strict.StrictBundle(bundle.GlobalEnvironment)
	tCtx := context.Background()

	pConf, err := testutil.ProcessorFromYAML(`
bloblang: root = this
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(senv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")

	msg = message.QuickBatch([][]byte{[]byte(`{"hello":"world"}`)})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, `{"hello":"world"}`, string(msgs[0].Get(0).AsBytes()))
}

func TestStrictBundleProcessorNested(t *testing.T) {
	senv := strict.StrictBundle(bundle.GlobalEnvironment)
	tCtx := context.Background()

	pConf, err := testutil.ProcessorFromYAML(`
processors:
 - bloblang: root = this
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(senv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")

	msg = message.QuickBatch([][]byte{[]byte(`{"hello":"world"}`)})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, `{"hello":"world"}`, string(msgs[0].Get(0).AsBytes()))
}

func TestDisableStrictBundleProcessor(t *testing.T) {
	senv := strict.StrictBundle(bundle.GlobalEnvironment)
	tCtx := context.Background()

	pConf, err := testutil.ProcessorFromYAML(`
processors:
 - bloblang: root = this
 - catch: []
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(senv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Len(t, msgs, 1)
	require.NoError(t, res)
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).AsBytes()))

	msg = message.QuickBatch([][]byte{[]byte(`{"hello":"world"}`)})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, `{"hello":"world"}`, string(msgs[0].Get(0).AsBytes()))
}

func TestStrictBundleProcessorMultiMessage(t *testing.T) {
	senv := strict.StrictBundle(bundle.GlobalEnvironment)
	tCtx := context.Background()

	pConf, err := testutil.ProcessorFromYAML(`
processors:
  - mapping: |
      root = this
  - mapping: |
      root = "What is love?"
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(senv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{
		[]byte("not a structured doc"),
		[]byte(`{"foo":"oof"}`),
		[]byte(`{"bar":"rab"}`),
	})
	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")

	// Ensure the ordering of the message does not influence the error message
	msg = message.QuickBatch([][]byte{
		[]byte(`{"foo":"oof"}`),
		[]byte("not a structured doc"),
		[]byte(`{"bar":"rab"}`),
	})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")

	// Multiple errored messages
	msg = message.QuickBatch([][]byte{
		[]byte(`{"foo":"oof"}`),
		[]byte("not a structured doc"),
		[]byte(`another unstructred doc`),
	})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")

	// Check if the next mapping Processor is reached when no error is present (happy path)
	msg = message.QuickBatch([][]byte{
		[]byte(`{"msg":"Baby don't hurt me"}`),
		[]byte(`{"msg":"Don't hurt me"}`),
		[]byte(`{"msg":"No more"}`),
	})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.NotEmpty(t, msgs)
	require.NoError(t, res)

	assert.Equal(t, 3, msgs[0].Len())
	assert.Equal(t, "What is love?", string(msgs[0].Get(0).AsBytes()))
	assert.Equal(t, "What is love?", string(msgs[0].Get(1).AsBytes()))
	assert.Equal(t, "What is love?", string(msgs[0].Get(2).AsBytes()))

}

// FIXME: The output should be in memory, not a file.
func TestStrictBundleOutput(t *testing.T) {
	streamBuilder := service.NewStreamBuilder()
	err := streamBuilder.SetLoggerYAML(`level: off`)
	require.NoError(t, err)

	// error all messages
	err = streamBuilder.AddProcessorYAML(`mapping: root = throw("error")`)
	require.NoError(t, err)

	// include a DLQ where errored messages go to stdout
	err = streamBuilder.AddOutputYAML(fmt.Sprintf(`
switch: 
  cases:
    - check: !errored()
      continue: true
      output:
        file:
          path: %s/data.txt
          codec: lines
    - check: errored()
      continue: false
      output:
        stdout: {}
`, t.TempDir()))
	require.NoError(t, err)

	sendFn, err := streamBuilder.AddProducerFunc()
	require.NoError(t, err)

	stream, err := streamBuilder.BuildStrict()
	require.NoError(t, err)

	// redirect stdout
	originalStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() {
		os.Stdout = originalStdout
	}()

	done := make(chan struct{})

	go func() {
		defer close(done)
		perr := sendFn(context.Background(), service.NewMessage([]byte("praise be to the omnissiah")))
		require.NoError(t, perr)

		perr = stream.StopWithin(time.Second)
		require.NoError(t, perr)
	}()

	err = stream.Run(context.Background())
	require.NoError(t, err)

	<-done

	var buf bytes.Buffer
	w.Close()
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	r.Close()

	assert.Equal(t, "praise be to the omnissiah\n", buf.String())
}

func TestDisableStrictBundleBloblangFunction(t *testing.T) {
	tCtx := context.Background()
	pConf, err := testutil.ProcessorFromYAML(`
processors:
  - mapping: |
      root = this
  - mapping: |
      root = if errored() { "naughty" } else { "nice" }
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		strict.OptSetStrictModeFromManager()...,
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Len(t, msgs, 1)
	require.NoError(t, res)
	assert.Equal(t, "naughty", string(msgs[0].Get(0).AsBytes()))

	msg = message.QuickBatch([][]byte{[]byte(`{"hello":"world"}`)})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "nice", string(msgs[0].Get(0).AsBytes()))
}

func TestDisableStrictBundleBloblangMethods(t *testing.T) {
	tCtx := context.Background()
	pConf, err := testutil.ProcessorFromYAML(`
processors:
  - mapping: |
      root = this.catch(err -> {"error": err})
  - mapping: |
      root = if this.exists("error") { "naughty" } else { "nice" }
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		strict.OptSetStrictModeFromManager()...,
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Len(t, msgs, 1)
	require.NoError(t, res)
	assert.Equal(t, "naughty", string(msgs[0].Get(0).AsBytes()))

	msg = message.QuickBatch([][]byte{[]byte(`{"hello":"world"}`)})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "nice", string(msgs[0].Get(0).AsBytes()))
}
