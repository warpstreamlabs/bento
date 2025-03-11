package errorsampling_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/bundle/errorsampling"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/errorhandling"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

//------------------------------------------------------------------------------

type mockLogger struct {
	log.Modular

	buffer        *bytes.Buffer
	errors        []string
	fields        []map[string]string
	mappingFields []any
}

func (m *mockLogger) Error(format string, v ...any) {
	m.Modular.Error(format, v...)

	errLog := strings.TrimSpace(m.buffer.String())
	logLines := strings.Split(errLog, "\n")

	m.errors = append(m.errors, logLines...)

	m.buffer.Reset()
}

func (m *mockLogger) WithFields(fields map[string]string) log.Modular {
	m.fields = append(m.fields, fields)
	return m
}

func (m *mockLogger) With(args ...any) log.Modular {
	m.mappingFields = append(m.mappingFields, args...)
	return m
}

func newLogger(t *testing.T) *mockLogger {
	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"

	buf := bytes.NewBuffer(nil)
	logWithBuffer, err := log.New(buf, ifs.OS(), lConf)
	require.NoError(t, err)

	return &mockLogger{Modular: logWithBuffer, buffer: buf}
}

//------------------------------------------------------------------------------

func TestErrorSamplingBundleProcessor(t *testing.T) {
	conf := errorhandling.NewConfig()
	conf.Log.Enabled = true
	conf.Log.AddPayload = true
	conf.Log.SamplingRatio = 1

	senv := errorsampling.ErrorSamplingBundle(conf, bundle.GlobalEnvironment)
	tCtx := context.Background()

	pConf, err := testutil.ProcessorFromYAML(`
noop: {}
`)
	require.NoError(t, err)

	logger := newLogger(t)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(senv),
		manager.OptSetLogger(logger),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{
		[]byte("error message"),
	})

	_ = msg.Iter(func(i int, p *message.Part) error {
		p.ErrorSet(errors.New("foo"))
		return nil
	})

	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Equal(t, "error message", string(msgs[0].Get(0).AsBytes()))
	require.NoError(t, res)

	require.NotEmpty(t, logger.errors)
	require.NotEmpty(t, logger.fields)
	require.NotEmpty(t, logger.mappingFields)

	require.Equal(t, "{\"@service\":\"bento\",\"level\":\"error\",\"msg\":\"failed processing\"}", logger.errors[0])
	require.Equal(t, map[string]string{"label": ""}, logger.fields[0])
	require.Equal(t, map[string]string{"path": "root.noop"}, logger.fields[1])
	require.Equal(t, map[string]string{"error": "foo", "raw_payload": "error message"}, logger.fields[2])

	batch := message.QuickBatch([][]byte{
		[]byte("message"),
		[]byte("error message"),
		[]byte("another message"),
		[]byte("another error message"),
	})

	_ = batch.Iter(func(i int, p *message.Part) error {
		if i%2 == 1 {
			p.ErrorSet(errors.New("foo"))
		}
		return nil
	})

	msgs, res = proc.ProcessBatch(tCtx, batch)
	require.Len(t, msgs[0], 4)
	require.NoError(t, res)

	require.Equal(t, "{\"@service\":\"bento\",\"level\":\"error\",\"msg\":\"failed processing\"}", logger.errors[1])
	require.Equal(t, map[string]string{"error": "foo", "raw_payload": "error message"}, logger.fields[3])

	require.Equal(t, "{\"@service\":\"bento\",\"level\":\"error\",\"msg\":\"failed processing\"}", logger.errors[2])
	require.Equal(t, map[string]string{"error": "foo", "raw_payload": "another error message"}, logger.fields[4])
}
