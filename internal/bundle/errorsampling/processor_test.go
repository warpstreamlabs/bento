package errorsampling

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/errorhandling"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

//------------------------------------------------------------------------------

type mockProc struct{}

func (m mockProc) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	for _, m := range msg {
		_, err := m.AsStructuredMut()
		m.ErrorSet(err)
	}
	return []message.Batch{msg}, nil
}

func (m mockProc) Close(ctx context.Context) error {
	// Do nothing as our processor doesn't require resource cleanup.
	return nil
}

//------------------------------------------------------------------------------

func TestProcessorWrapWithErrorLogger(t *testing.T) {
	tCtx := context.Background()
	mgr := mock.NewManager()

	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"

	var buf bytes.Buffer
	logger, err := log.New(&buf, ifs.OS(), lConf)
	require.NoError(t, err)

	mgr.L = logger

	mock := &mockProc{}

	conf := errorhandling.Config{
		Log: errorhandling.LogConfig{
			Enabled:       true,
			AddPayload:    true,
			SamplingRatio: 1,
		},
	}
	procWithErrorSampler := wrapWithErrorLogger(mock, mgr.Logger(), conf)

	msg := message.QuickBatch([][]byte{
		[]byte("not a structured doc"),
	})
	msgs, res := procWithErrorSampler.ProcessBatch(tCtx, msg)
	require.Equal(t, "not a structured doc", string(msgs[0].Get(0).AsBytes()))
	require.NoError(t, res)

	logLines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, logLines, 1)

	require.Contains(t, logLines, "{\"@service\":\"bento\",\"error\":\"invalid character 'o' in literal null (expecting 'u')\",\"level\":\"error\",\"msg\":\"failed processing\",\"raw_payload\":\"not a structured doc\"}")

	buf.Reset()

	batch := message.QuickBatch([][]byte{
		[]byte("not a structured doc"),
		[]byte(`{"foo":"oof"}`),
		[]byte("another unstructured doc"),
		[]byte(`{"bar":"rab"}`),
	})
	msgs, res = procWithErrorSampler.ProcessBatch(tCtx, batch)
	require.Len(t, msgs[0], 4)
	require.NoError(t, res)

	logLines = strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, logLines, 2)

	require.Equal(t, "{\"@service\":\"bento\",\"error\":\"invalid character 'o' in literal null (expecting 'u')\",\"level\":\"error\",\"msg\":\"failed processing\",\"raw_payload\":\"not a structured doc\"}", logLines[0])
	require.Equal(t, "{\"@service\":\"bento\",\"error\":\"invalid character 'a' looking for beginning of value\",\"level\":\"error\",\"msg\":\"failed processing\",\"raw_payload\":\"another unstructured doc\"}", logLines[1])
}
