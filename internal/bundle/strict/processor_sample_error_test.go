package strict

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestProcessorWrapWithErrorSampler(t *testing.T) {
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

	procWithErrorSampler := wrapWithErrorSampling(mock, mgr, 1)

	msg := message.QuickBatch([][]byte{
		[]byte("not a structured doc"),
	})
	msgs, res := procWithErrorSampler.ProcessBatch(tCtx, msg)
	require.Equal(t, "not a structured doc", string(msgs[0].Get(0).AsBytes()))
	require.NoError(t, res)

	logLines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, logLines, 1)

	require.Contains(t, logLines, "{\"@service\":\"bento\",\"error\":\"invalid character 'o' in literal null (expecting 'u')\",\"level\":\"error\",\"msg\":\"sample error payload\",\"parse_payload_error\":\"invalid character 'o' in literal null (expecting 'u')\",\"raw_payload\":\"not a structured doc\"}")

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

	require.Equal(t, "{\"@service\":\"bento\",\"error\":\"invalid character 'o' in literal null (expecting 'u')\",\"level\":\"error\",\"msg\":\"sample error payload\",\"parse_payload_error\":\"invalid character 'o' in literal null (expecting 'u')\",\"raw_payload\":\"not a structured doc\"}", logLines[0])
	require.Equal(t, "{\"@service\":\"bento\",\"error\":\"invalid character 'a' looking for beginning of value\",\"level\":\"error\",\"msg\":\"sample error payload\",\"parse_payload_error\":\"invalid character 'a' looking for beginning of value\",\"raw_payload\":\"another unstructured doc\"}", logLines[1])
}
