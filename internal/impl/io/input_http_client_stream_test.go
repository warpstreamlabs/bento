package io

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// The context given to Connect covers the connection only: cancelling it once connected must not
// cut the response the input keeps streaming.
func TestHTTPClientStreamOutlivesConnectContext(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("foo\n"))
		w.(http.Flusher).Flush()
		<-release
		_, _ = w.Write([]byte("bar\n"))
	}))
	t.Cleanup(tserve.Close)
	t.Cleanup(releaseServer)

	conf, err := httpClientInputSpec().ParseYAML(fmt.Sprintf(`
url: %v/stream
stream:
  enabled: true
  reconnect: false
`, tserve.URL), nil)
	require.NoError(t, err)

	rdr, err := newHTTPClientInputFromParsed(conf, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdr.Close(context.Background()) })

	connectCtx, cancelConnect := context.WithCancel(t.Context())
	require.NoError(t, rdr.Connect(connectCtx))
	cancelConnect()

	batch, _, err := rdr.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 1)
	first, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo", string(first))

	releaseServer()
	batch, _, err = rdr.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 1)
	second, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "bar", string(second))
}
