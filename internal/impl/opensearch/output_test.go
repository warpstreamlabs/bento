package opensearch_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// Flushes triggered while the batch is still being added (buffer over the
// indexer's flush threshold) must honour the context passed to WriteBatch,
// otherwise a stalled cluster keeps the request alive past shutdown or a
// caller timeout.
func TestWriteBatchHonoursContext(t *testing.T) {
	release := make(chan struct{})
	cancelled := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)

	o := outputFromConf(t, `
index: test_ctx_index
id: 'foo'
urls: [ %v ]
action: index
`, `"`+srv.URL+`"`)

	require.NoError(t, o.Connect(t.Context()))

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	// Larger than the bulk indexer's default flush threshold (5 MB).
	big := []byte(`{"a":"` + strings.Repeat("x", 6<<20) + `"}`)

	done := make(chan error, 1)
	go func() {
		done <- o.WriteBatch(ctx, service.MessageBatch{service.NewMessage(big)})
	}()

	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteBatch did not return after its context expired")
	}

	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight bulk request was not cancelled with the batch context")
	}
}
