package sftp

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// countingLister counts the directory listings the watcher makes, one per
// poll of a single glob.
type countingLister struct {
	sftp.FileLister
	lists atomic.Int32
}

func (c *countingLister) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	if r.Method == "List" {
		c.lists.Add(1)
	}
	return c.FileLister.Filelist(r)
}

// newInMemSFTPClient returns a client for an in-memory SFTP server, and the
// lister that counts its directory listings.
func newInMemSFTPClient(t *testing.T) (*sftp.Client, *countingLister) {
	t.Helper()

	handlers := sftp.InMemHandler()
	lister := &countingLister{FileLister: handlers.FileList}
	handlers.FileList = lister

	clientConn, serverConn := net.Pipe()
	server := sftp.NewRequestServer(serverConn, handlers)
	go func() { _ = server.Serve() }()

	client, err := sftp.NewClientPipe(clientConn, clientConn)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, lister
}

func newTestWatcher(pollInterval time.Duration) *watcherPathProvider {
	return &watcherPathProvider{
		mgr:          service.MockResources(service.MockResourcesOptAddCache("files_memory")),
		cacheName:    "files_memory",
		pollInterval: pollInterval,
		targetPaths:  []string{"/*.txt"},
	}
}

// nextWithin calls w.Next and fails the test if it doesn't return within
// timeout, rather than hanging when Next ignores its context.
func nextWithin(t *testing.T, ctx context.Context, w *watcherPathProvider, client *sftp.Client, timeout time.Duration) (string, error) {
	t.Helper()

	type result struct {
		path string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		path, err := w.Next(ctx, client)
		done <- result{path, err}
	}()
	select {
	case res := <-done:
		return res.path, res.err
	case <-time.After(timeout):
		require.FailNow(t, "Next did not return", "waited %v", timeout)
		return "", nil
	}
}

// With nothing to read, the watcher used to poll again straight away instead
// of waiting poll_interval, and didn't check its context between polls, so
// it never returned.
func TestSFTPWatcherWaitsPollIntervalBetweenPolls(t *testing.T) {
	client, lister := newInMemSFTPClient(t)
	w := newTestWatcher(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 450*time.Millisecond)
	defer cancel()
	_, err := nextWithin(t, ctx, w, client, 5*time.Second)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// One poll straight away, then one every 100ms.
	polls := lister.lists.Load()
	assert.GreaterOrEqual(t, polls, int32(3))
	assert.LessOrEqual(t, polls, int32(6))
}

// The first poll must not wait for poll_interval, and a file found by a later
// poll is returned once that poll runs.
func TestSFTPWatcherPollsStraightAwayThenEveryInterval(t *testing.T) {
	client, _ := newInMemSFTPClient(t)
	f, err := client.Create("/a.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w := newTestWatcher(300 * time.Millisecond)
	ctx := t.Context()

	start := time.Now()
	path, err := nextWithin(t, ctx, w, client, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "/a.txt", path)
	assert.Less(t, time.Since(start), 200*time.Millisecond, "the first poll waited for poll_interval")

	f, err = client.Create("/b.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	path, err = nextWithin(t, ctx, w, client, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "/b.txt", path)
	assert.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond, "the second poll did not wait for poll_interval")
}
