package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	pkgsftp "github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func newTestSFTPReader(t *testing.T, srv *testSFTPServer, watcher bool) *sftpReader {
	t.Helper()

	conf := fmt.Sprintf(`
address: %s
paths:
  - "*.txt"
credentials:
  username: blobfish
  password: secret
scanner:
  to_the_end: {}
watcher:
  enabled: %t
  minimum_age: 0s
  poll_interval: 50ms
  cache: files_memory
`, srv.address, watcher)

	parsed, err := sftpInputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)
	r, err := newSFTPReaderFromParsed(parsed, service.MockResources(service.MockResourcesOptAddCache("files_memory")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r
}

func newTestSFTPWriter(t *testing.T, srv *testSFTPServer, path, codec string) *sftpWriter {
	t.Helper()

	conf := fmt.Sprintf(`
address: %s
path: %s
credentials:
  username: blobfish
  password: secret
codec: %s
`, srv.address, path, codec)

	parsed, err := sftpOutputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)
	w, err := newWriterFromParsed(parsed, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

// readLikeInput reads and acks one single-message batch the way bento drives
// an input: it connects whenever a read reports service.ErrNotConnected, and
// retries a read that fails with any other error. It returns the error of the
// last attempt.
func readLikeInput(ctx context.Context, r *sftpReader) (string, error) {
	var err error
	for range 5 {
		if err = r.Connect(ctx); err != nil {
			if errors.Is(err, service.ErrEndOfInput) || ctx.Err() != nil {
				return "", err
			}
			continue
		}

		var batch service.MessageBatch
		var ackFn service.AckFunc
		if batch, ackFn, err = r.ReadBatch(ctx); err != nil {
			if ctx.Err() != nil {
				return "", err
			}
			continue
		}
		if len(batch) != 1 {
			return "", fmt.Errorf("expected a batch of 1 message, got %d", len(batch))
		}
		content, err := batch[0].AsBytes()
		if err != nil {
			return "", err
		}
		if err := ackFn(ctx, nil); err != nil {
			return "", err
		}
		return string(content), nil
	}
	return "", err
}

// writeLikeOutput writes msg the way bento drives an output: on
// service.ErrNotConnected it connects and retries. Any other error is
// returned, since a lost connection must be reported as ErrNotConnected.
func writeLikeOutput(ctx context.Context, w *sftpWriter, msg *service.Message) error {
	var err error
	for range 3 {
		if err = w.Write(ctx, msg); !errors.Is(err, service.ErrNotConnected) {
			return err
		}
		if err = w.Connect(ctx); err != nil {
			return err
		}
	}
	return err
}

// After the server dropped the connection, the input kept using the dead
// client. With static paths it closed the client but carried on opening the
// remaining files on the nil client, which panicked. The watcher logged
// "Failed to scan files from path" on every poll forever, inside Connect.
func TestSFTPInputReconnectsAfterConnectionDrop(t *testing.T) {
	for _, watcher := range []bool{false, true} {
		t.Run(fmt.Sprintf("watcher=%t", watcher), func(t *testing.T) {
			srv := startTestSFTPServer(t)
			want := map[string]bool{"a": true, "b": true, "c": true}
			for name := range want {
				srv.writeFile(t, name+".txt", name)
			}

			r := newTestSFTPReader(t, srv, watcher)
			ctx := t.Context()

			got := map[string]int{}
			first, err := readLikeInput(ctx, r)
			require.NoError(t, err)
			got[first]++

			srv.dropConnections()

			for i := 0; len(got) < len(want); i++ {
				require.Less(t, i, 10, "reads never recovered after the connection dropped, got %v", got)

				readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				content, err := readLikeInput(readCtx, r)
				cancel()
				require.NotErrorIs(t, err, service.ErrEndOfInput, "input ended without reading every file, got %v", got)
				if err == nil {
					got[content]++
				}
			}

			for name := range want {
				assert.Equal(t, 1, got[name], "file %s read %d times", name, got[name])
			}
			if !watcher {
				_, err := readLikeInput(ctx, r)
				require.ErrorIs(t, err, service.ErrEndOfInput)
			}
		})
	}
}

// A file whose read is cut short by a lost connection must be read again
// after reconnecting, not dropped.
func TestSFTPInputRereadsFileInterruptedByConnectionDrop(t *testing.T) {
	for _, watcher := range []bool{false, true} {
		t.Run(fmt.Sprintf("watcher=%t", watcher), func(t *testing.T) {
			srv := startTestSFTPServer(t)
			srv.writeFile(t, "a.txt", "a")
			srv.writeFile(t, "b.txt", "b")

			r := newTestSFTPReader(t, srv, watcher)
			ctx := t.Context()

			// Open the first file, then drop the connection before it is read.
			require.NoError(t, r.Connect(ctx))
			r.scannerMut.Lock()
			interrupted := r.currentPath
			r.scannerMut.Unlock()
			require.NotEmpty(t, interrupted)
			srv.dropConnections()

			got := map[string]int{}
			for i := 0; len(got) < 2; i++ {
				require.Less(t, i, 10, "reads never recovered after the connection dropped, got %v", got)

				readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				content, err := readLikeInput(readCtx, r)
				cancel()
				require.NotErrorIs(t, err, service.ErrEndOfInput, "input ended without reading every file, got %v", got)
				if err == nil {
					got[content]++
				}
			}
			assert.Equal(t, map[string]int{"a": 1, "b": 1}, got)
		})
	}
}

// Writes must recover after the server drops the connection, whether the
// writer notices the drop before or during a write.
func TestSFTPOutputReconnectsAfterConnectionDrop(t *testing.T) {
	for _, codec := range []string{"all-bytes", "lines"} {
		t.Run(codec, func(t *testing.T) {
			srv := startTestSFTPServer(t)
			w := newTestSFTPWriter(t, srv, `${! meta("name") }.txt`, codec)
			ctx := t.Context()

			msg := func(name, body string) *service.Message {
				m := service.NewMessage([]byte(body))
				m.MetaSetMut("name", name)
				return m
			}

			require.NoError(t, w.Connect(ctx))
			require.NoError(t, w.Write(ctx, msg("first", "one")))

			for i := range 20 {
				srv.dropConnections()
				require.NoError(t, writeLikeOutput(ctx, w, msg(fmt.Sprintf("file-%d", i), "two")))
			}

			require.NoError(t, writeLikeOutput(ctx, w, msg("last", "three")))
			require.NoError(t, w.Close(ctx))
			assert.Contains(t, srv.readFile(t, "last.txt"), "three")
		})
	}
}

// Closing the input or output must close the SSH connection, not just the
// SFTP session on it.
func TestSFTPClosesSSHConnection(t *testing.T) {
	srv := startTestSFTPServer(t)
	srv.writeFile(t, "a.txt", "a")
	ctx := t.Context()

	w := newTestSFTPWriter(t, srv, "out.txt", "all-bytes")
	require.NoError(t, w.Connect(ctx))
	require.NoError(t, w.Write(ctx, service.NewMessage([]byte("x"))))
	require.NoError(t, w.Close(ctx))

	r := newTestSFTPReader(t, srv, false)
	_, err := readLikeInput(ctx, r)
	require.NoError(t, err)
	require.NoError(t, r.Close(ctx))

	require.Eventually(t, func() bool { return srv.liveConns() == 0 }, 5*time.Second, 10*time.Millisecond,
		"%d SSH connections left open", srv.liveConns())
}

type failingReadCloser struct{ err error }

func (f failingReadCloser) Read([]byte) (int, error) { return 0, f.err }
func (failingReadCloser) Close() error               { return nil }

// A read sent as the connection closes fails with an error that wraps io.EOF.
// The scanner must not take it for the end of the file and ack the file as
// fully read, which would keep the watcher from reading it again and would
// delete it when delete_on_finish is set.
func TestSFTPInputConnectionLostMidReadIsNotEndOfFile(t *testing.T) {
	for _, tt := range []struct {
		name    string
		readErr error
		wantAck error
	}{
		{name: "end of file", readErr: io.EOF},
		{name: "lost connection", readErr: fmt.Errorf("failed to send packet: %w", io.EOF), wantAck: pkgsftp.ErrSSHFxConnectionLost},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := sftpInputSpec().ParseYAML(`
address: localhost:22
paths: [ "*.txt" ]
`, nil)
			require.NoError(t, err)
			r, err := newSFTPReaderFromParsed(parsed, service.MockResources())
			require.NoError(t, err)

			acked := false
			var ackErr error
			scanner, err := r.scannerCtor.Create(connLostReader{failingReadCloser{tt.readErr}}, func(_ context.Context, err error) error {
				acked, ackErr = true, err
				return nil
			}, service.NewScannerSourceDetails())
			require.NoError(t, err)

			// Read the way ReadBatch does: until the scanner returns an error,
			// then close it.
			ctx := t.Context()
			for range 3 {
				_, ackFn, err := scanner.NextBatch(ctx)
				if err != nil {
					break
				}
				require.NoError(t, ackFn(ctx, nil))
			}
			require.NoError(t, scanner.Close(ctx))

			require.True(t, acked)
			if tt.wantAck == nil {
				require.NoError(t, ackErr)
			} else {
				require.ErrorIs(t, ackErr, tt.wantAck)
			}
		})
	}
}
