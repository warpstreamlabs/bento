package sftp

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	pkgsftp "github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/codec"
)

// blockingPathProvider yields a single path and then blocks in Next until the
// context is cancelled, like the watcher does between polls.
type blockingPathProvider struct {
	path string
	done bool
}

func (b *blockingPathProvider) Next(ctx context.Context, _ *pkgsftp.Client) (string, error) {
	if !b.done {
		b.done = true
		return b.path, nil
	}
	<-ctx.Done()
	return "", ctx.Err()
}

func (b *blockingPathProvider) Ack(context.Context, string, error) error { return nil }

type oneMsgCodec struct{}

func (oneMsgCodec) Create(rdr io.ReadCloser, aFn service.AckFunc, _ *service.ScannerSourceDetails) (codec.DeprecatedFallbackStream, error) {
	return &oneMsgStream{rdr: rdr, aFn: aFn}, nil
}
func (oneMsgCodec) Close(context.Context) error { return nil }

type oneMsgStream struct {
	rdr  io.ReadCloser
	aFn  service.AckFunc
	sent bool
}

func (s *oneMsgStream) NextBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	if s.sent {
		return nil, nil, io.EOF
	}
	s.sent = true
	return service.MessageBatch{service.NewMessage([]byte("hello"))}, s.aFn, nil
}

func (s *oneMsgStream) Close(context.Context) error { return s.rdr.Close() }

func TestDeleteOnFinishWhileWaitingForNextFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(file, []byte("hello"), 0o644))

	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	srv, err := pkgsftp.NewServer(struct {
		io.Reader
		io.WriteCloser
	}{sr, sw})
	require.NoError(t, err)
	go func() { _ = srv.Serve() }()
	client, err := pkgsftp.NewClientPipe(cr, cw)
	require.NoError(t, err)
	defer func() {
		_ = sw.Close()
		_ = cw.Close()
		_ = client.Close()
	}()

	s := &sftpReader{
		log:            service.MockResources().Logger(),
		client:         client,
		pathProvider:   &blockingPathProvider{path: file},
		scannerCtor:    oneMsgCodec{},
		deleteOnFinish: true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, s.Connect(ctx))
	_, ackFn, err := s.ReadBatch(ctx)
	require.NoError(t, err)
	_, _, err = s.ReadBatch(ctx)
	require.ErrorIs(t, err, service.ErrNotConnected)

	// The input now reconnects, which blocks waiting for a new file to appear.
	connDone := make(chan error, 1)
	go func() { connDone <- s.Connect(ctx) }()
	time.Sleep(100 * time.Millisecond)

	// The last file is acked while Connect is still waiting, it must be deleted.
	ackDone := make(chan error, 1)
	go func() { ackDone <- ackFn(ctx, nil) }()

	select {
	case err := <-ackDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("ack of the last file blocked while Connect waits for the next file")
	}
	_, statErr := os.Stat(file)
	assert.True(t, os.IsNotExist(statErr), "file should have been deleted")

	cancel()
	select {
	case <-connDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Connect did not return after cancellation")
	}
}
