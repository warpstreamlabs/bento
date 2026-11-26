package sftp

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	pkgsftp "github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

type failOnCloseFile struct{}

func (failOnCloseFile) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }
func (failOnCloseFile) Close() error                           { return errors.New("quota exceeded") }

type failOnCloseWriter struct{}

func (failOnCloseWriter) Filewrite(*pkgsftp.Request) (io.WriterAt, error) {
	return failOnCloseFile{}, nil
}

func TestOutputReturnsCloseError(t *testing.T) {
	clientConn, serverConn := net.Pipe()

	handlers := pkgsftp.InMemHandler()
	handlers.FilePut = failOnCloseWriter{}
	server := pkgsftp.NewRequestServer(serverConn, handlers)
	go func() { _ = server.Serve() }()
	t.Cleanup(func() { _ = server.Close() })

	client, err := pkgsftp.NewClientPipe(clientConn, clientConn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	path, err := service.NewInterpolatedString("file.txt")
	require.NoError(t, err)
	suffixFn, appendMode, err := codecGetWriter("all-bytes")
	require.NoError(t, err)

	w := &sftpWriter{client: client, path: path, suffixFn: suffixFn, appendMode: appendMode}
	err = w.Write(context.Background(), service.NewMessage([]byte("message")))
	assert.ErrorContains(t, err, "quota exceeded")
}
