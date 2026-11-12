package sftp

import (
	"bytes"
	"context"
	"errors"
	"testing"

	pkgsftp "github.com/pkg/sftp"
	"github.com/warpstreamlabs/bento/public/service"
)

type discardSink struct{}

func (*discardSink) Close() error                { return nil }
func (*discardSink) Write(p []byte) (int, error) { return len(p), nil }

func TestOutputDropsClientOnConnectionLost(t *testing.T) {
	c, err := pkgsftp.NewClientPipe(bytes.NewReader([]byte{0, 0, 0, 5, 2, 0, 0, 0, 3}), &discardSink{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.Wait()
	path, err := service.NewInterpolatedString("folder/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	w := &sftpWriter{client: c, path: path}
	err = w.Write(context.Background(), service.NewMessage([]byte("message")))
	if !errors.Is(err, service.ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
	if w.client != nil {
		t.Fatal("dead client retained: subsequent Connect returns nil without establishing a connection")
	}
}
