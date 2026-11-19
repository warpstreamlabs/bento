package hdfs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeFile struct {
	writeErr, closeErr error
	closed             bool
}

func (f *fakeFile) Write(p []byte) (int, error) { return len(p), f.writeErr }

func (f *fakeFile) Close() error {
	f.closed = true
	return f.closeErr
}

func TestWriteAndClose(t *testing.T) {
	closeErr := errors.New("close failed")
	writeErr := errors.New("write failed")

	f := &fakeFile{}
	require.NoError(t, writeAndClose(f, []byte("x")))
	assert.True(t, f.closed)

	f = &fakeFile{closeErr: closeErr}
	assert.ErrorIs(t, writeAndClose(f, []byte("x")), closeErr)

	f = &fakeFile{writeErr: writeErr}
	assert.ErrorIs(t, writeAndClose(f, []byte("x")), writeErr)
	assert.True(t, f.closed)
}
