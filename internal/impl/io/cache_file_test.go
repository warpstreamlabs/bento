package io

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestFileCache(t *testing.T) {
	dir, err := os.MkdirTemp("", "bento_file_cache_test")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	tCtx := context.Background()
	c := newFileCache(dir, service.MockResources())

	_, err = c.Get(tCtx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	exists, err := c.Exists(tCtx, "foo")
	assert.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, c.Set(tCtx, "foo", []byte("1"), nil))

	act, err := c.Get(tCtx, "foo")
	require.NoError(t, err)
	assert.Equal(t, "1", string(act))

	exists, err = c.Exists(tCtx, "foo")
	assert.NoError(t, err)
	assert.True(t, exists)

	require.NoError(t, c.Add(tCtx, "bar", []byte("2"), nil))

	act, err = c.Get(tCtx, "bar")
	require.NoError(t, err)
	assert.Equal(t, "2", string(act))

	exists, err = c.Exists(tCtx, "foo")
	assert.NoError(t, err)
	assert.True(t, exists)

	assert.Equal(t, service.ErrKeyAlreadyExists, c.Add(tCtx, "foo", []byte("2"), nil))

	require.NoError(t, c.Set(tCtx, "foo", []byte("3"), nil))

	act, err = c.Get(tCtx, "foo")
	require.NoError(t, err)
	assert.Equal(t, "3", string(act))

	exists, err = c.Exists(tCtx, "foo")
	assert.NoError(t, err)
	assert.True(t, exists)

	require.NoError(t, c.Delete(tCtx, "foo"))

	_, err = c.Get(tCtx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	exists, err = c.Exists(tCtx, "foo")
	assert.NoError(t, err)
	assert.False(t, exists)
}
