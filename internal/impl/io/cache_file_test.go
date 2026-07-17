package io

import (
	"context"
	"os"
	"path/filepath"
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

	require.NoError(t, c.Set(tCtx, "foo", []byte("1"), nil))

	act, err := c.Get(tCtx, "foo")
	require.NoError(t, err)
	assert.Equal(t, "1", string(act))

	require.NoError(t, c.Add(tCtx, "bar", []byte("2"), nil))

	act, err = c.Get(tCtx, "bar")
	require.NoError(t, err)
	assert.Equal(t, "2", string(act))

	assert.Equal(t, service.ErrKeyAlreadyExists, c.Add(tCtx, "foo", []byte("2"), nil))

	require.NoError(t, c.Set(tCtx, "foo", []byte("3"), nil))

	act, err = c.Get(tCtx, "foo")
	require.NoError(t, err)
	assert.Equal(t, "3", string(act))

	require.NoError(t, c.Delete(tCtx, "foo"))

	_, err = c.Get(tCtx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)
}

func TestFileCacheListKeys(t *testing.T) {
	dir := t.TempDir()

	tCtx := context.Background()
	c := newFileCache(dir, service.MockResources())

	var _ service.ListableCache = c

	require.NoError(t, c.Set(tCtx, "foo", []byte("1"), nil))
	require.NoError(t, c.Set(tCtx, "bar", []byte("2"), nil))

	nestedKey := filepath.Join("nested", "baz")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nested"), 0o755))
	require.NoError(t, c.Set(tCtx, nestedKey, []byte("3"), nil))

	keys, err := c.ListKeys(tCtx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"foo", "bar", nestedKey}, keys)

	require.NoError(t, c.Delete(tCtx, "foo"))

	keys, err = c.ListKeys(tCtx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"bar", nestedKey}, keys)
}

func TestFileCacheListKeysCancelled(t *testing.T) {
	dir := t.TempDir()

	c := newFileCache(dir, service.MockResources())
	require.NoError(t, c.Set(context.Background(), "foo", []byte("1"), nil))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.ListKeys(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
