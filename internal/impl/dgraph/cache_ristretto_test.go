package dgraph

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestRistrettoCache(t *testing.T) {
	c, err := newRistrettoCache(0, false, nil)
	require.NoError(t, err)

	ctx := context.Background()

	_, err = c.Get(ctx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	exists, err := c.Exists(ctx, "foo")
	assert.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, c.Set(ctx, "foo", []byte("1"), nil))

	var res []byte
	require.Eventually(t, func() bool {
		res, err = c.Get(ctx, "foo")
		return err == nil
	}, time.Millisecond*100, time.Millisecond)
	assert.Equal(t, []byte("1"), res)

	require.Eventually(t, func() bool {
		exists, err = c.Exists(ctx, "foo")
		return err == nil
	}, time.Millisecond*100, time.Millisecond)
	assert.True(t, exists)

	assert.NoError(t, c.Delete(ctx, "foo"))

	_, err = c.Get(ctx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	exists, err = c.Exists(ctx, "foo")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestRistrettoCacheWithTTL(t *testing.T) {
	c, err := newRistrettoCache(0, false, nil)
	require.NoError(t, err)

	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "foo", []byte("1"), nil))

	var res []byte
	require.Eventually(t, func() bool {
		res, err = c.Get(ctx, "foo")
		return err == nil
	}, time.Millisecond*100, time.Millisecond)
	assert.Equal(t, []byte("1"), res)

	var exists bool
	require.Eventually(t, func() bool {
		exists, err = c.Exists(ctx, "foo")
		return err == nil
	}, time.Millisecond*100, time.Millisecond)
	assert.True(t, exists)

	assert.NoError(t, c.Delete(ctx, "foo"))

	_, err = c.Get(ctx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	exists, err = c.Exists(ctx, "foo")
	assert.NoError(t, err)
	assert.False(t, exists)

	ttl := time.Millisecond * 200
	require.NoError(t, c.Set(ctx, "foo", []byte("1"), &ttl))

	assert.Eventually(t, func() bool {
		_, err = c.Get(ctx, "foo")
		return err == service.ErrKeyNotFound
	}, time.Second, time.Millisecond*5)

	require.NoError(t, c.Set(ctx, "foo", []byte("1"), &ttl))

	assert.Eventually(t, func() bool {
		exists, err = c.Exists(ctx, "foo")
		return err == nil && !exists
	}, time.Second, time.Millisecond*5)
}
