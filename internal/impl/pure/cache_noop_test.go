package pure

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestNoopCacheStandard(t *testing.T) {
	t.Parallel()

	resources := service.MockResources()

	c := noopMemCache("TestNoopCacheStandard", resources.Logger())

	err := c.Set(context.Background(), "foo", []byte("bar"), nil)
	require.NoError(t, err)

	value, err := c.Get(context.Background(), "foo")
	require.EqualError(t, err, "key does not exist")

	exists, err := c.Exists(context.Background(), "foo")
	require.NoError(t, err)
	require.False(t, exists)

	assert.Nil(t, value)
}
