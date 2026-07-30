package ifs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOSAccess(t *testing.T) {
	var fs FS = TestFS{}

	require.False(t, IsOS(fs))

	fs = OS()

	require.True(t, IsOS(fs))
}
