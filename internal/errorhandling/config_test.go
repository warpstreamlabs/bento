package errorhandling_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/testutil"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func TestErrorHandlingConfigOK(t *testing.T) {
	cfg, err := testutil.ConfigFromYAML(`
error_handling:
  log:
    sampling_ratio: 0.5
`)
	require.NoError(t, err)
	require.Equal(t, "none", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.5, cfg.ErrorHandling.Log.SamplingRatio)
	require.False(t, cfg.ErrorHandling.Log.AddPayload)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  log:
    sampling_ratio: 0
`)
	require.NoError(t, err)
	require.Equal(t, "none", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.0, cfg.ErrorHandling.Log.SamplingRatio)
	require.False(t, cfg.ErrorHandling.Log.AddPayload)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  log:
    sampling_ratio: 1
`)
	require.NoError(t, err)
	require.Equal(t, "none", cfg.ErrorHandling.Strategy)
	require.Equal(t, 1.0, cfg.ErrorHandling.Log.SamplingRatio)
	require.False(t, cfg.ErrorHandling.Log.AddPayload)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: reject
`)
	require.NoError(t, err)
	require.Equal(t, "reject", cfg.ErrorHandling.Strategy)
	require.Equal(t, 1.0, cfg.ErrorHandling.Log.SamplingRatio)
	require.False(t, cfg.ErrorHandling.Log.AddPayload)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: reject
`)
	require.NoError(t, err)
	require.Equal(t, "reject", cfg.ErrorHandling.Strategy)
	require.Equal(t, 1.0, cfg.ErrorHandling.Log.SamplingRatio)
	require.False(t, cfg.ErrorHandling.Log.AddPayload)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: reject
  log:
    add_payload: true
`)
	require.NoError(t, err)
	require.Equal(t, "reject", cfg.ErrorHandling.Strategy)
	require.Equal(t, 1.0, cfg.ErrorHandling.Log.SamplingRatio)
	require.True(t, cfg.ErrorHandling.Log.AddPayload)
}

func TestErrorHandlingConfigError(t *testing.T) {
	t.Skip("Linting is not actioned from ConfigFromYAML. Explore alternative way of negative testing.")

	_, err := testutil.ConfigFromYAML(`
error_handling:
  log:
    sampling_ratio: -0.5
`)
	require.Error(t, err)

	_, err = testutil.ConfigFromYAML(`
error_handling:
  log:
    sampling_ratio: 1.1
`)
	require.Error(t, err)

	_, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: wrong
`)
	require.Error(t, err)
}
