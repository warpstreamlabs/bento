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
  error_sample_rate: 0.5
`)
	require.NoError(t, err)
	require.Equal(t, "", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.5, cfg.ErrorHandling.ErrorSampleRate)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  error_sample_rate: 0
`)
	require.NoError(t, err)
	require.Equal(t, "", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.0, cfg.ErrorHandling.ErrorSampleRate)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  error_sample_rate: 1
`)
	require.NoError(t, err)
	require.Equal(t, "", cfg.ErrorHandling.Strategy)
	require.Equal(t, 1.0, cfg.ErrorHandling.ErrorSampleRate)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: backoff
`)
	require.NoError(t, err)
	require.Equal(t, "backoff", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.0, cfg.ErrorHandling.ErrorSampleRate)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: backoff
`)
	require.NoError(t, err)
	require.Equal(t, "backoff", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.0, cfg.ErrorHandling.ErrorSampleRate)
	require.Equal(t, 0, cfg.ErrorHandling.MaxRetries)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: backoff
  max_retries: 10
`)
	require.NoError(t, err)
	require.Equal(t, "backoff", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.0, cfg.ErrorHandling.ErrorSampleRate)
	require.Equal(t, 10, cfg.ErrorHandling.MaxRetries)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: backoff
  max_retries: 10
  error_sample_rate: 0.8
`)
	require.NoError(t, err)
	require.Equal(t, "backoff", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.8, cfg.ErrorHandling.ErrorSampleRate)
	require.Equal(t, 10, cfg.ErrorHandling.MaxRetries)

	cfg, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: reject
`)
	require.NoError(t, err)
	require.Equal(t, "reject", cfg.ErrorHandling.Strategy)
	require.Equal(t, 0.0, cfg.ErrorHandling.ErrorSampleRate)
}

func TestErrorHandlingConfigError(t *testing.T) {
	t.Skip("Linting is not actioned from ConfigFromYAML. Explore alternative way of negative testing.")

	_, err := testutil.ConfigFromYAML(`
error_handling:
  error_sample_rate: -0.5
`)
	require.Error(t, err)

	_, err = testutil.ConfigFromYAML(`
error_handling:
  error_sample_rate: 1.1
`)
	require.Error(t, err)

	_, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: wrong
`)
	require.Error(t, err)

	_, err = testutil.ConfigFromYAML(`
error_handling:
  strategy: reject
  max_retries: -1
`)
	require.Error(t, err)

}
