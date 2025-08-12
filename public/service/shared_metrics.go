package service

import (
	"context"
	"fmt"
	"os"

	"github.com/warpstreamlabs/bento/internal/api"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager"
)

// SharedMetricsSetup provides a way to create shared metrics that can be used
// across multiple StreamBuilders, allowing them to aggregate metrics to the
// same destination and expose a single HTTP endpoint.
//
// This solves the common problem when building multiple streams programmatically
// with StreamBuilder APIs: each stream creates its own metrics registry and
// HTTP server, leading to port conflicts. With SharedMetricsSetup, all streams
// can share the same metrics registry and expose metrics via a single endpoint.
//
// Example usage:
//
//	// Create shared metrics setup
//	sharedMetrics, err := service.NewSharedMetricsSetup(`
//	prometheus:
//	  use_histogram_timing: false
//	`, 9090)
//	if err != nil {
//	  log.Fatal(err)
//	}
//	defer sharedMetrics.Shutdown(context.Background())
//
//	// Start the shared metrics server
//	go sharedMetrics.ListenAndServe()
//
//	// Create multiple streams that share the same metrics with distinct labels
//	streamNames := []string{"orders-processor", "user-events", "notifications"}
//	for _, streamName := range streamNames {
//	  builder := service.NewStreamBuilder()
//	  builder.SetStreamName(streamName)  // This adds stream="orders-processor" labels
//	  sharedMetrics.ConfigureStreamBuilder(builder)
//	  // ... configure your stream normally
//	  stream, err := builder.Build()
//	  // ... run your stream
//	}
//
//	// All streams now report to the same Prometheus registry on port 9090
//	// Metrics are labeled with stream names for easy identification:
//	// - input_received{stream="orders-processor",...}
//	// - input_received{stream="user-events",...}
//	// - input_received{stream="notifications",...}
type SharedMetricsSetup struct {
	metrics     *metrics.Namespaced
	api         *api.Type
	httpAddress string
}

// NewSharedMetricsSetup creates a shared metrics setup from a YAML configuration.
// This allows multiple StreamBuilders to aggregate their metrics to the same
// destination (e.g., shared Prometheus registry) and expose them via a single
// HTTP server.
func NewSharedMetricsSetup(metricsYAML string, httpPort int) (*SharedMetricsSetup, error) {
	return NewSharedMetricsSetupWithEnvironment(metricsYAML, httpPort, GlobalEnvironment())
}

// NewSharedMetricsSetupWithEnvironment creates a shared metrics setup from a YAML
// configuration using a custom environment. This is useful for testing with
// custom metrics exporters.
func NewSharedMetricsSetupWithEnvironment(metricsYAML string, httpPort int, env *Environment) (*SharedMetricsSetup, error) {
	// Create a temporary manager for initializing metrics
	tmpMgr, err := manager.New(
		manager.NewResourceConfig(),
		manager.OptSetLogger(log.Noop()),
		manager.OptSetEnvironment(env.internal),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary manager: %w", err)
	}

	// Parse YAML string into yaml.Node
	yamlBytes := []byte(metricsYAML)
	if yamlBytes, err = config.ReplaceEnvVariables(yamlBytes, os.LookupEnv); err != nil {
		return nil, fmt.Errorf("failed to replace environment variables: %w", err)
	}

	yamlNode, err := docs.UnmarshalYAML(yamlBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Parse metrics configuration from YAML node
	metricsConf, err := metrics.FromAny(env.internal, yamlNode)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metrics YAML: %w", err)
	}

	// Initialize metrics
	stats, err := env.internal.MetricsInit(metricsConf, tmpMgr)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	// Create HTTP API server for metrics endpoint
	httpConf := api.NewConfig()
	httpAddress := fmt.Sprintf("0.0.0.0:%d", httpPort)
	httpConf.Address = httpAddress
	httpConf.Enabled = true

	apiServer, err := api.New("", "", httpConf, nil, log.Noop(), stats)
	if err != nil {
		return nil, fmt.Errorf("failed to create API server: %w", err)
	}

	return &SharedMetricsSetup{
		metrics:     stats,
		api:         apiServer,
		httpAddress: httpAddress,
	}, nil
}

// ConfigureStreamBuilder configures a StreamBuilder to use the shared metrics
// and disables its individual HTTP server to prevent port conflicts.
//
// If the StreamBuilder has a stream name set (via SetStreamName), all metrics
// will be labeled with "stream": streamName, similar to how Bento's streams
// mode works. This allows you to distinguish metrics from different streams
// in the shared registry. If no stream name is set, metrics will be emitted
// without stream labels (backward compatible).
//
// After calling this method, the StreamBuilder will:
//   - Send all its metrics to the shared metrics registry with optional stream labels
//   - Have its individual HTTP server disabled to avoid port conflicts
//   - Still function normally in all other respects
//
// Example with stream labeling:
//
//	builder.SetStreamName("orders-processor")
//	sharedMetrics.ConfigureStreamBuilder(builder)
//	// All metrics will have label "stream": "orders-processor"
//
// Example without stream labeling (backward compatible):
//
//	sharedMetrics.ConfigureStreamBuilder(builder)
//	// Metrics emitted without stream labels
func (s *SharedMetricsSetup) ConfigureStreamBuilder(builder *StreamBuilder) {
	sharedMetrics := s.metrics
	if builder.streamName != "" {
		sharedMetrics = s.metrics.WithLabels("stream", builder.streamName)
	}
	builder.SetSharedMetrics(sharedMetrics)
	// Disable HTTP server on individual streams since we have a shared one
	builder.http.Enabled = false
}

// ListenAndServe starts the HTTP server for the shared metrics endpoint.
// This is a blocking call that should typically be run in a goroutine.
//
// The server will expose metrics at the standard endpoints:
//   - /metrics - Prometheus format metrics
//   - /stats - Same metrics in the configured format
func (s *SharedMetricsSetup) ListenAndServe() error {
	return s.api.ListenAndServe()
}

// Shutdown gracefully shuts down the shared metrics HTTP server and closes
// the metrics registry. This should be called when you're done with the
// shared metrics setup to clean up resources.
func (s *SharedMetricsSetup) Shutdown(ctx context.Context) error {
	if s.api != nil {
		if err := s.api.Shutdown(ctx); err != nil {
			return err
		}
	}
	if s.metrics != nil {
		return s.metrics.Close()
	}
	return nil
}

// GetMetrics returns the shared metrics instance. This can be useful for
// advanced use cases where you need direct access to the metrics registry.
func (s *SharedMetricsSetup) GetMetrics() *metrics.Namespaced {
	return s.metrics
}

// GetHTTPAddress returns the address the HTTP server is configured to listen on.
// This is useful for logging or constructing full URLs to the metrics endpoint.
func (s *SharedMetricsSetup) GetHTTPAddress() string {
	return s.httpAddress
}

// SetMetricsForTesting is a helper method for testing that allows setting
// the metrics instance directly without going through the full setup process.
// This should only be used in tests.
func (s *SharedMetricsSetup) SetMetricsForTesting(metrics *metrics.Namespaced) {
	s.metrics = metrics
}
