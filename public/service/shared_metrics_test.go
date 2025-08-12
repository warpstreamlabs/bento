package service_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestSharedMetricsBasic(t *testing.T) {
	sharedMetrics, err := service.NewSharedMetricsSetup(`none: {}`, 0)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, sharedMetrics.Shutdown(ctx))
}

func TestSharedMetricsWithStreamBuilder(t *testing.T) {
	sharedMetrics, err := service.NewSharedMetricsSetup(`none: {}`, 0)
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		_ = sharedMetrics.Shutdown(ctx)
	}()

	builder := service.NewStreamBuilder()

	// Test handler is nil before setting shared metrics
	require.Nil(t, builder.GetMetricsHandler())

	// Configure with shared metrics
	sharedMetrics.ConfigureStreamBuilder(builder)

	err = builder.SetYAML(`
input:
  generate:
    count: 2
    mapping: 'root = "test"'
output:
  drop: {}
logger:
  level: OFF
`)
	require.NoError(t, err)

	stream, err := builder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	err = stream.Run(ctx)
	require.NoError(t, err)
}

func TestSharedMetricsEmission(t *testing.T) {
	localMetrics := metrics.NewLocal()
	namespacedMetrics := metrics.NewNamespaced(localMetrics)

	// Create two streams with shared local metrics directly
	var streams []*service.Stream
	for i := 0; i < 2; i++ {
		builder := service.NewStreamBuilder()
		builder.SetStreamName(fmt.Sprintf("stream-%d", i))
		builder.SetSharedMetrics(namespacedMetrics)

		err := builder.SetYAML(`
input:
  generate:
    count: 2
    mapping: 'root = "test"'
output:
  drop: {}
logger:
  level: OFF
`)
		require.NoError(t, err)

		stream, err := builder.Build()
		require.NoError(t, err)
		streams = append(streams, stream)
	}

	// Run both streams
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	for _, stream := range streams {
		wg.Add(1)
		go func(s *service.Stream) {
			defer wg.Done()
			_ = s.Run(ctx)
		}(stream)
	}

	wg.Wait()
	time.Sleep(time.Millisecond * 100)

	// Verify metrics were emitted and aggregated
	counters := localMetrics.GetCounters()

	var totalReceived int64
	hasInputReceived := false
	for metricName, value := range counters {
		if strings.Contains(metricName, "input_received") {
			hasInputReceived = true
			totalReceived += value
		}
	}

	require.True(t, hasInputReceived, "Should have input_received metrics")
	require.Equal(t, int64(4), totalReceived, "Should have processed 4 messages total")

	// Verify timings exist
	timings := localMetrics.GetTimings()
	require.NotEmpty(t, timings, "Should have timing metrics")
}

func TestSharedMetricsExplicitNaming(t *testing.T) {
	localMetrics := metrics.NewLocal()
	namespacedMetrics := metrics.NewNamespaced(localMetrics)

	// Test multiple streams with explicit component names for distinct metrics
	streamConfigs := []struct {
		name  string
		count int
	}{
		{"orders", 2},
		{"users", 3},
	}

	var streams []*service.Stream
	for _, config := range streamConfigs {
		builder := service.NewStreamBuilder()
		builder.SetSharedMetrics(namespacedMetrics)

		yamlConfig := fmt.Sprintf(`
input:
  label: "%s_input"
  generate:
    count: %d
    mapping: 'root = {"source": "%s"}'
output:
  label: "%s_output"
  drop: {}
logger:
  level: OFF
`, config.name, config.count, config.name, config.name)

		err := builder.SetYAML(yamlConfig)
		require.NoError(t, err)

		stream, err := builder.Build()
		require.NoError(t, err)
		streams = append(streams, stream)
	}

	// Run all streams
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	for _, stream := range streams {
		wg.Add(1)
		go func(s *service.Stream) {
			defer wg.Done()
			_ = s.Run(ctx)
		}(stream)
	}

	wg.Wait()
	time.Sleep(time.Millisecond * 100)

	// Verify distinct metrics for each stream
	counters := localMetrics.GetCounters()
	streamMetrics := make(map[string]int64)

	for metricName, value := range counters {
		if strings.Contains(metricName, "input_received") {
			if strings.Contains(metricName, "orders_input") {
				streamMetrics["orders"] += value
			} else if strings.Contains(metricName, "users_input") {
				streamMetrics["users"] += value
			}
		}
	}

	require.Equal(t, int64(2), streamMetrics["orders"], "Orders stream should process 2 messages")
	require.Equal(t, int64(3), streamMetrics["users"], "Users stream should process 3 messages")

	totalMessages := streamMetrics["orders"] + streamMetrics["users"]
	require.Equal(t, int64(5), totalMessages, "Total should be 5 messages across streams")
}

func TestSharedMetricsWithStreamLabeling(t *testing.T) {
	localMetrics := metrics.NewLocal()
	namespacedMetrics := metrics.NewNamespaced(localMetrics)
	sharedSetup := &service.SharedMetricsSetup{}
	sharedSetup.SetMetricsForTesting(namespacedMetrics)

	// Create multiple streams with different stream names
	streamNames := []string{"orders-stream", "users-stream", "notifications-stream"}
	var streams []*service.Stream

	for _, streamName := range streamNames {
		builder := service.NewStreamBuilder()
		builder.SetStreamName(streamName)
		sharedSetup.ConfigureStreamBuilder(builder)

		yamlConfig := `
input:
  generate:
    count: 1
    mapping: 'root = {"stream": "` + streamName + `"}'
output:
  drop: {}
logger:
  level: OFF
`
		err := builder.SetYAML(yamlConfig)
		require.NoError(t, err)

		stream, err := builder.Build()
		require.NoError(t, err)
		streams = append(streams, stream)
	}

	// Run all streams
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	for _, stream := range streams {
		wg.Add(1)
		go func(s *service.Stream) {
			defer wg.Done()
			_ = s.Run(ctx)
		}(stream)
	}

	wg.Wait()
	time.Sleep(time.Millisecond * 100)

	// Verify metrics are properly labeled by stream
	counters := localMetrics.GetCounters()
	streamMetrics := make(map[string]int64)

	for metricName, value := range counters {
		if strings.Contains(metricName, "input_received") {
			for _, streamName := range streamNames {
				// Look for stream label in metric name like: input_received{stream="orders-stream",path="root.input"}
				expectedLabel := fmt.Sprintf(`stream="%s"`, streamName)
				if strings.Contains(metricName, expectedLabel) {
					streamMetrics[streamName] += value
				}
			}
		}
	}

	// Each stream should have processed exactly 1 message
	for _, streamName := range streamNames {
		require.Equal(t, int64(1), streamMetrics[streamName],
			fmt.Sprintf("Stream %s should have processed 1 message", streamName))
	}

	// Total should be 3 messages (1 per stream)
	totalMessages := int64(0)
	for _, count := range streamMetrics {
		totalMessages += count
	}
	require.Equal(t, int64(3), totalMessages, "Should have 3 messages total across all streams")
}

func TestStreamLabelingSimple(t *testing.T) {
	localMetrics := metrics.NewLocal()

	// Test basic labeling functionality
	baseMetrics := metrics.NewNamespaced(localMetrics)
	streamMetrics := baseMetrics.WithLabels("stream", "test-stream")

	// Emit a simple counter
	counter := streamMetrics.GetCounter("test_counter")
	counter.Incr(1)

	// Check what was emitted
	counters := localMetrics.GetCounters()

	// Should see counter with stream label
	found := false
	for metricName := range counters {
		if strings.Contains(metricName, "stream=") && strings.Contains(metricName, "test-stream") {
			found = true
			break
		}
	}
	require.True(t, found, "Should find metric with stream label")
}
