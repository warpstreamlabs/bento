package kubernetes

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestKubernetesWatchConfigParse(t *testing.T) {
	spec := kubernetesWatchInputConfig()

	tests := []struct {
		name        string
		config      string
		expectError bool
	}{
		{
			name: "watch pods",
			config: `
resource: pods
`,
			expectError: false,
		},
		{
			name: "watch deployments in namespace",
			config: `
resource: deployments
namespaces:
  - production
label_selector:
  app: backend
`,
			expectError: false,
		},
		{
			name: "watch custom resource",
			config: `
custom_resource:
  group: stable.example.com
  version: v1
  resource: crontabs
`,
			expectError: false,
		},
		{
			name: "filter event types",
			config: `
resource: pods
event_types:
  - ADDED
  - DELETED
include_initial_list: false
`,
			expectError: false,
		},
		{
			name: "all namespaces with resource",
			config: `
resource: services
namespaces: []
`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(tt.config, nil)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, conf)
			}
		})
	}
}

// newTestWatchInput creates a minimal kubernetesWatchInput for testing
// without requiring a real Kubernetes connection.
func newTestWatchInput() *kubernetesWatchInput {
	return &kubernetesWatchInput{
		eventChan:    make(chan watchEvent, 100),
		shutSig:      shutdown.NewSignaller(),
		resourceVers: make(map[string]string),
	}
}

func TestCloseTriggersEndOfInput(t *testing.T) {
	k := newTestWatchInput()

	// Start a goroutine that will call Read
	readDone := make(chan error, 1)
	go func() {
		ctx := context.Background()
		_, _, err := k.Read(ctx)
		readDone <- err
	}()

	// Give Read time to block on the channel
	time.Sleep(10 * time.Millisecond)

	// Close should trigger ErrEndOfInput
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := k.Close(ctx)
	require.NoError(t, err)

	// Verify Read returned ErrEndOfInput
	select {
	case readErr := <-readDone:
		assert.ErrorIs(t, readErr, service.ErrEndOfInput)
	case <-time.After(time.Second):
		t.Fatal("Read did not return after Close")
	}
}

func TestCloseDrainsEventsBeforeShutdown(t *testing.T) {
	k := newTestWatchInput()

	// Add some events to the channel before closing
	testEvents := []watchEvent{
		{eventType: "ADDED", object: &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"name":      "test-pod-1",
					"namespace": "default",
				},
			},
		}},
		{eventType: "MODIFIED", object: &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"name":      "test-pod-2",
					"namespace": "default",
				},
			},
		}},
	}

	for _, evt := range testEvents {
		k.eventChan <- evt
	}

	// Read the events before closing
	ctx := context.Background()
	receivedEvents := 0

	for i := 0; i < len(testEvents); i++ {
		msg, ack, err := k.Read(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.NotNil(t, ack)
		receivedEvents++
	}

	assert.Equal(t, len(testEvents), receivedEvents, "should receive all events before close")

	// Now close
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := k.Close(closeCtx)
	require.NoError(t, err)

	// Subsequent reads should return ErrEndOfInput
	_, _, err = k.Read(ctx)
	assert.ErrorIs(t, err, service.ErrEndOfInput)
}

func TestReadReturnsErrEndOfInputOnClosedChannel(t *testing.T) {
	k := newTestWatchInput()

	// Close the channel directly to simulate shutdown
	close(k.eventChan)

	ctx := context.Background()
	_, _, err := k.Read(ctx)

	assert.ErrorIs(t, err, service.ErrEndOfInput)
}

func TestReadRespectsContextCancellation(t *testing.T) {
	k := newTestWatchInput()

	ctx, cancel := context.WithCancel(context.Background())

	// Start Read in a goroutine
	readDone := make(chan error, 1)
	go func() {
		_, _, err := k.Read(ctx)
		readDone <- err
	}()

	// Give Read time to block
	time.Sleep(10 * time.Millisecond)

	// Cancel the context
	cancel()

	// Verify Read returned context.Canceled
	select {
	case readErr := <-readDone:
		assert.ErrorIs(t, readErr, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Read did not return after context cancellation")
	}

	// Clean up
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	_ = k.Close(closeCtx)
}

func TestConcurrentReadsAndClose(t *testing.T) {
	k := newTestWatchInput()

	// Start multiple readers
	const numReaders = 5
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, _, err := k.Read(ctx)
			errors <- err
		}()
	}

	// Give readers time to block
	time.Sleep(20 * time.Millisecond)

	// Close the input
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := k.Close(closeCtx)
	require.NoError(t, err)

	// Wait for all readers to finish
	wg.Wait()
	close(errors)

	// All readers should have received ErrEndOfInput
	for readErr := range errors {
		assert.ErrorIs(t, readErr, service.ErrEndOfInput)
	}
}
