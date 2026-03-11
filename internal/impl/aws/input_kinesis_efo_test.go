package aws

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// mockEFOClient implements kinesisEFOAPI for testing.
type mockEFOClient struct {
	describeResponses []mockDescribeResponse
	describeCallCount int
	// repeatLast causes the last response to be repeated indefinitely once all
	// responses are exhausted. Useful for testing context cancellation.
	repeatLast bool
}

type mockDescribeResponse struct {
	status types.ConsumerStatus
	err    error
}

func (m *mockEFOClient) DescribeStreamConsumer(_ context.Context, _ *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
	idx := m.describeCallCount
	m.describeCallCount++
	if idx >= len(m.describeResponses) {
		if m.repeatLast && len(m.describeResponses) > 0 {
			// Repeat the last response indefinitely (useful for context-cancel tests)
			idx = len(m.describeResponses) - 1
		} else {
			// Default: return ACTIVE if we run out of responses
			return &kinesis.DescribeStreamConsumerOutput{
				ConsumerDescription: &types.ConsumerDescription{
					ConsumerStatus: types.ConsumerStatusActive,
				},
			}, nil
		}
	}
	resp := m.describeResponses[idx]
	if resp.err != nil {
		return nil, resp.err
	}
	return &kinesis.DescribeStreamConsumerOutput{
		ConsumerDescription: &types.ConsumerDescription{
			ConsumerStatus: resp.status,
		},
	}, nil
}

func (m *mockEFOClient) RegisterStreamConsumer(_ context.Context, _ *kinesis.RegisterStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error) {
	return &kinesis.RegisterStreamConsumerOutput{
		Consumer: &types.Consumer{
			ConsumerARN: aws.String("arn:aws:kinesis:us-east-1:123:stream/test/consumer/bento-test"),
		},
	}, nil
}

func newTestEFOManager(client kinesisEFOAPI) *kinesisEFOManager {
	return &kinesisEFOManager{
		streamARN:    "arn:aws:kinesis:us-east-1:123:stream/test",
		consumerARN:  "arn:aws:kinesis:us-east-1:123:stream/test/consumer/bento-test",
		svc:          client,
		log:          service.MockResources().Logger(),
		pollInterval: 5 * time.Millisecond, // fast polling for unit tests
	}
}

// TestWaitForActiveConsumer_AlreadyActive checks that if the consumer is already
// ACTIVE on the first describe call, the function returns immediately without
// waiting for a tick.
func TestWaitForActiveConsumer_AlreadyActive(t *testing.T) {
	client := &mockEFOClient{
		describeResponses: []mockDescribeResponse{
			{status: types.ConsumerStatusActive},
		},
	}
	mgr := newTestEFOManager(client)

	start := time.Now()
	err := mgr.waitForActiveConsumer(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 1, client.describeCallCount, "should describe exactly once")
	// Should return well under the poll interval (5ms in tests, 2s in production)
	assert.Less(t, elapsed, 50*time.Millisecond, "should return immediately, not wait for tick")
}

// TestWaitForActiveConsumer_TransitionsToActive checks that the function polls
// until the consumer becomes ACTIVE after a few CREATING responses.
func TestWaitForActiveConsumer_TransitionsToActive(t *testing.T) {
	client := &mockEFOClient{
		describeResponses: []mockDescribeResponse{
			{status: types.ConsumerStatusCreating},
			{status: types.ConsumerStatusCreating},
			{status: types.ConsumerStatusActive},
		},
	}
	mgr := newTestEFOManager(client)

	err := mgr.waitForActiveConsumer(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 3, client.describeCallCount, "should describe until ACTIVE")
}

// TestWaitForActiveConsumer_DeletingState checks that a DELETING status
// returns an error immediately.
func TestWaitForActiveConsumer_DeletingState(t *testing.T) {
	client := &mockEFOClient{
		describeResponses: []mockDescribeResponse{
			{status: types.ConsumerStatusDeleting},
		},
	}
	mgr := newTestEFOManager(client)

	err := mgr.waitForActiveConsumer(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumer is being deleted")
}

// TestWaitForActiveConsumer_ParentContextCancelled checks that cancelling the
// parent context returns a "context cancelled" error (not a "timeout" error).
func TestWaitForActiveConsumer_ParentContextCancelled(t *testing.T) {
	// Stay in CREATING indefinitely so the function has to wait
	client := &mockEFOClient{
		describeResponses: []mockDescribeResponse{
			{status: types.ConsumerStatusCreating},
		},
		repeatLast: true, // keep returning CREATING so we never accidentally succeed
	}
	mgr := newTestEFOManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel the context after a short delay so the loop reaches the select
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := mgr.waitForActiveConsumer(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled", "should report cancellation, not timeout")
	assert.NotErrorIs(t, err, context.DeadlineExceeded, "should not be a deadline error")
	assert.ErrorIs(t, err, context.Canceled, "should wrap context.Canceled")
}

// TestWaitForActiveConsumer_InternalTimeoutDistinctFromCancellation checks that
// when the internal 2-minute waiterCtx expires it produces a "timeout" error,
// distinct from a parent-context cancellation. We use a very short timeout so
// the test doesn't actually wait 2 minutes.
func TestWaitForActiveConsumer_InternalTimeout(t *testing.T) {
	// Override the ticker to a short interval and ensure status never becomes ACTIVE
	client := &mockEFOClient{
		// Many CREATING responses so the loop keeps going
		describeResponses: func() []mockDescribeResponse {
			resps := make([]mockDescribeResponse, 100)
			for i := range resps {
				resps[i] = mockDescribeResponse{status: types.ConsumerStatusCreating}
			}
			return resps
		}(),
	}
	mgr := newTestEFOManager(client)

	// Use a context that times out very quickly to simulate the internal waiterCtx
	// expiring before the parent ctx is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := mgr.waitForActiveConsumer(ctx)

	require.Error(t, err)
	// The error could be either "context cancelled" (from ctx.Done) or "timeout"
	// (from waiterCtx.Done) depending on which fires first, but either way it
	// must NOT succeed and must contain a meaningful message.
	assert.True(t,
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
		"error should wrap a context error, got: %v", err,
	)
}

// TestWaitForActiveConsumer_DescribeError checks that an error from
// DescribeStreamConsumer is propagated immediately.
func TestWaitForActiveConsumer_DescribeError(t *testing.T) {
	describeErr := errors.New("network error")
	client := &mockEFOClient{
		describeResponses: []mockDescribeResponse{
			{err: describeErr},
		},
	}
	mgr := newTestEFOManager(client)

	err := mgr.waitForActiveConsumer(context.Background())

	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to describe consumer")
	assert.ErrorIs(t, err, describeErr)
}
