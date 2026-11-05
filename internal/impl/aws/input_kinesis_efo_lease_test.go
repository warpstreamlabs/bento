package aws

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// newLeaseTestReader builds a kinesisReader wired for lease tests: an unbuffered
// msgChan like production, a short commit period, and a stubbed checkpoint call
// so no DynamoDB client is needed.
func newLeaseTestReader(commitPeriod time.Duration, checkpointFn func(ctx context.Context, streamID, shardID, sequence string) (bool, error)) *kinesisReader {
	mgr := service.MockResources()
	k := &kinesisReader{
		log:           mgr.Logger(),
		mgr:           mgr,
		batcher:       service.BatchPolicy{Count: 1},
		conf:          kiConfig{CheckpointLimit: 1024},
		commitPeriod:  commitPeriod,
		msgChan:       make(chan asyncMessage),
		runningShards: map[string]struct{}{},
		checkpointFn:  checkpointFn,
	}
	k.ctx, k.done = context.WithCancel(context.Background())
	return k
}

func newLeaseTestBatcher(t *testing.T, k *kinesisReader) *awsKinesisRecordBatcher {
	t.Helper()
	batcher, err := k.newAWSKinesisRecordBatcher(streamInfo{id: "test-stream"}, "shard-0", "seq-0")
	require.NoError(t, err)
	return batcher
}

// TestSendToPipelineRenewsLeaseWhileBlocked is the regression test for the OOM
// loop: while a consumer waits for a busy pipeline it must keep renewing its
// lease, otherwise the balancer treats the shard as unowned and starts a second
// consumer for it.
func TestSendToPipelineRenewsLeaseWhileBlocked(t *testing.T) {
	const commitPeriod = 20 * time.Millisecond

	var checkpoints atomic.Int32
	k := newLeaseTestReader(commitPeriod, func(context.Context, string, string, string) (bool, error) {
		checkpoints.Add(1)
		return true, nil // lease still ours
	})
	defer k.done()

	batcher := newLeaseTestBatcher(t, k)
	info := streamInfo{id: "test-stream"}
	state := awsKinesisConsumerConsuming
	commitCtx, commitCtxClose := context.WithTimeout(k.ctx, commitPeriod)
	defer commitCtxClose()

	pendingMsg := asyncMessage{msg: service.MessageBatch{service.NewMessage([]byte("hello"))}}

	sendDone := make(chan bool, 1)
	go func() {
		sendDone <- k.sendToPipeline(k.ctx, info, "shard-0", batcher, &pendingMsg, &state, &commitCtx, &commitCtxClose)
	}()

	// Hold the pipeline busy for several commit periods without reading.
	time.Sleep(6 * commitPeriod)

	renewalsWhileBlocked := checkpoints.Load()
	assert.GreaterOrEqual(t, renewalsWhileBlocked, int32(3),
		"lease must be renewed repeatedly while blocked on the pipeline; got %d renewals", renewalsWhileBlocked)

	// The send is still outstanding — backpressure is preserved.
	select {
	case <-sendDone:
		t.Fatal("sendToPipeline returned before the pipeline accepted the message")
	default:
	}

	// Now drain, and the send should complete.
	select {
	case got := <-k.msgChan:
		assert.Len(t, got.msg, 1)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the message to reach the pipeline")
	}

	select {
	case sent := <-sendDone:
		assert.True(t, sent, "send should report success once the pipeline accepts")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sendToPipeline to return")
	}

	assert.Equal(t, asyncMessage{}, pendingMsg, "pendingMsg should be cleared once sent")
}

// TestSendToPipelineStopsWhenLeaseLost checks that losing the lease while
// blocked ends the send rather than waiting forever, and marks the consumer as
// yielding so the shard is handed over cleanly.
func TestSendToPipelineStopsWhenLeaseLost(t *testing.T) {
	const commitPeriod = 20 * time.Millisecond

	k := newLeaseTestReader(commitPeriod, func(context.Context, string, string, string) (bool, error) {
		return false, nil // someone else owns the lease now
	})
	defer k.done()

	batcher := newLeaseTestBatcher(t, k)
	info := streamInfo{id: "test-stream"}
	state := awsKinesisConsumerConsuming
	commitCtx, commitCtxClose := context.WithTimeout(k.ctx, commitPeriod)
	defer commitCtxClose()

	pendingMsg := asyncMessage{msg: service.MessageBatch{service.NewMessage([]byte("hello"))}}

	sendDone := make(chan bool, 1)
	go func() {
		sendDone <- k.sendToPipeline(k.ctx, info, "shard-0", batcher, &pendingMsg, &state, &commitCtx, &commitCtxClose)
	}()

	// Never read from msgChan: only the lost lease can end the send.
	select {
	case sent := <-sendDone:
		assert.False(t, sent, "send should report failure when the lease is lost")
	case <-time.After(2 * time.Second):
		t.Fatal("sendToPipeline did not give up after losing the lease")
	}

	assert.Equal(t, awsKinesisConsumerYielding, state, "consumer should be marked as yielding")

	// The batch is dropped rather than retained. Holding it would leave the
	// consumer's exit drain blocked on the same full pipeline that cost us the
	// lease, so the goroutine would never exit and its deferred cleanup would
	// never yield the checkpoint or deregister the shard. Dropping is safe
	// because the yielded checkpoint uses the acked sequence, which sits before
	// anything unsent.
	assert.Nil(t, pendingMsg.msg, "unsent batch should be dropped when the lease is lost")
}

// TestConsumerExitDrainDoesNotBlockAfterLeaseLoss is the reason the batch is
// dropped above: it reproduces the exit path a consumer takes after losing its
// lease, with the pipeline still full, and checks it can finish.
func TestConsumerExitDrainDoesNotBlockAfterLeaseLoss(t *testing.T) {
	const commitPeriod = 20 * time.Millisecond

	k := newLeaseTestReader(commitPeriod, func(context.Context, string, string, string) (bool, error) {
		return false, nil // lease gone
	})
	defer k.done()

	batcher := newLeaseTestBatcher(t, k)
	info := streamInfo{id: "test-stream"}
	state := awsKinesisConsumerConsuming
	commitCtx, commitCtxClose := context.WithTimeout(k.ctx, commitPeriod)
	defer commitCtxClose()

	pendingMsg := asyncMessage{msg: service.MessageBatch{service.NewMessage([]byte("hello"))}}

	// Lose the lease while blocked on a pipeline nobody is draining.
	require.False(t, k.sendToPipeline(k.ctx, info, "shard-0", batcher, &pendingMsg, &state, &commitCtx, &commitCtxClose))
	require.Equal(t, awsKinesisConsumerYielding, state)

	// This mirrors the tail drain in runEFOConsumer. With the batch dropped it
	// is a no-op; if the batch were retained it would block until shutdown and
	// the consumer's deferred cleanup would never run.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		if pendingMsg.msg != nil {
			select {
			case k.msgChan <- pendingMsg:
			case <-k.ctx.Done():
			}
		}
	}()

	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("consumer exit drain blocked after lease loss; the unsent batch was not dropped")
	}
}

// TestSendToPipelineStopsOnShutdown checks the shutdown path still works.
func TestSendToPipelineStopsOnShutdown(t *testing.T) {
	const commitPeriod = time.Hour // never fires; shutdown is the only exit

	k := newLeaseTestReader(commitPeriod, func(context.Context, string, string, string) (bool, error) {
		return true, nil
	})

	batcher := newLeaseTestBatcher(t, k)
	info := streamInfo{id: "test-stream"}
	state := awsKinesisConsumerConsuming
	commitCtx, commitCtxClose := context.WithTimeout(k.ctx, commitPeriod)
	defer commitCtxClose()

	pendingMsg := asyncMessage{msg: service.MessageBatch{service.NewMessage([]byte("hello"))}}

	sendDone := make(chan bool, 1)
	go func() {
		sendDone <- k.sendToPipeline(k.ctx, info, "shard-0", batcher, &pendingMsg, &state, &commitCtx, &commitCtxClose)
	}()

	time.Sleep(50 * time.Millisecond)
	k.done() // shut down

	select {
	case sent := <-sendDone:
		assert.False(t, sent, "send should report failure on shutdown")
	case <-time.After(2 * time.Second):
		t.Fatal("sendToPipeline did not return on shutdown")
	}
}

// TestRunningShardRegistry covers the bookkeeping the balancer relies on.
func TestRunningShardRegistry(t *testing.T) {
	k := newLeaseTestReader(time.Second, nil)
	defer k.done()

	assert.False(t, k.isShardRunning("stream-a", "shard-0"))

	require.True(t, k.claimRunningShard("stream-a", "shard-0"), "first claim should succeed")
	assert.True(t, k.isShardRunning("stream-a", "shard-0"))

	assert.False(t, k.claimRunningShard("stream-a", "shard-0"), "second claim for the same shard should fail")

	// Shards are namespaced per stream, and per shard within a stream.
	assert.True(t, k.claimRunningShard("stream-b", "shard-0"), "same shard ID on another stream is independent")
	assert.True(t, k.claimRunningShard("stream-a", "shard-1"), "another shard on the same stream is independent")

	k.releaseRunningShard("stream-a", "shard-0")
	assert.False(t, k.isShardRunning("stream-a", "shard-0"))
	assert.True(t, k.claimRunningShard("stream-a", "shard-0"), "claim should succeed again after release")

	// The other entries are untouched by that release.
	assert.True(t, k.isShardRunning("stream-b", "shard-0"))
	assert.True(t, k.isShardRunning("stream-a", "shard-1"))
}

// TestRunningShardRegistryConcurrent checks the registry admits exactly one
// consumer per shard when the balancer races with itself.
func TestRunningShardRegistryConcurrent(t *testing.T) {
	k := newLeaseTestReader(time.Second, nil)
	defer k.done()

	const attempts = 50
	var granted atomic.Int32
	var wg sync.WaitGroup
	for range attempts {
		wg.Go(func() {
			if k.claimRunningShard("stream-a", "shard-0") {
				granted.Add(1)
			}
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), granted.Load(), "exactly one concurrent claim should be granted")
}

// TestStartConsumerSkipsAlreadyRunningShard is the regression test for the
// self-stealing balancer: a shard whose lease looks stale but which this client
// is already consuming must not get a second consumer.
func TestStartConsumerSkipsAlreadyRunningShard(t *testing.T) {
	k := newLeaseTestReader(time.Second, nil)
	defer k.done()

	info := streamInfo{id: "test-stream"}
	require.True(t, k.claimRunningShard(info.id, "shard-0"), "precondition: shard registered as running")

	var wg sync.WaitGroup
	wg.Add(1)

	// startConsumer owns the wait group increment, so this must not block.
	err := k.startConsumer(&wg, info, "shard-0", "seq-0")
	require.NoError(t, err, "skipping an already-running shard is not an error")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("startConsumer did not release the wait group when skipping the shard")
	}

	assert.True(t, k.isShardRunning(info.id, "shard-0"), "the original consumer's registration must survive")
}
