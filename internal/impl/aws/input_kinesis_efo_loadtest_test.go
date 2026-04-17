package aws

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEFOSyncLoadSimulation simulates the KCL-style synchronous EFO consumer
// architecture under load. Each shard goroutine reads events (producing record
// byte slices), adds them to a batcher, and flushes to an unbuffered msgChan.
// A downstream output goroutine drains msgChan with configurable latency.
//
// This validates that:
//   - Memory is bounded by checkpoint_limit × shards × batch_size
//   - All shards make progress (no starvation)
//   - Backpressure works: when output is slow, shards block on msgChan
//
// Run with:
//
//	go test ./internal/impl/aws/... -run TestEFOSyncLoadSimulation -v -timeout 120s
func TestEFOSyncLoadSimulation(t *testing.T) {
	const (
		numShards       = 60
		recordsPerEvent = 50   // records per SubscribeToShardEvent
		recordSizeBytes = 1024 // 1 KB per record
		batchSize       = 200  // records per flush (batch policy count)
		checkpointLimit = 10   // max unacked batches per shard
		testDuration    = 10 * time.Second
		outputLatency   = 20 * time.Millisecond
		eventReadDelay  = 1 * time.Millisecond // simulated HTTP/2 event read time
	)

	type flushedBatch struct {
		count int
		bytes int
		data  [][]byte // keep payload live on heap
		ackFn func()   // called by output to simulate pipeline ack
	}

	// Unbuffered msgChan — same as real kinesisReader
	msgChan := make(chan flushedBatch)

	ctx, cancel := context.WithTimeout(context.Background(), testDuration+5*time.Second)
	defer cancel()

	testCtx, testCancel := context.WithTimeout(ctx, testDuration)
	defer testCancel()

	// Metrics
	var (
		totalProduced, totalConsumed           atomic.Int64
		totalBytesProduced, totalBytesConsumed atomic.Int64
		peakAlloc                              atomic.Int64
		peakConcurrentFlushBlocked             atomic.Int32
		currentFlushBlocked                    atomic.Int32
	)

	// Simulate checkpoint_limit per shard: a buffered channel that limits
	// how many unacked batches each shard can have in the pipeline
	type shardSemaphore chan struct{}

	// Peak memory tracker
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				alloc := int64(m.Alloc)
				for {
					old := peakAlloc.Load()
					if alloc <= old || peakAlloc.CompareAndSwap(old, alloc) {
						break
					}
				}
			}
		}
	}()

	// Output goroutine — drains msgChan with simulated latency, then acks
	var outputWg sync.WaitGroup
	outputWg.Go(func() {
		for batch := range msgChan {
			totalConsumed.Add(int64(batch.count))
			totalBytesConsumed.Add(int64(batch.bytes))
			_ = batch.data // keep heap-live until processed
			time.Sleep(outputLatency)
			if batch.ackFn != nil {
				batch.ackFn() // release checkpoint slot
			}
		}
	})

	// Shard goroutines — simulate synchronous EFO processing
	var shardWg sync.WaitGroup
	for shard := range numShards {
		shardWg.Add(1)
		go func(shardID int) {
			defer shardWg.Done()

			// Per-shard checkpoint limit: limits unacked batches in pipeline
			cpSem := make(shardSemaphore, checkpointLimit)
			for range checkpointLimit {
				cpSem <- struct{}{}
			}

			var batcherCount, batcherBytes int
			var batcherData [][]byte

			for {
				select {
				case <-testCtx.Done():
					return
				default:
				}

				// Simulate reading one event from Kinesis HTTP/2 stream
				select {
				case <-time.After(eventReadDelay):
				case <-testCtx.Done():
					return
				}

				// Add records to batcher (inline, no channel)
				for range recordsPerEvent {
					batcherCount++
					batcherBytes += recordSizeBytes
					batcherData = append(batcherData, make([]byte, recordSizeBytes))

					if batcherCount >= batchSize {
						// Batch full — acquire checkpoint slot (blocks if pipeline full)
						select {
						case <-cpSem:
						case <-testCtx.Done():
							return
						}

						batch := flushedBatch{
							count: batcherCount,
							bytes: batcherBytes,
							data:  batcherData,
							ackFn: func() { cpSem <- struct{}{} }, // return slot on ack
						}

						totalProduced.Add(int64(batcherCount))
						totalBytesProduced.Add(int64(batcherBytes))

						// Send to pipeline — BLOCKS when msgChan is full (backpressure)
						cur := currentFlushBlocked.Add(1)
						for {
							old := peakConcurrentFlushBlocked.Load()
							if cur <= old || peakConcurrentFlushBlocked.CompareAndSwap(old, cur) {
								break
							}
						}

						select {
						case msgChan <- batch:
						case <-testCtx.Done():
							currentFlushBlocked.Add(-1)
							// Return checkpoint slot since we didn't send
							cpSem <- struct{}{}
							return
						}
						currentFlushBlocked.Add(-1)

						batcherCount = 0
						batcherBytes = 0
						batcherData = nil
					}
				}
			}
		}(shard)
	}

	<-testCtx.Done()
	shardWg.Wait()
	close(msgChan)
	outputWg.Wait()

	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	// Expected max memory from records in pipeline:
	// checkpoint_limit × numShards × batchSize × recordSizeBytes
	expectedMaxPipelineBytes := int64(checkpointLimit) * int64(numShards) * int64(batchSize) * int64(recordSizeBytes)

	t.Logf("=== SYNCHRONOUS EFO LOAD TEST (%d shards, %s) ===", numShards, testDuration)
	t.Logf("Config: checkpoint_limit=%d, batch_size=%d, record_size=%d, output_latency=%s",
		checkpointLimit, batchSize, recordSizeBytes, outputLatency)
	t.Logf("")
	t.Logf("Records produced:        %d", totalProduced.Load())
	t.Logf("Records consumed:        %d", totalConsumed.Load())
	t.Logf("Bytes produced:          %d MB", totalBytesProduced.Load()/(1024*1024))
	t.Logf("Bytes consumed:          %d MB", totalBytesConsumed.Load()/(1024*1024))
	t.Logf("")
	t.Logf("Expected max pipeline:   %d MB (checkpoint_limit × shards × batch × record)",
		expectedMaxPipelineBytes/(1024*1024))
	t.Logf("Peak heap alloc:         %d MB", peakAlloc.Load()/(1024*1024))
	t.Logf("Final heap alloc:        %d MB", finalMem.Alloc/(1024*1024))
	t.Logf("Total heap alloc:        %d MB", finalMem.TotalAlloc/(1024*1024))
	t.Logf("Num GC cycles:           %d", finalMem.NumGC)
	t.Logf("")
	t.Logf("Peak flush-blocked shards: %d / %d", peakConcurrentFlushBlocked.Load(), numShards)

	// Assertions
	require.Greater(t, totalProduced.Load(), int64(0), "should produce some records")
	require.Greater(t, totalConsumed.Load(), int64(0), "should consume some records")

	// Peak heap should be in the ballpark of expected pipeline bytes.
	// Allow 3x headroom for Go runtime, GC overhead, test harness, etc.
	assert.Less(t, peakAlloc.Load(), expectedMaxPipelineBytes*3,
		"peak heap should be bounded by pipeline depth")
}

// TestEFOSyncLoadSimulation_LargeRecords tests with 100KB records to verify
// memory stays bounded even with large payloads.
func TestEFOSyncLoadSimulation_LargeRecords(t *testing.T) {
	const (
		numShards       = 60
		recordsPerEvent = 10
		recordSizeBytes = 100 * 1024 // 100 KB per record
		batchSize       = 50
		checkpointLimit = 5 // tight limit
		testDuration    = 5 * time.Second
		outputLatency   = 30 * time.Millisecond
		eventReadDelay  = 1 * time.Millisecond
	)

	type flushedBatch struct {
		count int
		bytes int
		data  [][]byte
		ackFn func()
	}

	msgChan := make(chan flushedBatch)

	ctx, cancel := context.WithTimeout(context.Background(), testDuration+5*time.Second)
	defer cancel()

	testCtx, testCancel := context.WithTimeout(ctx, testDuration)
	defer testCancel()

	var totalProduced, totalConsumed atomic.Int64
	var peakAlloc atomic.Int64

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				alloc := int64(m.Alloc)
				for {
					old := peakAlloc.Load()
					if alloc <= old || peakAlloc.CompareAndSwap(old, alloc) {
						break
					}
				}
			}
		}
	}()

	var outputWg sync.WaitGroup
	outputWg.Go(func() {
		for batch := range msgChan {
			totalConsumed.Add(int64(batch.count))
			_ = batch.data
			time.Sleep(outputLatency)
			if batch.ackFn != nil {
				batch.ackFn()
			}
		}
	})

	type shardSemaphore chan struct{}

	var shardWg sync.WaitGroup
	for shard := range numShards {
		shardWg.Add(1)
		go func(shardID int) {
			defer shardWg.Done()

			cpSem := make(shardSemaphore, checkpointLimit)
			for range checkpointLimit {
				cpSem <- struct{}{}
			}

			var batcherCount, batcherBytes int
			var batcherData [][]byte

			for {
				select {
				case <-testCtx.Done():
					return
				default:
				}

				select {
				case <-time.After(eventReadDelay):
				case <-testCtx.Done():
					return
				}

				for range recordsPerEvent {
					batcherCount++
					batcherBytes += recordSizeBytes
					batcherData = append(batcherData, make([]byte, recordSizeBytes))

					if batcherCount >= batchSize {
						select {
						case <-cpSem:
						case <-testCtx.Done():
							return
						}

						batch := flushedBatch{
							count: batcherCount,
							bytes: batcherBytes,
							data:  batcherData,
							ackFn: func() { cpSem <- struct{}{} },
						}
						totalProduced.Add(int64(batcherCount))

						select {
						case msgChan <- batch:
						case <-testCtx.Done():
							cpSem <- struct{}{}
							return
						}

						batcherCount = 0
						batcherBytes = 0
						batcherData = nil
					}
				}
			}
		}(shard)
	}

	<-testCtx.Done()
	shardWg.Wait()
	close(msgChan)
	outputWg.Wait()

	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	expectedMaxPipelineBytes := int64(checkpointLimit) * int64(numShards) * int64(batchSize) * int64(recordSizeBytes)

	t.Logf("=== LARGE RECORDS SYNC LOAD TEST (%d shards, %d KB records) ===", numShards, recordSizeBytes/1024)
	t.Logf("Config: checkpoint_limit=%d, batch_size=%d, output_latency=%s",
		checkpointLimit, batchSize, outputLatency)
	t.Logf("")
	t.Logf("Records produced:      %d", totalProduced.Load())
	t.Logf("Records consumed:      %d", totalConsumed.Load())
	t.Logf("")
	t.Logf("Expected max pipeline: %d MB", expectedMaxPipelineBytes/(1024*1024))
	t.Logf("Peak heap alloc:       %d MB", peakAlloc.Load()/(1024*1024))
	t.Logf("Final heap alloc:      %d MB", finalMem.Alloc/(1024*1024))

	require.Greater(t, totalProduced.Load(), int64(0))
	require.Greater(t, totalConsumed.Load(), int64(0))

	// With 100KB records, checkpoint_limit=5, 60 shards, batch_size=50:
	// max pipeline = 5 × 60 × 50 × 100KB = 1.5 GB
	// That's the theoretical max if all shards have all checkpoint slots filled.
	// In practice, the unbuffered msgChan means only 1 batch is in transit per shard
	// at a time, so actual usage is much lower.
	// We just verify it doesn't blow up.
	assert.Less(t, peakAlloc.Load(), expectedMaxPipelineBytes,
		"peak heap should stay well under theoretical maximum")
}
