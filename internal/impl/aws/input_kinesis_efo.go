package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/warpstreamlabs/bento/public/service"
)

// errBackpressureTimeout is returned when WaitForSpace times out due to sustained backpressure.
// This is a retryable error that should trigger a backoff before resubscribing.
var errBackpressureTimeout = errors.New("backpressure timeout waiting for space in pending pool")

// kinesisEFOManager handles Enhanced Fan Out consumer registration and lifecycle
type kinesisEFOManager struct {
	streamARN    string
	consumerName string
	consumerARN  string
	svc          *kinesis.Client
	log          *service.Logger
}

// newKinesisEFOManager creates a new EFO manager
func newKinesisEFOManager(conf *kiEFOConfig, streamARN, clientID string, svc *kinesis.Client, log *service.Logger) (*kinesisEFOManager, error) {
	if conf == nil {
		return nil, errors.New("enhanced fan out config is nil")
	}

	if conf.ConsumerName != "" && conf.ConsumerARN != "" {
		return nil, errors.New("cannot specify both consumer_name and consumer_arn")
	}

	consumerName := conf.ConsumerName
	if consumerName == "" && conf.ConsumerARN == "" {
		consumerName = "bento-" + clientID
	}

	return &kinesisEFOManager{
		streamARN:    streamARN,
		consumerName: consumerName,
		consumerARN:  conf.ConsumerARN,
		svc:          svc,
		log:          log,
	}, nil
}

// ensureConsumerRegistered registers the consumer if needed and returns the consumer ARN
func (m *kinesisEFOManager) ensureConsumerRegistered(ctx context.Context) (string, error) {
	if m.consumerARN != "" {
		m.log.Debugf("Using provided consumer ARN: %s", m.consumerARN)
		return m.consumerARN, nil
	}

	m.log.Debugf("Registering Enhanced Fan Out consumer: %s for stream: %s", m.consumerName, m.streamARN)

	registerInput := &kinesis.RegisterStreamConsumerInput{
		StreamARN:    aws.String(m.streamARN),
		ConsumerName: aws.String(m.consumerName),
	}

	output, err := m.svc.RegisterStreamConsumer(ctx, registerInput)
	if err != nil {
		var resourceInUse *types.ResourceInUseException
		if errors.As(err, &resourceInUse) {
			m.log.Debugf("Consumer %s already exists, describing to get ARN", m.consumerName)
			return m.describeAndWaitForActive(ctx)
		}
		return "", fmt.Errorf("failed to register consumer: %w", err)
	}

	if output.Consumer == nil || output.Consumer.ConsumerARN == nil {
		return "", errors.New("RegisterStreamConsumer succeeded but returned no consumer ARN")
	}

	m.consumerARN = *output.Consumer.ConsumerARN
	m.log.Debugf("Registered consumer with ARN: %s, waiting for ACTIVE status", m.consumerARN)

	if err := m.waitForActiveConsumer(ctx); err != nil {
		return "", fmt.Errorf("failed waiting for consumer to become active: %w", err)
	}

	return m.consumerARN, nil
}

// describeAndWaitForActive describes an existing consumer and waits for it to be active
func (m *kinesisEFOManager) describeAndWaitForActive(ctx context.Context) (string, error) {
	describeInput := &kinesis.DescribeStreamConsumerInput{
		StreamARN:    aws.String(m.streamARN),
		ConsumerName: aws.String(m.consumerName),
	}

	output, err := m.svc.DescribeStreamConsumer(ctx, describeInput)
	if err != nil {
		return "", fmt.Errorf("failed to describe consumer: %w", err)
	}

	if output.ConsumerDescription == nil || output.ConsumerDescription.ConsumerARN == nil {
		return "", errors.New("consumer description missing ARN")
	}

	m.consumerARN = *output.ConsumerDescription.ConsumerARN
	m.log.Debugf("Found existing consumer with ARN: %s", m.consumerARN)

	if output.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
		m.log.Debugf("Consumer is already ACTIVE")
		return m.consumerARN, nil
	}

	if err := m.waitForActiveConsumer(ctx); err != nil {
		return "", fmt.Errorf("failed waiting for consumer to become active: %w", err)
	}

	return m.consumerARN, nil
}

// waitForActiveConsumer waits for the consumer to reach ACTIVE status
func (m *kinesisEFOManager) waitForActiveConsumer(ctx context.Context) error {
	waiterCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-waiterCtx.Done():
			return fmt.Errorf("timeout waiting for consumer to become ACTIVE: %w", waiterCtx.Err())
		case <-ticker.C:
			describeInput := &kinesis.DescribeStreamConsumerInput{
				ConsumerARN: aws.String(m.consumerARN),
			}

			output, err := m.svc.DescribeStreamConsumer(waiterCtx, describeInput)
			if err != nil {
				return fmt.Errorf("failed to describe consumer: %w", err)
			}

			if output.ConsumerDescription != nil {
				status := output.ConsumerDescription.ConsumerStatus
				m.log.Debugf("Consumer status: %s", status)

				if status == types.ConsumerStatusActive {
					m.log.Debugf("Consumer is now ACTIVE")
					return nil
				}

				if status == types.ConsumerStatusDeleting {
					return errors.New("consumer is being deleted")
				}
			}
		}
	}
}

// runEFOConsumer consumes from a shard using Enhanced Fan Out
func (k *kinesisReader) runEFOConsumer(wg *sync.WaitGroup, info streamInfo, shardID, startingSequence string) error {
	// Create record batcher (same as polling mode)
	var recordBatcher *awsKinesisRecordBatcher
	var err error
	if recordBatcher, err = k.newAWSKinesisRecordBatcher(info, shardID, startingSequence); err != nil {
		wg.Done()
		if _, checkErr := k.checkpointer.Checkpoint(context.Background(), info.id, shardID, startingSequence, true); checkErr != nil {
			k.log.Errorf("Failed to gracefully yield checkpoint: %v\n", checkErr)
		}
		return err
	}

	// Track consumer state
	state := awsKinesisConsumerConsuming
	var pendingMsg asyncMessage

	// Buffer for pending records from the subscription
	var pending []types.Record

	// Channels for subscription control
	subscriptionTrigger := make(chan string, 1) // Trigger for initial subscription or resubscription
	subscriptionTrigger <- startingSequence     // Start with initial sequence

	// Channels for timed batches and message flush
	var nextTimedBatchChan <-chan time.Time
	var nextFlushChan chan<- asyncMessage
	var nextRecordsChan <-chan []types.Record
	commitCtx, commitCtxClose := context.WithTimeout(k.ctx, k.commitPeriod)

	go func() {
		defer func() {
			commitCtxClose()
			recordBatcher.Close(context.Background(), state == awsKinesisConsumerFinished)

			// Release any remaining pending records back to the global pool
			if len(pending) > 0 {
				k.globalPendingPool.Release(len(pending))
			}

			reason := ""
			switch state {
			case awsKinesisConsumerFinished:
				reason = " because the shard is closed"
				if err := k.checkpointer.Delete(k.ctx, info.id, shardID); err != nil {
					k.log.Errorf("Failed to remove checkpoint for finished stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			case awsKinesisConsumerYielding:
				reason = " because the shard has been claimed by another client"
				if err := k.checkpointer.Yield(k.ctx, info.id, shardID, recordBatcher.GetSequence()); err != nil {
					k.log.Errorf("Failed to yield checkpoint for stolen stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			case awsKinesisConsumerClosing:
				reason = " because the pipeline is shutting down"
				if _, err := k.checkpointer.Checkpoint(context.Background(), info.id, shardID, recordBatcher.GetSequence(), true); err != nil {
					k.log.Errorf("Failed to store final checkpoint for stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			}

			wg.Done()
			k.log.Debugf("Closing stream '%v' shard '%v' as client '%v'%v", info.id, shardID, k.checkpointer.clientID, reason)
		}()

		k.log.Debugf("Consuming stream '%v' shard '%v' with Enhanced Fan Out as client '%v'", info.id, shardID, k.checkpointer.clientID)

		// Start subscription in a separate goroutine
		bufferCap := 0
		if k.conf.EnhancedFanOut != nil {
			bufferCap = k.conf.EnhancedFanOut.RecordBufferCap
		}
		recordsChan := make(chan []types.Record, bufferCap)
		// errorsChan is used for logging/monitoring only - subscription goroutine
		// handles its own retries. Non-blocking sends handle overflow gracefully.
		errorsChan := make(chan error, 1)
		resubscribeChan := make(chan string, 1)
		shardFinishedChan := make(chan struct{}, 1)

		// drainRecordsChan drains any remaining records from recordsChan after
		// the subscription goroutine has stopped, releasing their pool capacity.
		// This prevents leaking pool capacity when the consumer exits with buffered records.
		drainRecordsChan := func() {
			for {
				select {
				case records := <-recordsChan:
					k.globalPendingPool.Release(len(records))
				default:
					return
				}
			}
		}

		var subscriptionWg sync.WaitGroup
		subscriptionWg.Add(1)
		go func() {
			defer subscriptionWg.Done()

			// Subscription goroutine manages its own backoff for retries
			subBoff := backoff.NewExponentialBackOff()
			subBoff.InitialInterval = 300 * time.Millisecond
			subBoff.MaxInterval = 5 * time.Second
			subBoff.MaxElapsedTime = 0 // Never stop retrying

			for sequence := range subscriptionTrigger {
				currentSeq := sequence

				// Inner retry loop - keeps trying until success or context cancellation
				for {
					select {
					case <-k.ctx.Done():
						return
					default:
					}

					continuationSeq, shardFinished, err := k.efoSubscribeAndStream(k.ctx, info, shardID, currentSeq, recordsChan)

					if err != nil {
						// Check for non-retryable errors - these should stop the subscription
						var resourceNotFound *types.ResourceNotFoundException
						var invalidArg *types.InvalidArgumentException
						if errors.As(err, &resourceNotFound) || errors.As(err, &invalidArg) {
							// Send to errorsChan for main loop to handle shutdown.
							// Use blocking send (with context) to ensure fatal errors are not dropped.
							k.log.Errorf("Non-retryable EFO error for shard %v: %v", shardID, err)
							select {
							case <-k.ctx.Done():
							case errorsChan <- err:
							}
							return // Stop retrying
						}

						// Log retryable error (non-blocking)
						select {
						case errorsChan <- err:
						default:
							// Channel full, just log locally
							if errors.Is(err, errBackpressureTimeout) {
								k.log.Debugf("EFO backpressure timeout for shard %v, will retry", shardID)
							} else {
								k.log.Warnf("EFO subscription error for shard %v, will retry: %v", shardID, err)
							}
						}

						// Update sequence for retry if we got a continuation
						if continuationSeq != "" {
							currentSeq = continuationSeq
						}

						// Backoff before retry
						backoffDuration := subBoff.NextBackOff()
						select {
						case <-k.ctx.Done():
							return
						case <-time.After(backoffDuration):
						}
						continue // Retry the subscription
					}

					// Success - reset backoff
					subBoff.Reset()

					if shardFinished {
						// Shard is closed, signal to main loop
						select {
						case shardFinishedChan <- struct{}{}:
						default:
						}
						return
					}

					// Subscription completed normally, update sequence and notify main loop
					if continuationSeq != "" {
						currentSeq = continuationSeq
					}
					select {
					case <-k.ctx.Done():
						return
					case resubscribeChan <- currentSeq:
					}
					break // Exit retry loop, wait for next trigger from main loop
				}
			}
		}()

		// Main consumer loop (similar to polling consumer)
		for {
			if pendingMsg.msg == nil {
				// If our consumer is finished and we've run out of pending
				// records then we're done.
				if len(pending) == 0 && state == awsKinesisConsumerFinished {
					if pendingMsg, _ = recordBatcher.FlushMessage(k.ctx); pendingMsg.msg == nil {
						close(subscriptionTrigger)
						subscriptionWg.Wait()
						drainRecordsChan()
						return
					}
				} else if recordBatcher.HasPendingMessage() {
					var err error
					if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
						k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
					}
				} else if len(pending) > 0 {
					var i int
					var r types.Record
					for i, r = range pending {
						if recordBatcher.AddRecord(r) {
							var err error
							if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
								k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
							}
							break
						}
					}
					// Release processed records back to the global pool
					processedCount := i + 1
					k.globalPendingPool.Release(processedCount)
					pending = pending[processedCount:]
				}
			}

			// Decide whether to flush
			if pendingMsg.msg != nil {
				nextFlushChan = k.msgChan
			} else {
				nextFlushChan = nil
			}

			// Always listen for records - backpressure is applied in efoSubscribeAndStream
			// via globalPendingPool.Acquire() before sending to recordsChan
			nextRecordsChan = recordsChan

			if nextTimedBatchChan == nil {
				if tNext, exists := recordBatcher.UntilNext(); exists {
					nextTimedBatchChan = time.After(tNext)
				}
			}

			select {
			case <-commitCtx.Done():
				if k.ctx.Err() != nil {
					state = awsKinesisConsumerClosing
					close(subscriptionTrigger)
					subscriptionWg.Wait()
					drainRecordsChan()
					return
				}

				commitCtxClose()
				commitCtx, commitCtxClose = context.WithTimeout(k.ctx, k.commitPeriod)

				if state == awsKinesisConsumerConsuming {
					stillOwned, err := k.checkpointer.Checkpoint(k.ctx, info.id, shardID, recordBatcher.GetSequence(), false)
					if err != nil {
						k.log.Errorf("Failed to store checkpoint for Kinesis stream '%v' shard '%v': %v", info.id, shardID, err)
					} else if !stillOwned {
						state = awsKinesisConsumerYielding
						close(subscriptionTrigger)
						subscriptionWg.Wait()
						drainRecordsChan()
						return
					}
				}

			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil

			case nextFlushChan <- pendingMsg:
				pendingMsg = asyncMessage{}

			case records := <-nextRecordsChan:
				// Received records from subscription
				// Space was already acquired in efoSubscribeAndStream before sending
				pending = append(pending, records...)

			case err := <-errorsChan:
				// Subscription error received - log it.
				// The subscription goroutine handles its own retry logic with backoff,
				// so we don't need to trigger resubscription from here.
				var resourceNotFound *types.ResourceNotFoundException
				var invalidArg *types.InvalidArgumentException

				if errors.As(err, &resourceNotFound) || errors.As(err, &invalidArg) {
					// Non-retryable errors are still fatal
					k.log.Errorf("Non-retryable EFO error for shard %v: %v", shardID, err)
					state = awsKinesisConsumerClosing
					close(subscriptionTrigger)
					subscriptionWg.Wait()
					drainRecordsChan()
					return
				}

				// Log retryable errors (subscription goroutine handles retry)
				if errors.Is(err, errBackpressureTimeout) {
					k.log.Debugf("EFO backpressure timeout for shard %v, subscription goroutine will retry", shardID)
				} else {
					k.log.Warnf("EFO subscription error for shard %v, subscription goroutine will retry: %v", shardID, err)
				}

			case sequence := <-resubscribeChan:
				// Subscription completed successfully, resubscribe immediately to maintain continuous data flow
				select {
				case subscriptionTrigger <- sequence:
				case <-k.ctx.Done():
				}

			case <-shardFinishedChan:
				// Shard is closed, mark as finished so we can drain pending records
				state = awsKinesisConsumerFinished

			case <-k.ctx.Done():
				state = awsKinesisConsumerClosing
				close(subscriptionTrigger)
				subscriptionWg.Wait()
				drainRecordsChan()
				return
			}
		}
	}()

	return nil
}

// efoSubscribeAndStream subscribes to a shard and streams records to a channel
// Returns: continuationSequence, shardFinished, error
func (k *kinesisReader) efoSubscribeAndStream(ctx context.Context, info streamInfo, shardID, startingSequence string, recordsChan chan<- []types.Record) (string, bool, error) {
	if info.efoManager == nil || info.efoManager.consumerARN == "" {
		return "", false, errors.New("EFO manager or consumer ARN not initialized")
	}

	// Build starting position
	var startingPosition *types.StartingPosition
	if startingSequence == "" {
		// No sequence yet, use TRIM_HORIZON or LATEST based on config
		if k.conf.StartFromOldest {
			startingPosition = &types.StartingPosition{
				Type: types.ShardIteratorTypeTrimHorizon,
			}
		} else {
			startingPosition = &types.StartingPosition{
				Type: types.ShardIteratorTypeLatest,
			}
		}
	} else {
		// Continue from last sequence
		startingPosition = &types.StartingPosition{
			Type:           types.ShardIteratorTypeAfterSequenceNumber,
			SequenceNumber: aws.String(startingSequence),
		}
	}

	k.log.Debugf("Subscribing to shard %v with sequence %v", shardID, startingSequence)

	input := &kinesis.SubscribeToShardInput{
		ConsumerARN:      aws.String(info.efoManager.consumerARN),
		ShardId:          aws.String(shardID),
		StartingPosition: startingPosition,
	}

	output, err := k.svc.SubscribeToShard(ctx, input)
	if err != nil {
		return "", false, fmt.Errorf("failed to subscribe to shard: %w", err)
	}

	// Process the event stream
	eventStream := output.GetStream()
	defer eventStream.Close()

	continuationSeq := ""
	lastReceivedSeq := ""
	shardFinished := false
	eventsChan := eventStream.Events()

	// Timeout for waiting on backpressure - if we wait too long, close the subscription
	// cleanly and resubscribe rather than letting AWS forcibly terminate the connection.
	// 30 seconds is well under the 5-minute EFO subscription timeout.
	const backpressureTimeout = 30 * time.Second

	for {
		// Wait for space in the global pool before fetching the next event
		// This applies backpressure to Kinesis before data enters memory
		switch k.globalPendingPool.WaitForSpace(ctx, backpressureTimeout) {
		case WaitForSpaceCancelled:
			// Context cancelled
			if continuationSeq == "" {
				continuationSeq = lastReceivedSeq
			}
			return continuationSeq, false, ctx.Err()
		case WaitForSpaceTimeout:
			// Backpressure timeout - close subscription cleanly and return error to trigger backoff
			// This prevents AWS from forcibly terminating the connection after extended inactivity
			// and ensures we don't immediately resubscribe while backpressure persists
			k.log.Debugf("Backpressure timeout for shard %v, closing subscription to resubscribe with backoff", shardID)
			if continuationSeq == "" {
				continuationSeq = lastReceivedSeq
			}
			return continuationSeq, false, errBackpressureTimeout
		case WaitForSpaceOK:
			// Space available, continue
		}

		// Now fetch the next event
		event, ok := <-eventsChan
		if !ok {
			break
		}

		switch e := event.(type) {
		case *types.SubscribeToShardEventStreamMemberSubscribeToShardEvent:
			// Got records event
			shardEvent := e.Value

			// Send records to channel and track last received sequence
			if len(shardEvent.Records) > 0 {
				// Acquire the actual space for this batch
				if !k.globalPendingPool.Acquire(ctx, len(shardEvent.Records)) {
					// Context cancelled, return with current sequence
					if continuationSeq == "" {
						continuationSeq = lastReceivedSeq
					}
					return continuationSeq, false, ctx.Err()
				}

				// Track the last record's sequence number for fallback
				lastRecord := shardEvent.Records[len(shardEvent.Records)-1]
				if lastRecord.SequenceNumber != nil {
					lastReceivedSeq = *lastRecord.SequenceNumber
				}
				select {
				case recordsChan <- shardEvent.Records:
				case <-ctx.Done():
					// Release the acquired space since we couldn't send
					k.globalPendingPool.Release(len(shardEvent.Records))
					// Use lastReceivedSeq as fallback if continuationSeq not set
					if continuationSeq == "" {
						continuationSeq = lastReceivedSeq
					}
					return continuationSeq, false, ctx.Err()
				}
			}

			// Update continuation sequence for next subscription
			if shardEvent.ContinuationSequenceNumber != nil {
				continuationSeq = *shardEvent.ContinuationSequenceNumber
			}

			// Check if shard is closed (has child shards)
			if len(shardEvent.ChildShards) > 0 {
				k.log.Debugf("Shard %v is closed, child shards: %v", shardID, len(shardEvent.ChildShards))
				shardFinished = true
			}

			if shardEvent.MillisBehindLatest != nil {
				k.log.Debugf("Shard %v is %d milliseconds behind latest", shardID, *shardEvent.MillisBehindLatest)
			}

		default:
			k.log.Warnf("Unknown event type received: %T", event)
		}
	}

	// Use lastReceivedSeq as fallback if continuationSeq not set
	if continuationSeq == "" {
		continuationSeq = lastReceivedSeq
	}

	// Check for stream errors
	if err := eventStream.Err(); err != nil {
		// Check if it's just end of stream
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return continuationSeq, shardFinished, nil
		}
		return continuationSeq, shardFinished, fmt.Errorf("error receiving event: %w", err)
	}

	return continuationSeq, shardFinished, nil
}
