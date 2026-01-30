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

	// Backoff for error handling
	boff := k.boffPool.Get().(backoff.BackOff)

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
			boff.Reset()
			k.boffPool.Put(boff)

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
		errorsChan := make(chan error, 1)
		resubscribeChan := make(chan string, 1)
		shardFinishedChan := make(chan struct{}, 1)

		var subscriptionWg sync.WaitGroup
		subscriptionWg.Add(1)
		go func() {
			defer subscriptionWg.Done()
			for sequence := range subscriptionTrigger {
				continuationSeq, shardFinished, err := k.efoSubscribeAndStream(k.ctx, info, shardID, sequence, recordsChan)
				if err != nil {
					errorsChan <- err
				} else if shardFinished {
					// Shard is closed, signal to main loop
					// Don't resubscribe to closed shards
					select {
					case shardFinishedChan <- struct{}{}:
					default:
					}
					return
				} else {
					// Schedule resubscription with continuation sequence
					if continuationSeq != "" {
						resubscribeChan <- continuationSeq
					} else {
						// Use latest checkpointed sequence
						resubscribeChan <- recordBatcher.GetSequence()
					}
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
					pending = pending[i+1:]
				}
			}

			if pendingMsg.msg != nil {
				nextFlushChan = k.msgChan
				nextRecordsChan = nil
			} else {
				nextFlushChan = nil
				if len(pending) == 0 {
					nextRecordsChan = recordsChan
				} else {
					nextRecordsChan = nil
				}
			}

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
						return
					}
				}

			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil

			case nextFlushChan <- pendingMsg:
				pendingMsg = asyncMessage{}

			case records := <-nextRecordsChan:
				// Received records from subscription
				pending = append(pending, records...)
				boff.Reset()

			case err := <-errorsChan:
				// Subscription error occurred
				var resourceNotFound *types.ResourceNotFoundException
				var invalidArg *types.InvalidArgumentException

				if errors.As(err, &resourceNotFound) || errors.As(err, &invalidArg) {
					k.log.Errorf("Non-retryable EFO error for shard %v: %v", shardID, err)
					state = awsKinesisConsumerClosing
					close(subscriptionTrigger)
					subscriptionWg.Wait()
					return
				}

				// Retryable error - backoff and retry
				k.log.Warnf("EFO subscription error for shard %v, will retry: %v", shardID, err)
				backoffDuration := boff.NextBackOff()
				sequence := recordBatcher.GetSequence()
				time.AfterFunc(backoffDuration, func() {
					// Trigger resubscription after backoff, unless context has been cancelled
					select {
					case <-k.ctx.Done():
						return
					case subscriptionTrigger <- sequence:
					default:
					}
				})

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
	shardFinished := false
	for event := range eventStream.Events() {
		switch e := event.(type) {
		case *types.SubscribeToShardEventStreamMemberSubscribeToShardEvent:
			// Got records event
			shardEvent := e.Value

			// Send records to channel
			if len(shardEvent.Records) > 0 {
				select {
				case recordsChan <- shardEvent.Records:
				case <-ctx.Done():
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
