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

// kinesisEFOAPI is the subset of kinesis.Client methods used by kinesisEFOManager.
type kinesisEFOAPI interface {
	RegisterStreamConsumer(ctx context.Context, params *kinesis.RegisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error)
	DescribeStreamConsumer(ctx context.Context, params *kinesis.DescribeStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error)
}

// kinesisEFOManager handles Enhanced Fan Out consumer registration and lifecycle
type kinesisEFOManager struct {
	streamARN    string
	consumerName string
	consumerARN  string
	svc          kinesisEFOAPI
	log          *service.Logger
	// pollInterval controls how long waitForActiveConsumer waits between status
	// checks. Defaults to 2 seconds; overridable in tests for faster iteration.
	pollInterval time.Duration
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

	interval := m.pollInterval
	if interval == 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		// Check consumer status immediately before waiting for the next tick
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

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled waiting for consumer to become ACTIVE: %w", ctx.Err())
		case <-waiterCtx.Done():
			// Check if it was the parent context that cancelled (both fire when parent cancels)
			if ctx.Err() != nil {
				return fmt.Errorf("context cancelled waiting for consumer to become ACTIVE: %w", ctx.Err())
			}
			return fmt.Errorf("timeout waiting for consumer to become ACTIVE: %w", waiterCtx.Err())
		case <-ticker.C:
		}
	}
}

// runEFOConsumer consumes from a shard using Enhanced Fan Out with synchronous
// processing. Inspired by the KCL v2/v3 model: one goroutine per shard,
// process one event at a time, block on the pipeline when it's busy.
//
// Backpressure is implicit: when msgChan blocks (pipeline full), the shard
// stops reading from Kinesis. No pool or buffer management is needed.
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

	state := awsKinesisConsumerConsuming
	var pendingMsg asyncMessage
	boff := k.boffPool.Get().(backoff.BackOff)
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

		currentSeq := startingSequence

		// Outer loop: subscribe, process events, resubscribe on completion/error
		for state == awsKinesisConsumerConsuming {
			continuationSeq, shardFinished, subErr := k.efoSubscribeAndProcess(
				k.ctx, info, shardID, currentSeq,
				recordBatcher, &pendingMsg, &state,
				&commitCtx, &commitCtxClose,
			)

			if shardFinished {
				state = awsKinesisConsumerFinished
				break
			}

			if subErr != nil {
				if k.ctx.Err() != nil {
					state = awsKinesisConsumerClosing
					break
				}

				// Check for non-retryable errors
				var resourceNotFound *types.ResourceNotFoundException
				var invalidArg *types.InvalidArgumentException
				if errors.As(subErr, &resourceNotFound) || errors.As(subErr, &invalidArg) {
					k.log.Errorf("Non-retryable EFO error for shard %v: %v", shardID, subErr)
					state = awsKinesisConsumerClosing
					break
				}

				// Retryable error — backoff and retry
				k.log.Warnf("EFO subscription error for shard %v, will retry: %v", shardID, subErr)
				// Update sequence so retry doesn't reprocess from old position
				if continuationSeq != "" {
					currentSeq = continuationSeq
				}
				select {
				case <-time.After(boff.NextBackOff()):
				case <-k.ctx.Done():
					state = awsKinesisConsumerClosing
				}
				continue
			}

			// Subscription completed normally — reset backoff, resubscribe
			boff.Reset()

			if continuationSeq != "" {
				currentSeq = continuationSeq
			}
		}

		// Drain any remaining batched message before exiting
		if pendingMsg.msg != nil {
			select {
			case k.msgChan <- pendingMsg:
				pendingMsg = asyncMessage{}
			case <-k.ctx.Done():
			}
		}
	}()

	return nil
}

// efoSubscribeAndProcess subscribes to a shard and processes events inline.
// Records are added to the batcher and flushed to msgChan synchronously —
// when the pipeline is busy, this function blocks, which stops reading from
// Kinesis (natural backpressure).
//
// Returns: continuationSequence, shardFinished, error
func (k *kinesisReader) efoSubscribeAndProcess(
	ctx context.Context, info streamInfo, shardID, startingSequence string,
	recordBatcher *awsKinesisRecordBatcher,
	pendingMsg *asyncMessage, state *awsKinesisConsumerState,
	commitCtx *context.Context, commitCtxClose *context.CancelFunc,
) (string, bool, error) {
	if info.efoManager == nil || info.efoManager.consumerARN == "" {
		return "", false, errors.New("EFO manager or consumer ARN not initialized")
	}

	// Build starting position
	var startingPosition *types.StartingPosition
	if startingSequence == "" {
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

	eventStream := output.GetStream()
	defer eventStream.Close()

	continuationSeq := ""
	shardFinished := false
	eventsChan := eventStream.Events()

	// Timed batch support: if the batch policy has a time trigger, we need
	// to flush even when no events arrive from Kinesis.
	var nextTimedBatchChan <-chan time.Time

	for {
		// Set up timed batch flush if the batcher has a pending timer
		if nextTimedBatchChan == nil {
			if tNext, exists := recordBatcher.UntilNext(); exists {
				nextTimedBatchChan = time.After(tNext)
			}
		}

		select {
		case event, ok := <-eventsChan:
			if !ok {
				// Stream ended — flush any remaining records in batcher before returning
				if err := k.flushBatchedMessage(ctx, recordBatcher, pendingMsg, *commitCtx); err != nil {
					k.log.Errorf("Failed to flush remaining records on stream end: %v", err)
				}
				goto streamEnded
			}

			switch e := event.(type) {
			case *types.SubscribeToShardEventStreamMemberSubscribeToShardEvent:
				shardEvent := e.Value

				// Add records directly to batcher — no channel, no pending slice
				for _, record := range shardEvent.Records {
					if recordBatcher.AddRecord(record) {
						// Batch full — flush to pipeline
						if err := k.flushBatchedMessage(ctx, recordBatcher, pendingMsg, *commitCtx); err != nil {
							k.log.Errorf("Failed to flush message: %v", err)
							continue
						}
					}
				}

				// Send any pending flushed message to the pipeline
				if pendingMsg.msg != nil {
					select {
					case k.msgChan <- *pendingMsg:
						*pendingMsg = asyncMessage{}
					case <-ctx.Done():
						return continuationSeq, false, ctx.Err()
					}
				}

				// Update continuation sequence
				if shardEvent.ContinuationSequenceNumber != nil {
					continuationSeq = *shardEvent.ContinuationSequenceNumber
				}

				// Check if shard is closed
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

		case <-nextTimedBatchChan:
			// Timed batch trigger — flush even without new events
			nextTimedBatchChan = nil
			if err := k.flushBatchedMessage(ctx, recordBatcher, pendingMsg, *commitCtx); err != nil {
				k.log.Errorf("Failed to flush timed batch: %v", err)
			}

		case <-(*commitCtx).Done():
			// Periodic checkpoint / lease renewal — fires even during idle streams
			if ctx.Err() != nil {
				return continuationSeq, false, ctx.Err()
			}

			(*commitCtxClose)()
			*commitCtx, *commitCtxClose = context.WithTimeout(ctx, k.commitPeriod)

			if *state == awsKinesisConsumerConsuming {
				stillOwned, cpErr := k.checkpointer.Checkpoint(ctx, info.id, shardID, recordBatcher.GetSequence(), false)
				if cpErr != nil {
					k.log.Errorf("Failed to store checkpoint for Kinesis stream '%v' shard '%v': %v", info.id, shardID, cpErr)
				} else if !stillOwned {
					*state = awsKinesisConsumerYielding
					return continuationSeq, false, nil
				}
			}

		case <-ctx.Done():
			return continuationSeq, false, ctx.Err()
		}
	}

streamEnded:
	// Check for stream errors
	if err := eventStream.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return continuationSeq, shardFinished, nil
		}
		return continuationSeq, shardFinished, fmt.Errorf("error receiving event: %w", err)
	}

	return continuationSeq, shardFinished, nil
}

// flushBatchedMessage flushes the batcher into pendingMsg and sends it to the
// pipeline via msgChan. Blocks until the pipeline accepts the message
// (backpressure) or the context is cancelled.
func (k *kinesisReader) flushBatchedMessage(
	ctx context.Context,
	recordBatcher *awsKinesisRecordBatcher,
	pendingMsg *asyncMessage,
	commitCtx context.Context,
) error {
	// First send any previously flushed message that hasn't been sent yet
	if pendingMsg.msg != nil {
		select {
		case k.msgChan <- *pendingMsg:
			*pendingMsg = asyncMessage{}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Now flush the batcher
	var err error
	if *pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
		return fmt.Errorf("failed to flush message due to checkpoint error: %w", err)
	}

	if pendingMsg.msg != nil {
		select {
		case k.msgChan <- *pendingMsg:
			*pendingMsg = asyncMessage{}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
