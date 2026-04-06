package aws

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/gofrs/uuid"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// DynamoDB Streams Input Fields
	ddbsFieldTable          = "table"
	ddbsFieldStreamARN      = "stream_arn"
	ddbsFieldBatching       = "batching"
	ddbsFieldStartFromOldest = "start_from_oldest"
	ddbsFieldCheckpointLimit = "checkpoint_limit"
	ddbsFieldCommitPeriod    = "commit_period"
	ddbsFieldLeasePeriod     = "lease_period"
	ddbsFieldRebalancePeriod = "rebalance_period"
	ddbsFieldPollInterval    = "poll_interval"

	// DynamoDB Streams Checkpoint DynamoDB Fields
	ddbsCPFieldTable              = "table"
	ddbsCPFieldCreate             = "create"
	ddbsCPFieldBillingMode        = "billing_mode"
	ddbsCPFieldReadCapacityUnits  = "read_capacity_units"
	ddbsCPFieldWriteCapacityUnits = "write_capacity_units"
	ddbsFieldCheckpoint           = "checkpoint"
)

type ddbsConfig struct {
	Table           string
	StreamARN       string
	StartFromOldest bool
	CheckpointLimit int
	CommitPeriod    time.Duration
	LeasePeriod     time.Duration
	RebalancePeriod time.Duration
	PollInterval    time.Duration
	Checkpoint      ddbsCheckpointConfig
}

func ddbsInputConfigFromParsed(pConf *service.ParsedConfig) (conf ddbsConfig, err error) {
	if conf.Table, err = pConf.FieldString(ddbsFieldTable); err != nil {
		return
	}
	if conf.StreamARN, err = pConf.FieldString(ddbsFieldStreamARN); err != nil {
		return
	}
	if conf.StartFromOldest, err = pConf.FieldBool(ddbsFieldStartFromOldest); err != nil {
		return
	}
	if conf.CheckpointLimit, err = pConf.FieldInt(ddbsFieldCheckpointLimit); err != nil {
		return
	}
	if conf.CommitPeriod, err = pConf.FieldDuration(ddbsFieldCommitPeriod); err != nil {
		return
	}
	if conf.LeasePeriod, err = pConf.FieldDuration(ddbsFieldLeasePeriod); err != nil {
		return
	}
	if conf.RebalancePeriod, err = pConf.FieldDuration(ddbsFieldRebalancePeriod); err != nil {
		return
	}
	if conf.PollInterval, err = pConf.FieldDuration(ddbsFieldPollInterval); err != nil {
		return
	}
	if pConf.Contains(ddbsFieldCheckpoint) {
		if conf.Checkpoint, err = ddbsCheckpointConfigFromParsed(pConf.Namespace(ddbsFieldCheckpoint)); err != nil {
			return
		}
	}
	return
}

func dynamoDBStreamsInputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Services", "AWS").
		Summary("Consume messages from a DynamoDB stream.").
		Description(`
Consumes change data capture events from a DynamoDB stream. Each message represents an INSERT, MODIFY, or REMOVE event on items in the source DynamoDB table.

The latest consumed sequence for each shard is stored in a [DynamoDB table](#checkpoint), which allows this input to resume at the correct position during restarts. This table is also used for coordination across distributed instances when shard balancing.

Bento will not store a consumed sequence unless it is acknowledged at the output level, which ensures at-least-once delivery guarantees.

### Ordering

By default messages of a shard can be processed in parallel, up to a limit determined by the field ` + "`checkpoint_limit`" + `. However, if strict ordered processing is required then this value must be set to 1 in order to process shard messages in lock-step.

### Table Schema

It's possible to configure Bento to create the DynamoDB checkpoint table if it does not already exist. However, if you wish to create this yourself (recommended) then create a table with a string HASH key ` + "`StreamID`" + ` and a string RANGE key ` + "`ShardID`" + `.

### Stream ARN vs Table Name

You can specify either a ` + "`table`" + ` name (in which case the latest stream ARN is resolved automatically) or an explicit ` + "`stream_arn`" + `. If both are specified, ` + "`stream_arn`" + ` takes precedence.

### DynamoDB Streams Retention

DynamoDB Streams retains data for 24 hours. If the consumer falls behind by more than 24 hours, the checkpoint will be stale and the shard iterator will be invalid. In this case the consumer will automatically reset to the oldest or latest position based on the ` + "`start_from_oldest`" + ` setting.
`).Fields(
		service.NewStringField(ddbsFieldTable).
			Description("The DynamoDB table name to consume the stream from. The stream ARN is resolved automatically. Either `table` or `stream_arn` must be specified.").
			Default("").
			Examples("my-table"),
		service.NewStringField(ddbsFieldStreamARN).
			Description("An explicit DynamoDB stream ARN to consume from. Takes precedence over `table` if both are specified.").
			Default("").
			Examples("arn:aws:dynamodb:us-east-1:123456789012:table/my-table/stream/2024-03-20T18:19:47.921").
			Advanced(),
		service.NewObjectField(ddbsFieldCheckpoint,
			service.NewStringField(ddbsCPFieldTable).
				Description("The name of the DynamoDB table used for storing checkpoints.").
				Default(""),
			service.NewBoolField(ddbsCPFieldCreate).
				Description("Whether, if the checkpoint table does not exist, it should be created.").
				Default(false),
			service.NewStringEnumField(ddbsCPFieldBillingMode, "PROVISIONED", "PAY_PER_REQUEST").
				Description("When creating the checkpoint table, determines the billing mode.").
				Default("PAY_PER_REQUEST").
				Advanced(),
			service.NewIntField(ddbsCPFieldReadCapacityUnits).
				Description("Set the provisioned read capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
				Default(0).
				Advanced(),
			service.NewIntField(ddbsCPFieldWriteCapacityUnits).
				Description("Set the provisioned write capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
				Default(0).
				Advanced(),
		).
			Description("Determines the table used for storing and accessing the latest consumed sequence for shards, and for coordinating balanced consumers of streams."),
		service.NewIntField(ddbsFieldCheckpointLimit).
			Description("The maximum gap between the in flight sequence versus the latest acknowledged sequence at a given time. Increasing this limit enables parallel processing and batching at the output level to work on individual shards. Any given sequence will not be committed unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			Default(1024),
		service.NewAutoRetryNacksToggleField(),
		service.NewDurationField(ddbsFieldCommitPeriod).
			Description("The period of time between each update to the checkpoint table.").
			Default("5s"),
		service.NewDurationField(ddbsFieldRebalancePeriod).
			Description("The period of time between each attempt to rebalance shards across clients.").
			Default("30s").
			Advanced(),
		service.NewDurationField(ddbsFieldLeasePeriod).
			Description("The period of time after which a client that has failed to update a shard checkpoint is assumed to be inactive.").
			Default("30s").
			Advanced(),
		service.NewBoolField(ddbsFieldStartFromOldest).
			Description("Whether to consume from the oldest record when a sequence does not yet exist for the stream.").
			Default(true),
		service.NewDurationField(ddbsFieldPollInterval).
			Description("The minimum interval between GetRecords calls for a given shard. DynamoDB Streams supports up to 5 GetRecords calls per second per shard.").
			Default("1s").
			Advanced(),
	).
		Fields(config.SessionFields()...).
		Field(service.NewBatchPolicyField(ddbsFieldBatching)).
		LintRule(`root = match {
  this.table == "" && this.stream_arn == "" => ["either table or stream_arn must be specified"]
}`)

	return spec
}

func init() {
	err := service.RegisterBatchInput("aws_dynamodb_streams", dynamoDBStreamsInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			r, err := newDynamoDBStreamsReaderFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, r)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type dynamoDBStreamsReader struct {
	conf     ddbsConfig
	clientID string

	sess    aws.Config
	batcher service.BatchPolicy
	log     *service.Logger
	mgr     *service.Resources

	boffPool sync.Pool

	streamsSvc   *dynamodbstreams.Client
	dynamodbSvc  *dynamodb.Client
	checkpointer *ddbsCheckpointer

	streamARN string
	streamID  string // Checkpoint key: table name if available, otherwise stream ARN.

	commitPeriod    time.Duration
	leasePeriod     time.Duration
	rebalancePeriod time.Duration
	pollInterval    time.Duration

	cMut    sync.Mutex
	msgChan chan asyncMessage

	ctx  context.Context
	done func()

	closeOnce  sync.Once
	closedChan chan struct{}
}

func newDynamoDBStreamsReaderFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*dynamoDBStreamsReader, error) {
	conf, err := ddbsInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	sess, err := GetSession(context.TODO(), pConf)
	if err != nil {
		return nil, err
	}
	batcher, err := pConf.FieldBatchPolicy(ddbsFieldBatching)
	if err != nil {
		return nil, err
	}
	return newDynamoDBStreamsReaderFromConfig(conf, batcher, sess, mgr)
}

func newDynamoDBStreamsReaderFromConfig(conf ddbsConfig, batcher service.BatchPolicy, sess aws.Config, mgr *service.Resources) (*dynamoDBStreamsReader, error) {
	if batcher.IsNoop() {
		batcher.Count = 1
	}

	r := dynamoDBStreamsReader{
		conf:            conf,
		sess:            sess,
		batcher:         batcher,
		log:             mgr.Logger(),
		mgr:             mgr,
		closedChan:      make(chan struct{}),
		commitPeriod:    conf.CommitPeriod,
		leasePeriod:     conf.LeasePeriod,
		rebalancePeriod: conf.RebalancePeriod,
		pollInterval:    conf.PollInterval,
	}
	r.ctx, r.done = context.WithCancel(context.Background())

	u4, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	r.clientID = u4.String()

	r.boffPool = sync.Pool{
		New: func() any {
			boff := backoff.NewExponentialBackOff()
			boff.InitialInterval = time.Millisecond * 300
			boff.MaxInterval = time.Second * 5
			boff.MaxElapsedTime = 0
			return boff
		},
	}

	return &r, nil
}

//------------------------------------------------------------------------------

// resolveStreamARN resolves the stream ARN either from the explicit config or
// by describing the DynamoDB table.
func (r *dynamoDBStreamsReader) resolveStreamARN(ctx context.Context) (string, error) {
	if r.conf.StreamARN != "" {
		return r.conf.StreamARN, nil
	}

	out, err := r.dynamodbSvc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(r.conf.Table),
	})
	if err != nil {
		return "", fmt.Errorf("failed to describe table %q: %w", r.conf.Table, err)
	}
	if out.Table.LatestStreamArn == nil || *out.Table.LatestStreamArn == "" {
		return "", fmt.Errorf("table %q does not have streams enabled", r.conf.Table)
	}
	return *out.Table.LatestStreamArn, nil
}

// collectShards returns all shards for the given stream ARN via pagination.
func (r *dynamoDBStreamsReader) collectShards(ctx context.Context, streamARN string) ([]types.Shard, error) {
	var shards []types.Shard
	var lastShardID *string

	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(streamARN),
			ExclusiveStartShardId: lastShardID,
		}
		out, err := r.streamsSvc.DescribeStream(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to describe stream: %w", err)
		}
		if out.StreamDescription == nil {
			return nil, errors.New("stream description is nil")
		}

		shards = append(shards, out.StreamDescription.Shards...)

		lastShardID = out.StreamDescription.LastEvaluatedShardId
		if lastShardID == nil {
			break
		}
	}

	return shards, nil
}

// isDDBShardOpen returns true if the shard does not have an ending sequence number,
// meaning it is still accepting writes.
func isDDBShardOpen(s types.Shard) bool {
	if s.SequenceNumberRange == nil {
		return true
	}
	return s.SequenceNumberRange.EndingSequenceNumber == nil
}

func (r *dynamoDBStreamsReader) getShardIterator(ctx context.Context, shardID, sequence string) (string, error) {
	iterType := types.ShardIteratorTypeTrimHorizon
	if !r.conf.StartFromOldest {
		iterType = types.ShardIteratorTypeLatest
	}

	var startingSequence *string
	if sequence != "" {
		iterType = types.ShardIteratorTypeAfterSequenceNumber
		startingSequence = &sequence
	}

	res, err := r.streamsSvc.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:              aws.String(r.streamARN),
		ShardId:                aws.String(shardID),
		ShardIteratorType:      iterType,
		SequenceNumber:         startingSequence,
	})
	if err != nil {
		// If the sequence number is expired (>24h retention), fall back to
		// the configured starting position.
		var trimErr *types.TrimmedDataAccessException
		if errors.As(err, &trimErr) && sequence != "" {
			r.log.Warnf("Sequence expired for shard %v, resetting to %v", shardID, iterType)
			return r.getShardIterator(ctx, shardID, "")
		}
		return "", err
	}

	if res.ShardIterator == nil || *res.ShardIterator == "" {
		return "", fmt.Errorf("failed to obtain shard iterator for shard %v", shardID)
	}
	return *res.ShardIterator, nil
}

// getRecords fetches records from a shard iterator. Returns the records, the
// next iterator (empty string if shard is exhausted), and any error. On error
// the input iterator is always returned so callers can safely replace their
// stored iterator.
func (r *dynamoDBStreamsReader) getRecords(shardIter string) ([]types.Record, string, error) {
	res, err := r.streamsSvc.GetRecords(r.ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(shardIter),
	})
	if err != nil {
		return nil, shardIter, err
	}

	nextIter := ""
	if res.NextShardIterator != nil {
		nextIter = *res.NextShardIterator
	}
	return res.Records, nextIter, nil
}

//------------------------------------------------------------------------------

type ddbsConsumerState int

const (
	ddbsConsumerConsuming ddbsConsumerState = iota
	ddbsConsumerYielding
	ddbsConsumerFinished
	ddbsConsumerClosing
)

func (r *dynamoDBStreamsReader) runConsumer(wg *sync.WaitGroup, shardID, startingSequence string) (initErr error) {
	defer func() {
		if initErr != nil {
			wg.Done()
			if _, err := r.checkpointer.Checkpoint(context.Background(), r.streamID, shardID, startingSequence, true); err != nil {
				r.log.Errorf("Failed to gracefully yield checkpoint: %v\n", err)
			}
		}
	}()

	// Stores records, batches them up, and provides the batches for dispatch,
	// whilst ensuring only N records are in flight at a given time.
	var recordBatcher *ddbsRecordBatcher
	if recordBatcher, initErr = r.newDDBSRecordBatcher(shardID, startingSequence); initErr != nil {
		return initErr
	}

	boff := r.boffPool.Get().(backoff.BackOff)

	var pending []types.Record
	var iter string
	if iter, initErr = r.getShardIterator(r.ctx, shardID, startingSequence); initErr != nil {
		return initErr
	}

	state := ddbsConsumerConsuming
	var pendingMsg asyncMessage

	unblockedChan, blockedChan := make(chan time.Time), make(chan time.Time)
	close(unblockedChan)

	var nextTimedBatchChan <-chan time.Time
	var nextPullChan <-chan time.Time = unblockedChan
	var nextFlushChan chan<- asyncMessage
	commitCtx, commitCtxClose := context.WithTimeout(r.ctx, r.commitPeriod)

	go func() {
		defer func() {
			commitCtxClose()
			recordBatcher.Close(context.Background(), state == ddbsConsumerFinished)
			boff.Reset()
			r.boffPool.Put(boff)

			reason := ""
			switch state {
			case ddbsConsumerFinished:
				reason = " because the shard is closed"
				if err := r.checkpointer.Complete(r.ctx, r.streamID, shardID, recordBatcher.GetSequence()); err != nil {
					r.log.Errorf("Failed to mark shard '%v' as completed: %v", shardID, err)
				}
			case ddbsConsumerYielding:
				reason = " because the shard has been claimed by another client"
				if err := r.checkpointer.Yield(r.ctx, r.streamID, shardID, recordBatcher.GetSequence()); err != nil {
					r.log.Errorf("Failed to yield checkpoint for stolen shard '%v': %v", shardID, err)
				}
			case ddbsConsumerClosing:
				reason = " because the pipeline is shutting down"
				if _, err := r.checkpointer.Checkpoint(context.Background(), r.streamID, shardID, recordBatcher.GetSequence(), true); err != nil {
					r.log.Errorf("Failed to store final checkpoint for shard '%v': %v", shardID, err)
				}
			}

			wg.Done()
			r.log.Debugf("Closing shard '%v' as client '%v'%v", shardID, r.checkpointer.clientID, reason)
		}()

		r.log.Debugf("Consuming shard '%v' as client '%v'", shardID, r.checkpointer.clientID)

		unblockPullChan := func() {
			if nextPullChan == blockedChan {
				nextPullChan = unblockedChan
			}
		}

		for {
			var err error
			if state == ddbsConsumerConsuming && len(pending) == 0 && nextPullChan == unblockedChan {
				if pending, iter, err = r.getRecords(iter); err != nil {
					if !awsErrIsTimeout(err) {
						nextPullChan = time.After(boff.NextBackOff())

						var expiredErr *types.ExpiredIteratorException
						var trimErr *types.TrimmedDataAccessException
						if errors.As(err, &expiredErr) {
							r.log.Warn("Shard iterator expired, attempting to refresh")
							newIter, err := r.getShardIterator(r.ctx, shardID, recordBatcher.GetSequence())
							if err != nil {
								r.log.Errorf("Failed to refresh shard iterator: %v", err)
							} else {
								iter = newIter
							}
						} else if errors.As(err, &trimErr) {
							r.log.Warnf("Shard '%v' data trimmed (24h retention exceeded), resetting iterator", shardID)
							newIter, err := r.getShardIterator(r.ctx, shardID, "")
							if err != nil {
								r.log.Errorf("Failed to refresh shard iterator after trim: %v", err)
							} else {
								iter = newIter
							}
						} else {
							r.log.Errorf("Failed to pull DynamoDB stream records: %v\n", err)
						}
					}
				} else if len(pending) == 0 {
					// No records available, back off using poll interval
					nextPullChan = time.After(r.pollInterval)
				} else {
					boff.Reset()
					nextPullChan = blockedChan
				}
				if iter == "" {
					state = ddbsConsumerFinished
				}
			} else {
				unblockPullChan()
			}

			if pendingMsg.msg == nil {
				if len(pending) == 0 && state == ddbsConsumerFinished {
					if pendingMsg, _ = recordBatcher.FlushMessage(r.ctx); pendingMsg.msg == nil {
						return
					}
				} else if recordBatcher.HasPendingMessage() {
					if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
						r.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
					}
				} else if len(pending) > 0 {
					var i int
					var rec types.Record
					for i, rec = range pending {
						if recordBatcher.AddRecord(rec) {
							if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
								r.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
							}
							break
						}
					}
					if pending = pending[i+1:]; len(pending) == 0 {
						unblockPullChan()
					}
				} else {
					unblockPullChan()
				}
			}

			if pendingMsg.msg != nil {
				nextFlushChan = r.msgChan
			} else {
				nextFlushChan = nil
			}

			if nextTimedBatchChan == nil {
				if tNext, exists := recordBatcher.UntilNext(); exists {
					nextTimedBatchChan = time.After(tNext)
				}
			}

			select {
			case <-commitCtx.Done():
				if r.ctx.Err() != nil {
					state = ddbsConsumerClosing
					return
				}

				commitCtxClose()
				commitCtx, commitCtxClose = context.WithTimeout(r.ctx, r.commitPeriod)

				stillOwned, err := r.checkpointer.Checkpoint(r.ctx, r.streamID, shardID, recordBatcher.GetSequence(), false)
				if err != nil {
					r.log.Errorf("Failed to store checkpoint for shard '%v': %v", shardID, err)
				} else if !stillOwned {
					state = ddbsConsumerYielding
					return
				}
			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil
			case nextFlushChan <- pendingMsg:
				pendingMsg = asyncMessage{}
			case <-nextPullChan:
				nextPullChan = unblockedChan
			case <-r.ctx.Done():
				state = ddbsConsumerClosing
				return
			}
		}
	}()
	return nil
}

//------------------------------------------------------------------------------

func (r *dynamoDBStreamsReader) runBalancedShards() {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		r.closeOnce.Do(func() {
			close(r.msgChan)
			close(r.closedChan)
		})
	}()

	for {
		allShards, err := r.collectShards(r.ctx, r.streamARN)
		var checkpointData *ddbsCheckpointData
		if err == nil {
			checkpointData, err = r.checkpointer.GetCheckpointsAndClaims(r.ctx, r.streamID)
		}
		if err != nil {
			if r.ctx.Err() != nil {
				return
			}
			r.log.Errorf("Failed to obtain stream shards or claims: %v", err)
			select {
			case <-time.After(r.rebalancePeriod):
				continue
			case <-r.ctx.Done():
				return
			}
		}

		clientClaims := checkpointData.ClientClaims
		shardsWithCheckpoints := checkpointData.ShardsWithCheckpoints
		completedShards := checkpointData.CompletedShards

		// Build parent map so we can enforce parent-before-child ordering.
		parentOf := make(map[string]string, len(allShards))
		for _, s := range allShards {
			if s.ParentShardId != nil && *s.ParentShardId != "" {
				parentOf[*s.ShardId] = *s.ParentShardId
			}
		}

		unclaimedShards := make(map[string]string, len(allShards))
		for _, s := range allShards {
			shardID := *s.ShardId

			// Skip completed shards — they're fully consumed.
			if completedShards[shardID] {
				continue
			}

			// Skip child shards whose parent hasn't been fully consumed yet.
			// This ensures per-key ordering is preserved across shard splits.
			if parentID, hasParent := parentOf[shardID]; hasParent && !completedShards[parentID] {
				continue
			}

			if isDDBShardOpen(s) || shardsWithCheckpoints[shardID] {
				unclaimedShards[shardID] = ""
			}
		}
		for clientID, claims := range clientClaims {
			for _, claim := range claims {
				if time.Since(claim.LeaseTimeout) > r.leasePeriod*2 {
					unclaimedShards[claim.ShardID] = clientID
				} else {
					delete(unclaimedShards, claim.ShardID)
				}
			}
		}

		if len(unclaimedShards) > 0 {
			for shardID, clientID := range unclaimedShards {
				sequence, err := r.checkpointer.Claim(r.ctx, r.streamID, shardID, clientID)
				if err != nil {
					if r.ctx.Err() != nil {
						return
					}
					if !errors.Is(err, ErrLeaseNotAcquired) {
						r.log.Errorf("Failed to claim unclaimed shard '%v': %v", shardID, err)
					}
					continue
				}
				wg.Add(1)
				if err = r.runConsumer(&wg, shardID, sequence); err != nil {
					r.log.Errorf("Failed to start consumer: %v\n", err)
				}
			}
		} else {
			// No unclaimed shards — attempt to steal from overloaded clients.
			selfClaims := len(clientClaims[r.clientID])
			for clientID, claims := range clientClaims {
				if clientID == r.clientID {
					continue
				}
				if len(claims) > (selfClaims + 1) {
					targetShard := claims[rand.IntN(len(claims))].ShardID
					r.log.Debugf("Attempting to steal shard '%v' from client '%v'", targetShard, clientID)

					sequence, err := r.checkpointer.Claim(r.ctx, r.streamID, targetShard, clientID)
					if err != nil {
						if r.ctx.Err() != nil {
							return
						}
						if !errors.Is(err, ErrLeaseNotAcquired) {
							r.log.Errorf("Failed to steal shard '%v': %v", targetShard, err)
						}
						continue
					}

					wg.Add(1)
					if err = r.runConsumer(&wg, targetShard, sequence); err != nil {
						r.log.Errorf("Failed to start consumer: %v\n", err)
					} else {
						break
					}
				}
			}
		}

		select {
		case <-time.After(r.rebalancePeriod):
		case <-r.ctx.Done():
			return
		}
	}
}

//------------------------------------------------------------------------------

// Connect establishes clients and starts background shard consumers.
func (r *dynamoDBStreamsReader) Connect(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()
	if r.msgChan != nil {
		return nil
	}

	r.streamsSvc = dynamodbstreams.NewFromConfig(r.sess)
	r.dynamodbSvc = dynamodb.NewFromConfig(r.sess)

	streamARN, err := r.resolveStreamARN(ctx)
	if err != nil {
		return err
	}
	r.streamARN = streamARN

	// Use table name as checkpoint key when available — it's stable across
	// stream re-creation (e.g., when streams are disabled and re-enabled on a
	// table, a new ARN is generated but the table name stays the same).
	r.streamID = r.streamARN
	if r.conf.Table != "" {
		r.streamID = r.conf.Table
	}

	checkpointer, err := newDDBSCheckpointer(r.sess, r.clientID, r.conf.Checkpoint, r.leasePeriod, r.commitPeriod)
	if err != nil {
		return err
	}
	r.checkpointer = checkpointer

	// Verify the stream exists and is enabled.
	out, err := r.streamsSvc.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(r.streamARN),
	})
	if err != nil {
		return fmt.Errorf("failed to describe stream: %w", err)
	}
	if out.StreamDescription != nil && out.StreamDescription.StreamStatus == types.StreamStatusDisabled {
		return fmt.Errorf("stream %v is disabled", r.streamARN)
	}

	r.msgChan = make(chan asyncMessage)
	go r.runBalancedShards()

	r.log.Infof("Consuming DynamoDB stream: %v", r.streamARN)
	return nil
}

// ReadBatch attempts to read a message batch from the DynamoDB stream.
func (r *dynamoDBStreamsReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	r.cMut.Lock()
	msgChan := r.msgChan
	r.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// Close gracefully shuts down the DynamoDB Streams input.
func (r *dynamoDBStreamsReader) Close(ctx context.Context) error {
	r.done()
	select {
	case <-r.closedChan:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

//------------------------------------------------------------------------------

// dynamoDBStreamEventToJSON converts a DynamoDB stream record into a structured
// JSON map suitable for message payloads.
func dynamoDBStreamEventToJSON(rec types.Record) map[string]any {
	result := map[string]any{}

	if rec.EventID != nil {
		result["eventID"] = *rec.EventID
	}
	if rec.EventName != "" {
		result["eventName"] = string(rec.EventName)
	}
	if rec.EventVersion != nil {
		result["eventVersion"] = *rec.EventVersion
	}
	if rec.EventSource != nil {
		result["eventSource"] = *rec.EventSource
	}
	if rec.AwsRegion != nil {
		result["awsRegion"] = *rec.AwsRegion
	}

	if rec.Dynamodb != nil {
		dynamo := map[string]any{}

		if rec.Dynamodb.Keys != nil {
			dynamo["keys"] = streamsAttributeMapToJSON(rec.Dynamodb.Keys)
		}
		if rec.Dynamodb.NewImage != nil {
			dynamo["newImage"] = streamsAttributeMapToJSON(rec.Dynamodb.NewImage)
		}
		if rec.Dynamodb.OldImage != nil {
			dynamo["oldImage"] = streamsAttributeMapToJSON(rec.Dynamodb.OldImage)
		}
		if rec.Dynamodb.SequenceNumber != nil {
			dynamo["sequenceNumber"] = *rec.Dynamodb.SequenceNumber
		}
		if rec.Dynamodb.SizeBytes != nil {
			dynamo["sizeBytes"] = *rec.Dynamodb.SizeBytes
		}
		if rec.Dynamodb.StreamViewType != "" {
			dynamo["streamViewType"] = string(rec.Dynamodb.StreamViewType)
		}
		if rec.Dynamodb.ApproximateCreationDateTime != nil {
			dynamo["approximateCreationDateTime"] = rec.Dynamodb.ApproximateCreationDateTime.Format(time.RFC3339)
		}

		result["dynamodb"] = dynamo
	}

	return result
}

// streamsAttributeMapToJSON converts a DynamoDB Streams attribute value map to
// a plain map[string]any representation.
func streamsAttributeMapToJSON(attrs map[string]types.AttributeValue) map[string]any {
	result := make(map[string]any, len(attrs))
	for k, v := range attrs {
		result[k] = streamsAttributeValueToJSON(v)
	}
	return result
}

// streamsAttributeValueToJSON converts a single DynamoDB Streams attribute
// value to its Go native equivalent.
func streamsAttributeValueToJSON(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	case *types.AttributeValueMemberB:
		return v.Value
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		return v.Value
	case *types.AttributeValueMemberBS:
		return v.Value
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			list[i] = streamsAttributeValueToJSON(item)
		}
		return list
	case *types.AttributeValueMemberM:
		return streamsAttributeMapToJSON(v.Value)
	default:
		return nil
	}
}

// extractTableNameFromARN extracts the table name from a DynamoDB stream ARN.
// Format: arn:aws:dynamodb:region:account:table/TABLE_NAME/stream/TIMESTAMP
func extractTableNameFromARN(streamARN string) string {
	parts := strings.Split(streamARN, "/")
	if len(parts) >= 2 {
		return parts[1]
	}
	return streamARN
}
