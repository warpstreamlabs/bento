package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/warpstreamlabs/bento/public/service"
)

type ddbsRecordBatcher struct {
	streamARN string
	shardID   string

	batchPolicy  *service.Batcher
	checkpointer *checkpoint.Capped[string]

	flushedMessage service.MessageBatch

	batchedSequence string

	ackedSequence string
	ackedMut      sync.Mutex
	ackedWG       sync.WaitGroup
}

func (r *dynamoDBStreamsReader) newDDBSRecordBatcher(shardID, sequence string) (*ddbsRecordBatcher, error) {
	batchPolicy, err := r.batcher.NewBatcher(r.mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize batch policy for shard consumer: %w", err)
	}

	return &ddbsRecordBatcher{
		streamARN:     r.streamARN,
		shardID:       shardID,
		batchPolicy:   batchPolicy,
		checkpointer:  checkpoint.NewCapped[string](int64(r.conf.CheckpointLimit)),
		ackedSequence: sequence,
	}, nil
}

// AddRecord converts a DynamoDB Streams record into a message and adds it to
// the current batch. Returns true if the batch is ready to flush.
func (b *ddbsRecordBatcher) AddRecord(rec types.Record) bool {
	eventJSON := dynamoDBStreamEventToJSON(rec)

	data, err := json.Marshal(eventJSON)
	if err != nil {
		// Shouldn't happen for well-formed events, but handle defensively.
		data = []byte("{}")
	}

	p := service.NewMessage(data)

	// Set metadata for downstream processors/outputs.
	tableName := extractTableNameFromARN(b.streamARN)
	p.MetaSetMut("dynamodb_table", tableName)
	p.MetaSetMut("dynamodb_stream_arn", b.streamARN)
	p.MetaSetMut("dynamodb_shard_id", b.shardID)

	if rec.EventID != nil {
		p.MetaSetMut("dynamodb_event_id", *rec.EventID)
	}
	if rec.EventName != "" {
		p.MetaSetMut("dynamodb_event_name", string(rec.EventName))
	}
	if rec.Dynamodb != nil && rec.Dynamodb.SequenceNumber != nil {
		p.MetaSetMut("dynamodb_sequence_number", *rec.Dynamodb.SequenceNumber)
		b.batchedSequence = *rec.Dynamodb.SequenceNumber
	}

	if b.flushedMessage != nil {
		b.flushedMessage = append(b.flushedMessage, p)
		return true
	}
	return b.batchPolicy.Add(p)
}

func (b *ddbsRecordBatcher) HasPendingMessage() bool {
	return b.flushedMessage != nil
}

func (b *ddbsRecordBatcher) FlushMessage(ctx context.Context) (asyncMessage, error) {
	if b.flushedMessage == nil {
		var err error
		if b.flushedMessage, err = b.batchPolicy.Flush(ctx); err != nil || b.flushedMessage == nil {
			return asyncMessage{}, err
		}
	}

	resolveFn, err := b.checkpointer.Track(ctx, b.batchedSequence, int64(len(b.flushedMessage)))
	if err != nil {
		if ctx.Err() != nil {
			err = nil
		}
		return asyncMessage{}, err
	}

	b.ackedWG.Add(1)
	aMsg := asyncMessage{
		msg: b.flushedMessage,
		ackFn: func(ctx context.Context, res error) error {
			topSequence := resolveFn()
			if topSequence != nil {
				b.ackedMut.Lock()
				b.ackedSequence = *topSequence
				b.ackedMut.Unlock()
			}
			b.ackedWG.Done()
			return res
		},
	}
	b.flushedMessage = nil
	return aMsg, nil
}

func (b *ddbsRecordBatcher) UntilNext() (time.Duration, bool) {
	return b.batchPolicy.UntilNext()
}

func (b *ddbsRecordBatcher) GetSequence() string {
	b.ackedMut.Lock()
	seq := b.ackedSequence
	b.ackedMut.Unlock()
	return seq
}

func (b *ddbsRecordBatcher) Close(ctx context.Context, blocked bool) {
	if blocked {
		b.ackedWG.Wait()
	}
	_ = b.batchPolicy.Close(ctx)
}
