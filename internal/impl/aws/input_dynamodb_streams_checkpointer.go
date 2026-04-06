package aws

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/warpstreamlabs/bento/public/service"
)

type ddbsCheckpointConfig struct {
	Table              string
	Create             bool
	BillingMode        string
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

func ddbsCheckpointConfigFromParsed(pConf *service.ParsedConfig) (conf ddbsCheckpointConfig, err error) {
	if conf.Table, err = pConf.FieldString(ddbsCPFieldTable); err != nil {
		return
	}
	if conf.Create, err = pConf.FieldBool(ddbsCPFieldCreate); err != nil {
		return
	}
	if conf.BillingMode, err = pConf.FieldString(ddbsCPFieldBillingMode); err != nil {
		return
	}
	if conf.ReadCapacityUnits, err = int64Field(pConf, ddbsCPFieldReadCapacityUnits); err != nil {
		return
	}
	if conf.WriteCapacityUnits, err = int64Field(pConf, ddbsCPFieldWriteCapacityUnits); err != nil {
		return
	}
	return
}

const ddbsCompletedClientID = "_COMPLETED_"

// ddbsCheckpointData extends the Kinesis checkpoint data with completed shard
// tracking, which is needed for parent-child shard ordering.
type ddbsCheckpointData struct {
	awsKinesisCheckpointData
	// CompletedShards is a set of shard IDs that have been fully consumed.
	CompletedShards map[string]bool
}

// ddbsCheckpointer manages shard checkpointing for the DynamoDB Streams input.
// It reuses the same DynamoDB table schema as the Kinesis checkpointer
// (StreamID + ShardID) for consistency.
type ddbsCheckpointer struct {
	conf          ddbsCheckpointConfig
	clientID      string
	leaseDuration time.Duration
	commitPeriod  time.Duration
	svc           *dynamodb.Client
}

func newDDBSCheckpointer(
	aConf aws.Config,
	clientID string,
	conf ddbsCheckpointConfig,
	leaseDuration time.Duration,
	commitPeriod time.Duration,
) (*ddbsCheckpointer, error) {
	c := &ddbsCheckpointer{
		conf:          conf,
		leaseDuration: leaseDuration,
		commitPeriod:  commitPeriod,
		svc:           dynamodb.NewFromConfig(aConf),
		clientID:      clientID,
	}

	if err := c.ensureTableExists(context.TODO()); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *ddbsCheckpointer) ensureTableExists(ctx context.Context) error {
	_, err := c.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.conf.Table),
	})
	{
		var aerr *types.ResourceNotFoundException
		if err == nil || !errors.As(err, &aerr) {
			return err
		}
	}
	if !c.conf.Create {
		return fmt.Errorf("checkpoint table %v does not exist", c.conf.Table)
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("StreamID"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("ShardID"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingMode(c.conf.BillingMode),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("StreamID"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("ShardID"), KeyType: types.KeyTypeRange},
		},
		TableName: aws.String(c.conf.Table),
	}
	if c.conf.BillingMode == "PROVISIONED" {
		input.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  &c.conf.ReadCapacityUnits,
			WriteCapacityUnits: &c.conf.WriteCapacityUnits,
		}
	}
	if _, err = c.svc.CreateTable(ctx, input); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}
	return nil
}

// GetCheckpointsAndClaims retrieves all checkpoint data for a stream, including
// which shards have been marked as completed (fully consumed).
func (c *ddbsCheckpointer) GetCheckpointsAndClaims(ctx context.Context, streamID string) (*ddbsCheckpointData, error) {
	result := &ddbsCheckpointData{
		awsKinesisCheckpointData: awsKinesisCheckpointData{
			ShardsWithCheckpoints: make(map[string]bool),
			ClientClaims:          make(map[string][]awsKinesisClientClaim),
		},
		CompletedShards: make(map[string]bool),
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(c.conf.Table),
		KeyConditionExpression: aws.String("StreamID = :stream_id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":stream_id": &types.AttributeValueMemberS{
				Value: streamID,
			},
		},
	}

	paginator := dynamodb.NewQueryPaginator(c.svc, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to query checkpoints: %w", err)
		}

		for _, item := range page.Items {
			var shardID string
			if s, ok := item["ShardID"].(*types.AttributeValueMemberS); ok {
				shardID = s.Value
			}
			if shardID == "" {
				continue
			}

			result.ShardsWithCheckpoints[shardID] = true

			var clientID string
			if s, ok := item["ClientID"].(*types.AttributeValueMemberS); ok {
				clientID = s.Value
			}
			if clientID == "" {
				continue
			}

			// Shards marked with the completed sentinel are fully consumed.
			if clientID == ddbsCompletedClientID {
				result.CompletedShards[shardID] = true
				continue
			}

			var claim awsKinesisClientClaim
			claim.ShardID = shardID

			if s, ok := item["LeaseTimeout"].(*types.AttributeValueMemberS); ok {
				var parseErr error
				if claim.LeaseTimeout, parseErr = time.Parse(time.RFC3339Nano, s.Value); parseErr != nil {
					return nil, fmt.Errorf("failed to parse claim lease for shard %s: %w", shardID, parseErr)
				}
			}
			if claim.LeaseTimeout.IsZero() {
				return nil, fmt.Errorf("failed to extract lease timeout from claim for shard %s", shardID)
			}

			result.ClientClaims[clientID] = append(result.ClientClaims[clientID], claim)
		}
	}

	return result, nil
}

// Claim attempts to claim a shard. If fromClientID is specified the shard is
// stolen from that particular client.
func (c *ddbsCheckpointer) Claim(ctx context.Context, streamID, shardID, fromClientID string) (string, error) {
	newLeaseTimeoutString := time.Now().Add(c.leaseDuration).Format(time.RFC3339Nano)

	var conditionalExpression string
	expressionAttributeValues := map[string]types.AttributeValue{
		":new_client_id": &types.AttributeValueMemberS{
			Value: c.clientID,
		},
		":new_lease_timeout": &types.AttributeValueMemberS{
			Value: newLeaseTimeoutString,
		},
	}

	if fromClientID != "" {
		conditionalExpression = "ClientID = :old_client_id"
		expressionAttributeValues[":old_client_id"] = &types.AttributeValueMemberS{
			Value: fromClientID,
		}
	} else {
		conditionalExpression = "attribute_not_exists(ClientID)"
	}

	exp := "SET ClientID = :new_client_id, LeaseTimeout = :new_lease_timeout"
	res, err := c.svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		ReturnValues:              types.ReturnValueAllOld,
		TableName:                 &c.conf.Table,
		ConditionExpression:       &conditionalExpression,
		UpdateExpression:          &exp,
		ExpressionAttributeValues: expressionAttributeValues,
		Key: map[string]types.AttributeValue{
			"StreamID": &types.AttributeValueMemberS{Value: streamID},
			"ShardID":  &types.AttributeValueMemberS{Value: shardID},
		},
	})
	if err != nil {
		var aerr *types.ConditionalCheckFailedException
		if errors.As(err, &aerr) {
			return "", ErrLeaseNotAcquired
		}
		return "", err
	}

	var startingSequence string
	if s, ok := res.Attributes["SequenceNumber"].(*types.AttributeValueMemberS); ok {
		startingSequence = s.Value
	}

	var currentLease time.Time
	if s, ok := res.Attributes["LeaseTimeout"].(*types.AttributeValueMemberS); ok {
		currentLease, _ = time.Parse(time.RFC3339Nano, s.Value)
	}

	// Wait a grace period when stealing from another client.
	if fromClientID != "" && time.Since(currentLease) < c.leaseDuration {
		waitFor := c.leaseDuration - time.Since(currentLease) + time.Second
		select {
		case <-time.After(waitFor):
		case <-ctx.Done():
			return "", ctx.Err()
		}

		cp, err := c.getCheckpoint(ctx, streamID, shardID)
		if err != nil {
			return "", err
		}
		if cp != nil {
			startingSequence = cp.SequenceNumber
		}
	}

	return startingSequence, nil
}

func (c *ddbsCheckpointer) getCheckpoint(ctx context.Context, streamID, shardID string) (*awsKinesisCheckpoint, error) {
	rawItem, err := c.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.conf.Table),
		Key: map[string]types.AttributeValue{
			"ShardID":  &types.AttributeValueMemberS{Value: shardID},
			"StreamID": &types.AttributeValueMemberS{Value: streamID},
		},
	})
	if err != nil {
		var aerr *types.ResourceNotFoundException
		if errors.As(err, &aerr) {
			return nil, nil
		}
		return nil, err
	}

	cp := awsKinesisCheckpoint{}
	if s, ok := rawItem.Item["SequenceNumber"].(*types.AttributeValueMemberS); ok {
		cp.SequenceNumber = s.Value
	} else {
		return nil, errors.New("sequence ID was not found in checkpoint")
	}
	if s, ok := rawItem.Item["ClientID"].(*types.AttributeValueMemberS); ok {
		cp.ClientID = &s.Value
	}
	if s, ok := rawItem.Item["LeaseTimeout"].(*types.AttributeValueMemberS); ok {
		timeout, err := time.Parse(time.RFC3339Nano, s.Value)
		if err != nil {
			return nil, err
		}
		cp.LeaseTimeout = &timeout
	}

	return &cp, nil
}

// Checkpoint sets a sequence number for a shard. Returns whether the shard is
// still owned by this client.
func (c *ddbsCheckpointer) Checkpoint(ctx context.Context, streamID, shardID, sequenceNumber string, final bool) (bool, error) {
	item := map[string]types.AttributeValue{
		"StreamID": &types.AttributeValueMemberS{Value: streamID},
		"ShardID":  &types.AttributeValueMemberS{Value: shardID},
	}

	if sequenceNumber != "" {
		item["SequenceNumber"] = &types.AttributeValueMemberS{Value: sequenceNumber}
	}

	if !final {
		item["ClientID"] = &types.AttributeValueMemberS{Value: c.clientID}
		item["LeaseTimeout"] = &types.AttributeValueMemberS{
			Value: time.Now().Add(c.leaseDuration).Format(time.RFC3339Nano),
		}
	}

	if _, err := c.svc.PutItem(ctx, &dynamodb.PutItemInput{
		ConditionExpression: aws.String("ClientID = :client_id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":client_id": &types.AttributeValueMemberS{Value: c.clientID},
		},
		TableName: aws.String(c.conf.Table),
		Item:      item,
	}); err != nil {
		var aerr *types.ConditionalCheckFailedException
		if errors.As(err, &aerr) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Yield updates a checkpoint's sequence number without changing ownership.
func (c *ddbsCheckpointer) Yield(ctx context.Context, streamID, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return nil
	}

	_, err := c.svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(c.conf.Table),
		Key: map[string]types.AttributeValue{
			"StreamID": &types.AttributeValueMemberS{Value: streamID},
			"ShardID":  &types.AttributeValueMemberS{Value: shardID},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":new_sequence_number": &types.AttributeValueMemberS{Value: sequenceNumber},
		},
		UpdateExpression: aws.String("SET SequenceNumber = :new_sequence_number"),
	})
	return err
}

// Complete marks a shard as fully consumed. The checkpoint entry is retained
// (rather than deleted) so that child shards can verify their parent completed
// before starting consumption, preserving per-key ordering across shard splits.
func (c *ddbsCheckpointer) Complete(ctx context.Context, streamID, shardID, sequenceNumber string) error {
	item := map[string]types.AttributeValue{
		"StreamID": &types.AttributeValueMemberS{Value: streamID},
		"ShardID":  &types.AttributeValueMemberS{Value: shardID},
		"ClientID": &types.AttributeValueMemberS{Value: ddbsCompletedClientID},
	}
	if sequenceNumber != "" {
		item["SequenceNumber"] = &types.AttributeValueMemberS{Value: sequenceNumber}
	}
	_, err := c.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.conf.Table),
		Item:      item,
	})
	return err
}
