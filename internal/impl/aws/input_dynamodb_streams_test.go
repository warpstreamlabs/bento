package aws

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractTableNameFromARN(t *testing.T) {
	tests := []struct {
		name     string
		arn      string
		expected string
	}{
		{
			name:     "standard stream ARN",
			arn:      "arn:aws:dynamodb:us-east-1:123456789012:table/my-table/stream/2024-03-20T18:19:47.921",
			expected: "my-table",
		},
		{
			name:     "table with hyphens",
			arn:      "arn:aws:dynamodb:eu-west-1:999999999999:table/events-prod-v2/stream/2025-01-01T00:00:00.000",
			expected: "events-prod-v2",
		},
		{
			name:     "no slashes fallback",
			arn:      "my-table",
			expected: "my-table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractTableNameFromARN(tt.arn)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsDDBShardOpen(t *testing.T) {
	endSeq := "12345"
	tests := []struct {
		name     string
		shard    types.Shard
		expected bool
	}{
		{
			name:     "nil sequence range",
			shard:    types.Shard{},
			expected: true,
		},
		{
			name: "no ending sequence",
			shard: types.Shard{
				SequenceNumberRange: &types.SequenceNumberRange{
					StartingSequenceNumber: &endSeq,
				},
			},
			expected: true,
		},
		{
			name: "has ending sequence (closed)",
			shard: types.Shard{
				SequenceNumberRange: &types.SequenceNumberRange{
					StartingSequenceNumber: &endSeq,
					EndingSequenceNumber:   &endSeq,
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isDDBShardOpen(tt.shard))
		})
	}
}

func TestDynamoDBStreamEventToJSON(t *testing.T) {
	eventID := "event-123"
	eventVersion := "1.1"
	eventSource := "aws:dynamodb"
	awsRegion := "us-east-1"
	seqNum := "111111111111111111111111"
	sizeBytes := int64(256)
	creationTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	rec := types.Record{
		EventID:      &eventID,
		EventName:    types.OperationTypeInsert,
		EventVersion: &eventVersion,
		EventSource:  &eventSource,
		AwsRegion:    &awsRegion,
		Dynamodb: &types.StreamRecord{
			Keys: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#123"},
				"SK": &types.AttributeValueMemberS{Value: "PROFILE"},
			},
			NewImage: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "USER#123"},
				"SK":   &types.AttributeValueMemberS{Value: "PROFILE"},
				"Name": &types.AttributeValueMemberS{Value: "Alice"},
				"Age":  &types.AttributeValueMemberN{Value: "30"},
			},
			SequenceNumber:             &seqNum,
			SizeBytes:                  &sizeBytes,
			StreamViewType:             types.StreamViewTypeNewAndOldImages,
			ApproximateCreationDateTime: &creationTime,
		},
	}

	result := dynamoDBStreamEventToJSON(rec)

	assert.Equal(t, "event-123", result["eventID"])
	assert.Equal(t, "INSERT", result["eventName"])
	assert.Equal(t, "1.1", result["eventVersion"])
	assert.Equal(t, "aws:dynamodb", result["eventSource"])
	assert.Equal(t, "us-east-1", result["awsRegion"])

	dynamo, ok := result["dynamodb"].(map[string]any)
	require.True(t, ok, "dynamodb field should be a map")

	assert.Equal(t, seqNum, dynamo["sequenceNumber"])
	assert.Equal(t, int64(256), dynamo["sizeBytes"])
	assert.Equal(t, "NEW_AND_OLD_IMAGES", dynamo["streamViewType"])
	assert.Equal(t, "2025-01-15T10:30:00Z", dynamo["approximateCreationDateTime"])

	keys, ok := dynamo["keys"].(map[string]any)
	require.True(t, ok, "keys should be a map")
	assert.Equal(t, "USER#123", keys["PK"])
	assert.Equal(t, "PROFILE", keys["SK"])

	newImage, ok := dynamo["newImage"].(map[string]any)
	require.True(t, ok, "newImage should be a map")
	assert.Equal(t, "Alice", newImage["Name"])
	assert.Equal(t, "30", newImage["Age"])

	assert.Nil(t, dynamo["oldImage"], "oldImage should not be present for INSERT")
}

func TestAttributeValueToJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    types.AttributeValue
		expected any
	}{
		{
			name:     "string value",
			input:    &types.AttributeValueMemberS{Value: "hello"},
			expected: "hello",
		},
		{
			name:     "number value",
			input:    &types.AttributeValueMemberN{Value: "42"},
			expected: "42",
		},
		{
			name:     "bool value",
			input:    &types.AttributeValueMemberBOOL{Value: true},
			expected: true,
		},
		{
			name:     "null value",
			input:    &types.AttributeValueMemberNULL{Value: true},
			expected: nil,
		},
		{
			name:     "string set",
			input:    &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
			expected: []string{"a", "b"},
		},
		{
			name:     "number set",
			input:    &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
			expected: []string{"1", "2"},
		},
		{
			name: "list value",
			input: &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "item1"},
				&types.AttributeValueMemberN{Value: "99"},
			}},
			expected: []any{"item1", "99"},
		},
		{
			name: "map value",
			input: &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"nested": &types.AttributeValueMemberS{Value: "val"},
			}},
			expected: map[string]any{"nested": "val"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := streamsAttributeValueToJSON(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDynamoDBStreamsInputSpec(t *testing.T) {
	spec := dynamoDBStreamsInputSpec()
	require.NotNil(t, spec)
}
