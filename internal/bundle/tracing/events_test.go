package tracing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/warpstreamlabs/bento/internal/message"
)

func TestFlowIDGeneration(t *testing.T) {
	// Test that flow IDs are generated and consistent
	part := message.NewPart([]byte("test message"))

	// First call should generate a flow ID
	flowID1 := getOrCreateFlowID(part)
	assert.NotEmpty(t, flowID1)
	assert.Regexp(t, `^\d+$`, flowID1) // Should be just a number

	// Second call should return the same flow ID
	flowID2 := getOrCreateFlowID(part)
	assert.Equal(t, flowID1, flowID2)

	// Check that it's stored in metadata
	storedFlowID, exists := part.MetaGetMut("_bento_flow_id")
	assert.True(t, exists)
	assert.Equal(t, flowID1, storedFlowID)
}

func TestFlowIDFromExistingMetadata(t *testing.T) {
	// Test that existing flow ID in metadata is used
	part := message.NewPart([]byte("test message"))
	expectedFlowID := "existing_flow_123"
	part.MetaSetMut("_bento_flow_id", expectedFlowID)

	flowID := getOrCreateFlowID(part)
	assert.Equal(t, expectedFlowID, flowID)
}

func TestEventCreationWithFlowID(t *testing.T) {
	part := message.NewPart([]byte("test content"))
	part.MetaSetMut("test_meta", "test_value")

	// Test produce event
	before := time.Now()
	produceEvent := EventProduceOf(part)
	after := time.Now()

	assert.Equal(t, EventProduce, produceEvent.Type)
	assert.Equal(t, "test content", produceEvent.Content)
	assert.Equal(t, "test_value", produceEvent.Meta["test_meta"])
	assert.NotEmpty(t, produceEvent.FlowID)
	assert.True(t, produceEvent.Timestamp.After(before) || produceEvent.Timestamp.Equal(before))
	assert.True(t, produceEvent.Timestamp.Before(after) || produceEvent.Timestamp.Equal(after))

	// Test consume event with same part should have same flow ID
	consumeEvent := EventConsumeOf(part)
	assert.Equal(t, EventConsume, consumeEvent.Type)
	assert.Equal(t, produceEvent.FlowID, consumeEvent.FlowID)

	// Test delete event with part
	deleteEvent := EventDeleteOfPart(part)
	assert.Equal(t, EventDelete, deleteEvent.Type)
	assert.Equal(t, produceEvent.FlowID, deleteEvent.FlowID)
	assert.Empty(t, deleteEvent.Content)

	// Test error event with part
	testErr := assert.AnError
	errorEvent := EventErrorOfPart(part, testErr)
	assert.Equal(t, EventError, errorEvent.Type)
	assert.Equal(t, testErr.Error(), errorEvent.Content)
	assert.Equal(t, produceEvent.FlowID, errorEvent.FlowID)
}

func TestEventCreationWithoutPart(t *testing.T) {
	// Test delete event without part
	deleteEvent := EventDeleteOf()
	assert.Equal(t, EventDelete, deleteEvent.Type)
	assert.Empty(t, deleteEvent.FlowID)
	assert.Empty(t, deleteEvent.Content)

	// Test error event without part
	testErr := assert.AnError
	errorEvent := EventErrorOf(testErr)
	assert.Equal(t, EventError, errorEvent.Type)
	assert.Equal(t, testErr.Error(), errorEvent.Content)
	assert.Empty(t, errorEvent.FlowID)
}

func TestUniqueFlowIDs(t *testing.T) {
	// Test that different parts get different flow IDs
	part1 := message.NewPart([]byte("message 1"))
	part2 := message.NewPart([]byte("message 2"))

	flowID1 := getOrCreateFlowID(part1)
	flowID2 := getOrCreateFlowID(part2)

	assert.NotEqual(t, flowID1, flowID2)
	assert.NotEmpty(t, flowID1)
	assert.NotEmpty(t, flowID2)
}
