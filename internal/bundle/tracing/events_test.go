package tracing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/tracing"
)

func TestFlowIDGeneration(t *testing.T) {
	part := message.NewPart([]byte("test message"))

	part = tracing.EnsureFlowID(part)
	flowID1 := getFlowID(part)
	assert.NotEmpty(t, flowID1)
	// UUID V7 format: xxxxxxxx-xxxx-7xxx-xxxx-xxxxxxxxxxxx
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, flowID1)

	flowID2 := getFlowID(part)
	assert.Equal(t, flowID1, flowID2)

	ctx := message.GetContext(part)
	storedFlowID := tracing.GetFlowID(ctx)
	assert.NotEmpty(t, storedFlowID)
	assert.Equal(t, flowID1, storedFlowID)
}

func TestFlowIDFromExistingContext(t *testing.T) {
	part := message.NewPart([]byte("test message"))
	expectedFlowID := "existing_flow_123"
	ctx := tracing.WithFlowID(message.GetContext(part), expectedFlowID)
	part = part.WithContext(ctx)

	flowID := getFlowID(part)
	assert.Equal(t, expectedFlowID, flowID)
}

func TestEnsureFlowIDWithExisting(t *testing.T) {
	part := message.NewPart([]byte("test message"))
	expectedFlowID := "existing_flow_123"
	ctx := tracing.WithFlowID(message.GetContext(part), expectedFlowID)
	part = part.WithContext(ctx)

	part = tracing.EnsureFlowID(part)
	flowID := getFlowID(part)
	assert.Equal(t, expectedFlowID, flowID)
}

func TestEventCreationWithFlowID(t *testing.T) {
	part := message.NewPart([]byte("test content"))
	part.MetaSetMut("test_meta", "test_value")
	part = tracing.EnsureFlowID(part)

	before := time.Now()
	produceEvent := EventProduceOf(part)
	after := time.Now()

	assert.Equal(t, EventProduce, produceEvent.Type)
	assert.Equal(t, "test content", produceEvent.Content)
	assert.Equal(t, "test_value", produceEvent.Meta["test_meta"])
	assert.NotEmpty(t, produceEvent.FlowID)
	assert.True(t, produceEvent.Timestamp.After(before) || produceEvent.Timestamp.Equal(before))
	assert.True(t, produceEvent.Timestamp.Before(after) || produceEvent.Timestamp.Equal(after))

	consumeEvent := EventConsumeOf(part)
	assert.Equal(t, EventConsume, consumeEvent.Type)
	assert.Equal(t, produceEvent.FlowID, consumeEvent.FlowID)

	deleteEvent := EventDeleteOfPart(part)
	assert.Equal(t, EventDelete, deleteEvent.Type)
	assert.Equal(t, produceEvent.FlowID, deleteEvent.FlowID)
	assert.Empty(t, deleteEvent.Content)

	testErr := assert.AnError
	errorEvent := EventErrorOfPart(part, testErr)
	assert.Equal(t, EventError, errorEvent.Type)
	assert.Equal(t, testErr.Error(), errorEvent.Content)
	assert.Equal(t, produceEvent.FlowID, errorEvent.FlowID)
}

// TestEventCreationWithoutPart tests deprecated functions that create events without flow IDs.
// These functions are kept for backward compatibility.
func TestEventCreationWithoutPart(t *testing.T) {
	deleteEvent := EventDeleteOf() // Deprecated: use EventDeleteOfPart
	assert.Equal(t, EventDelete, deleteEvent.Type)
	assert.Empty(t, deleteEvent.FlowID)
	assert.Empty(t, deleteEvent.Content)

	testErr := assert.AnError
	errorEvent := EventErrorOf(testErr) // Deprecated: use EventErrorOfPart
	assert.Equal(t, EventError, errorEvent.Type)
	assert.Equal(t, testErr.Error(), errorEvent.Content)
	assert.Empty(t, errorEvent.FlowID)
}

func TestUniqueFlowIDs(t *testing.T) {
	part1 := message.NewPart([]byte("message 1"))
	part2 := message.NewPart([]byte("message 2"))

	part1 = tracing.EnsureFlowID(part1)
	part2 = tracing.EnsureFlowID(part2)

	flowID1 := getFlowID(part1)
	flowID2 := getFlowID(part2)

	assert.NotEqual(t, flowID1, flowID2)
	assert.NotEmpty(t, flowID1)
	assert.NotEmpty(t, flowID2)
}
