package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestFlowIDInNonTracedBuild(t *testing.T) {
	config := `
input:
  generate:
    count: 3
    interval: 1us
    mapping: |
      root.id = count("test_flow")
      meta captured_flow_id = flow_id()

pipeline:
  threads: 1
  processors:
    - bloblang: |
        root.message = "processed"
        root.original_id = this.id
        meta flow_check = flow_id()

output:
  drop: {}

logger:
  level: OFF
`

	strmBuilder := service.NewStreamBuilder()
	require.NoError(t, strmBuilder.SetYAML(config))

	strm, err := strmBuilder.Build()
	require.NoError(t, err)

	require.NoError(t, strm.Run(context.Background()))
}

func TestFlowIDConsistencyInTracedBuild(t *testing.T) {
	config := `
input:
  generate:
    count: 2
    interval: 1us
    mapping: |
      root.id = count("consistency_test")
      meta flow_at_input = flow_id()

pipeline:
  threads: 1
  processors:
    - bloblang: |
        root.message = "step1"
        root.original_id = this.id
        meta flow_at_proc1 = flow_id()
    - bloblang: |
        root.message = "step2"
        meta flow_at_proc2 = flow_id()

output:
  drop: {}

logger:
  level: OFF
`

	strmBuilder := service.NewStreamBuilder()
	require.NoError(t, strmBuilder.SetYAML(config))

	strm, trace, err := strmBuilder.BuildTracedV2()
	require.NoError(t, err)

	require.NoError(t, strm.Run(context.Background()))

	assert.Equal(t, 2, int(trace.TotalInput()))
	assert.Equal(t, 2, int(trace.TotalOutput()))

	inputEvents := trace.InputEvents(false)
	require.Contains(t, inputEvents, "root.input")

	for _, event := range inputEvents["root.input"] {
		assert.NotEmpty(t, event.FlowID, "Input event should have flow ID")
		assert.False(t, event.Timestamp.IsZero(), "Input event should have timestamp")
	}

	processorEvents := trace.ProcessorEvents(false)
	for procName, events := range processorEvents {
		for _, event := range events {
			assert.NotEmpty(t, event.FlowID, "Processor %s event should have flow ID", procName)
			assert.False(t, event.Timestamp.IsZero(), "Processor %s event should have timestamp", procName)
		}
	}

	outputEvents := trace.OutputEvents(false)
	require.Contains(t, outputEvents, "root.output")

	for _, event := range outputEvents["root.output"] {
		assert.NotEmpty(t, event.FlowID, "Output event should have flow ID")
		assert.False(t, event.Timestamp.IsZero(), "Output event should have timestamp")
	}
}

func TestFlowIDsByFlowID(t *testing.T) {
	config := `
input:
  generate:
    count: 3
    interval: 1us
    mapping: |
      root.id = count("flow_grouping_test")

pipeline:
  threads: 1
  processors:
    - bloblang: |
        root.processed = true

output:
  drop: {}

logger:
  level: OFF
`

	strmBuilder := service.NewStreamBuilder()
	require.NoError(t, strmBuilder.SetYAML(config))

	strm, trace, err := strmBuilder.BuildTracedV2()
	require.NoError(t, err)

	require.NoError(t, strm.Run(context.Background()))

	flowEvents := trace.EventsByFlowID(false)

	assert.NotEmpty(t, flowEvents, "Should have events grouped by flow ID")

	for flowID, events := range flowEvents {
		assert.NotEmpty(t, flowID, "Flow ID should not be empty")
		assert.NotEmpty(t, events, "Should have events for flow %s", flowID)

		var hasInput, hasOutput bool
		for _, event := range events {
			assert.Equal(t, flowID, event.FlowID, "All events should have same flow ID")
			if event.Type == service.TracingEventProduce {
				hasInput = true
			}
			if event.Type == service.TracingEventConsume {
				hasOutput = true
			}
		}

		assert.True(t, hasInput, "Flow %s should have input events", flowID)
		assert.True(t, hasOutput, "Flow %s should have output events", flowID)

		for i := 1; i < len(events); i++ {
			assert.True(t,
				events[i-1].Timestamp.Before(events[i].Timestamp) ||
					events[i-1].Timestamp.Equal(events[i].Timestamp),
				"Events should be sorted by timestamp for flow %s", flowID)
		}
	}

	assert.Len(t, flowEvents, 3, "Should have 3 distinct flows for 3 messages")
}
