package service

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/warpstreamlabs/bento/internal/bundle/tracing"
)

// TracingEventType describes the type of tracing event a component might
// experience during a config run.
//
// Experimental: This type may change outside of major version releases.
type TracingEventType string

// Various tracing event types.
//
// Experimental: This type may change outside of major version releases.
var (
	// Note: must match up with ./internal/bundle/tracing/events.go.
	TracingEventProduce TracingEventType = "PRODUCE"
	TracingEventConsume TracingEventType = "CONSUME"
	TracingEventDelete  TracingEventType = "DELETE"
	TracingEventError   TracingEventType = "ERROR"
	TracingEventUnknown TracingEventType = "UNKNOWN"
)

func convertTracingEventType(t tracing.EventType) TracingEventType {
	switch t {
	case tracing.EventProduce:
		return TracingEventProduce
	case tracing.EventConsume:
		return TracingEventConsume
	case tracing.EventDelete:
		return TracingEventDelete
	case tracing.EventError:
		return TracingEventError
	}
	return TracingEventUnknown
}

// TracingEvent represents a single event that occurred within the stream.
//
// Experimental: This type may change outside of major version releases.
type TracingEvent struct {
	Type      TracingEventType
	Content   string
	Meta      map[string]any
	FlowID    string
	Timestamp time.Time
}

// TracingSummary is a high level description of all traced events. When tracing
// a stream this should only be queried once the stream has ended.
//
// Experimental: This type may change outside of major version releases.
type TracingSummary struct {
	summary *tracing.Summary
}

// TotalInput returns the total traced input messages received.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) TotalInput() uint64 {
	return atomic.LoadUint64(&s.summary.Input)
}

// TotalProcessorErrors returns the total traced processor errors occurred.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) TotalProcessorErrors() uint64 {
	return atomic.LoadUint64(&s.summary.ProcessorErrors)
}

// TotalOutput returns the total traced output messages received.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) TotalOutput() uint64 {
	return atomic.LoadUint64(&s.summary.Output)
}

// InputEvents returns a map of input labels to events traced during the
// execution of a stream pipeline.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) InputEvents(flush bool) map[string][]TracingEvent {
	m := map[string][]TracingEvent{}
	for k, v := range s.summary.InputEvents(flush) {
		events := make([]TracingEvent, len(v))
		for i, e := range v {
			events[i] = TracingEvent{
				Type:      convertTracingEventType(e.Type),
				Content:   e.Content,
				Meta:      e.Meta,
				FlowID:    e.FlowID,
				Timestamp: e.Timestamp,
			}
		}
		m[k] = events
	}
	return m
}

// ProcessorEvents returns a map of processor labels to events traced during the
// execution of a stream pipeline.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) ProcessorEvents(flush bool) map[string][]TracingEvent {
	m := map[string][]TracingEvent{}
	for k, v := range s.summary.ProcessorEvents(flush) {
		events := make([]TracingEvent, len(v))
		for i, e := range v {
			events[i] = TracingEvent{
				Type:      convertTracingEventType(e.Type),
				Content:   e.Content,
				Meta:      e.Meta,
				FlowID:    e.FlowID,
				Timestamp: e.Timestamp,
			}
		}
		m[k] = events
	}
	return m
}

// OutputEvents returns a map of output labels to events traced during the
// execution of a stream pipeline.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) OutputEvents(flush bool) map[string][]TracingEvent {
	m := map[string][]TracingEvent{}
	for k, v := range s.summary.OutputEvents(flush) {
		events := make([]TracingEvent, len(v))
		for i, e := range v {
			events[i] = TracingEvent{
				Type:      convertTracingEventType(e.Type),
				Content:   e.Content,
				Meta:      e.Meta,
				FlowID:    e.FlowID,
				Timestamp: e.Timestamp,
			}
		}
		m[k] = events
	}
	return m
}

// EventsByFlowID returns all events grouped by flow ID, allowing you to trace
// the complete journey of each message through the pipeline. Events are sorted
// by timestamp within each flow.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) EventsByFlowID(flush bool) map[string][]TracingEvent {
	flowEvents := map[string][]TracingEvent{}

	// Collect events from all sources
	for _, events := range s.InputEvents(flush) {
		for _, event := range events {
			if event.FlowID != "" {
				flowEvents[event.FlowID] = append(flowEvents[event.FlowID], event)
			}
		}
	}

	for _, events := range s.ProcessorEvents(flush) {
		for _, event := range events {
			if event.FlowID != "" {
				flowEvents[event.FlowID] = append(flowEvents[event.FlowID], event)
			}
		}
	}

	for _, events := range s.OutputEvents(flush) {
		for _, event := range events {
			if event.FlowID != "" {
				flowEvents[event.FlowID] = append(flowEvents[event.FlowID], event)
			}
		}
	}

	// Sort events by timestamp within each flow
	for flowID, events := range flowEvents {
		sort.Slice(events, func(i, j int) bool {
			return events[i].Timestamp.Before(events[j].Timestamp)
		})
		flowEvents[flowID] = events
	}

	return flowEvents
}
