package tracing

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/tracing"
)

// EventType describes the type of event a component might experience during
// a config run.
type EventType string

// Various event types.
var (
	EventProduce EventType = "PRODUCE"
	EventConsume EventType = "CONSUME"
	EventDelete  EventType = "DELETE"
	EventError   EventType = "ERROR"
)

// NodeEvent represents a single event that occurred within the stream.
type NodeEvent struct {
	Type      EventType
	Content   string
	Meta      map[string]any
	FlowID    string    // Unique identifier for the message flow journey
	Timestamp time.Time // When the event occurred
}

// EventProduceOf creates a produce event from a message part.
func EventProduceOf(part *message.Part) NodeEvent {
	meta := map[string]any{}
	_ = part.MetaIterMut(func(s string, a any) error {
		meta[s] = message.CopyJSON(a)
		return nil
	})

	return NodeEvent{
		Type:      EventProduce,
		Content:   string(part.AsBytes()),
		Meta:      meta,
		FlowID:    getOrCreateFlowID(part),
		Timestamp: time.Now(),
	}
}

// EventConsumeOf creates a consumed event from a message part.
func EventConsumeOf(part *message.Part) NodeEvent {
	meta := map[string]any{}
	_ = part.MetaIterMut(func(s string, a any) error {
		meta[s] = message.CopyJSON(a)
		return nil
	})

	return NodeEvent{
		Type:      EventConsume,
		Content:   string(part.AsBytes()),
		Meta:      meta,
		FlowID:    getOrCreateFlowID(part),
		Timestamp: time.Now(),
	}
}

// EventDeleteOf creates a deleted event from a message part.
func EventDeleteOf() NodeEvent {
	return NodeEvent{
		Type:      EventDelete,
		FlowID:    "", // No part available, so no flow ID
		Timestamp: time.Now(),
	}
}

// EventDeleteOfPart creates a deleted event from a message part with flow ID.
func EventDeleteOfPart(part *message.Part) NodeEvent {
	return NodeEvent{
		Type:      EventDelete,
		FlowID:    getOrCreateFlowID(part),
		Timestamp: time.Now(),
	}
}

// EventErrorOf creates an error event from a message part.
func EventErrorOf(err error) NodeEvent {
	return NodeEvent{
		Type:      EventError,
		Content:   err.Error(),
		FlowID:    "", // No part available, so no flow ID
		Timestamp: time.Now(),
	}
}

// EventErrorOfPart creates an error event from a message part with flow ID.
func EventErrorOfPart(part *message.Part, err error) NodeEvent {
	return NodeEvent{
		Type:      EventError,
		Content:   err.Error(),
		FlowID:    getOrCreateFlowID(part),
		Timestamp: time.Now(),
	}
}

// getOrCreateFlowID retrieves or creates a flow ID for a message part.
// It first tries to get an existing flow ID from metadata, then from OpenTelemetry trace ID,
// and finally generates a new one if neither exists.
func getOrCreateFlowID(part *message.Part) string {
	// First, check if we already have a flow ID in metadata
	if flowID, exists := part.MetaGetMut("_bento_flow_id"); exists {
		if flowIDStr, ok := flowID.(string); ok && flowIDStr != "" {
			return flowIDStr
		}
	}

	// Try to use OpenTelemetry trace ID if available
	if traceID := tracing.GetTraceID(part); traceID != "" && traceID != "00000000000000000000000000000000" {
		// Store it in metadata for future use
		part.MetaSetMut("_bento_flow_id", traceID)
		return traceID
	}

	// Generate a new flow ID using atomic counter for simplicity and performance
	flowID := generateFlowID()
	part.MetaSetMut("_bento_flow_id", flowID)
	return flowID
}

// Flow ID counter for generating unique IDs
var flowIDCounter uint64

// generateFlowID creates a new unique flow ID using an atomic counter
func generateFlowID() string {
	id := atomic.AddUint64(&flowIDCounter, 1)
	return strconv.FormatUint(id, 10)
}

type control struct {
	isEnabled  int32
	eventLimit int64
}

func (c *control) SetEnabled(e bool) {
	if e {
		atomic.StoreInt32(&c.isEnabled, 1)
	} else {
		atomic.StoreInt32(&c.isEnabled, 0)
	}
}

func (c *control) SetEventLimit(n int64) {
	atomic.StoreInt64(&c.eventLimit, n)
}

func (c *control) IsEnabled() bool {
	return atomic.LoadInt32(&c.isEnabled) > 0
}

func (c *control) EventLimit() int64 {
	return atomic.LoadInt64(&c.eventLimit)
}

// Summary is a high level description of all traced events.
type Summary struct {
	Input           uint64
	Output          uint64
	ProcessorErrors uint64

	ctrl *control

	inputEvents     sync.Map
	processorEvents sync.Map
	outputEvents    sync.Map
}

// NewSummary creates a new tracing summary that can be passed to component
// constructors for adding traces.
func NewSummary() *Summary {
	return &Summary{
		ctrl: &control{isEnabled: 1},
	}
}

// SetEnabled sets whether tracing events are enabled across the stream.
func (s *Summary) SetEnabled(e bool) {
	s.ctrl.SetEnabled(e)
}

// SetEventLimit sets a limit as to how many event traces are stored, this limit
// is per component that's traced.
func (s *Summary) SetEventLimit(n int64) {
	s.ctrl.SetEventLimit(n)
}

func getEvents(flush bool, from *sync.Map) map[string][]NodeEvent {
	m := map[string][]NodeEvent{}
	from.Range(func(key, value any) bool {
		e := value.(*events)
		var extracted []NodeEvent
		if flush {
			extracted = e.Flush()
		} else {
			extracted = e.Extract()
		}
		m[key.(string)] = extracted
		return true
	})
	return m
}

// InputEvents returns a map of input labels to events traced during the
// execution of a stream pipeline. Set flush to true in order to clear the
// events after obtaining them.
func (s *Summary) InputEvents(flush bool) map[string][]NodeEvent {
	return getEvents(flush, &s.inputEvents)
}

// ProcessorEvents returns a map of processor labels to events traced during the
// execution of a stream pipeline.
func (s *Summary) ProcessorEvents(flush bool) map[string][]NodeEvent {
	return getEvents(flush, &s.processorEvents)
}

// OutputEvents returns a map of output labels to events traced during the
// execution of a stream pipeline.
func (s *Summary) OutputEvents(flush bool) map[string][]NodeEvent {
	return getEvents(flush, &s.outputEvents)
}

//------------------------------------------------------------------------------

func (s *Summary) wInputEvents(label string) (e *events, counter *uint64) {
	i, _ := s.inputEvents.LoadOrStore(label, &events{
		ctrl: s.ctrl,
	})
	return i.(*events), &s.Input
}

func (s *Summary) wOutputEvents(label string) (e *events, counter *uint64) {
	i, _ := s.outputEvents.LoadOrStore(label, &events{
		ctrl: s.ctrl,
	})
	return i.(*events), &s.Output
}

func (s *Summary) wProcessorEvents(label string) (e *events, errCounter *uint64) {
	i, _ := s.processorEvents.LoadOrStore(label, &events{
		ctrl: s.ctrl,
	})
	return i.(*events), &s.ProcessorErrors
}

type events struct {
	mut  sync.Mutex
	m    []NodeEvent
	mLen int64

	ctrl *control
}

func (e *events) IsEnabled() bool {
	if !e.ctrl.IsEnabled() {
		return false
	}
	if limit := e.ctrl.EventLimit(); limit > 0 {
		return atomic.LoadInt64(&e.mLen) < limit
	}
	return true
}

func (e *events) Add(event NodeEvent) {
	e.mut.Lock()
	defer e.mut.Unlock()

	atomic.AddInt64(&e.mLen, 1)
	e.m = append(e.m, event)
}

func (e *events) Extract() []NodeEvent {
	e.mut.Lock()
	defer e.mut.Unlock()

	eventsCopy := make([]NodeEvent, len(e.m))
	copy(eventsCopy, e.m)

	return eventsCopy
}

func (e *events) Flush() []NodeEvent {
	e.mut.Lock()
	defer e.mut.Unlock()

	tmpEvents := e.m
	e.m = nil
	atomic.StoreInt64(&e.mLen, 0)
	return tmpEvents
}
