package service

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

const (
	etsField   = "extract_tracing_map"
	nrswlField = "new_root_span_with_link"
)

// NewExtractTracingSpanMappingField returns a config field for mapping messages
// in order to extract distributed tracing information.
func NewExtractTracingSpanMappingField() *ConfigField {
	return NewBloblangField(etsField).
		Description("EXPERIMENTAL: A [Bloblang mapping](/docs/guides/bloblang/about) that attempts to extract an object containing tracing propagation information, which will then be used as the root tracing span for the message. The specification of the extracted fields must match the format used by the service wide tracer.").
		Examples(`root = @`, `root = this.meta.span`).
		Version("1.0.0").
		Optional().
		Advanced()
}

func NewRootSpanWithLinkField() *ConfigField {
	return NewBoolField(nrswlField).
		Description("EXPERIMENTAL: Starts a new root span with link to parent.").
		Optional().
		Advanced()
}

// WrapBatchInputExtractTracingSpanMapping wraps a BatchInput with a mechanism
// for extracting tracing spans using a bloblang mapping.
func (p *ParsedConfig) WrapBatchInputExtractTracingSpanMapping(inputName string, i BatchInput) (
	BatchInput, error) {
	if str, _ := p.FieldString(etsField); str == "" {
		return i, nil
	}

	exe, err := p.FieldBloblang(etsField)
	if err != nil {
		return nil, err
	}

	newSpan, _ := p.FieldBool(nrswlField)

	return &spanInjectBatchInput{inputName: inputName, mgr: p.mgr, mapping: exe, rdr: i, newSpan: newSpan}, nil
}

// WrapInputExtractTracingSpanMapping wraps a Input with a mechanism for
// extracting tracing spans from the consumed message using a Bloblang mapping.
func (p *ParsedConfig) WrapInputExtractTracingSpanMapping(inputName string, i Input) (Input, error) {
	if str, _ := p.FieldString(etsField); str == "" {
		return i, nil
	}
	exe, err := p.FieldBloblang(etsField)
	if err != nil {
		return nil, err
	}

	newSpan, _ := p.FieldBool(nrswlField)
	return &spanInjectInput{inputName: inputName, mgr: p.mgr, mapping: exe, rdr: i, newSpan: newSpan}, nil
}

func getPropMapCarrier(spanPart *Message) (propagation.MapCarrier, error) {
	structured, err := spanPart.AsStructured()
	if err != nil {
		return nil, err
	}

	spanMap, ok := structured.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected an object, got: %T", structured)
	}

	c := propagation.MapCarrier{}
	for k, v := range spanMap {
		if vStr, ok := v.(string); ok {
			c[strings.ToLower(k)] = vStr
		}
	}
	return c, nil
}

// spanInjectBatchInput wraps a BatchInput with a mechanism for
// extracting tracing spans from the consumed message using a Bloblang mapping.
type spanInjectBatchInput struct {
	inputName string
	mgr       bundle.NewManagement

	mapping *bloblang.Executor
	rdr     BatchInput
	newSpan bool
}

func (s *spanInjectBatchInput) Connect(ctx context.Context) error {
	return s.rdr.Connect(ctx)
}

func (s *spanInjectBatchInput) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	m, afn, err := s.rdr.ReadBatch(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanPart, err := m.BloblangQuery(0, s.mapping)
	if err != nil {
		s.mgr.Logger().Error("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	c, err := getPropMapCarrier(spanPart)
	if err != nil {
		s.mgr.Logger().Error("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	prov := s.mgr.Tracer()
	operationName := "input_" + s.inputName

	textProp := otel.GetTextMapPropagator()
	for i, p := range m {
		ctx := textProp.Extract(p.Context(), c)

		var opts []trace.SpanStartOption
		if s.newSpan {
			opts = []trace.SpanStartOption{
				trace.WithNewRoot(),
				trace.WithLinks(trace.LinkFromContext(ctx)),
			}
		}

		pCtx, _ := prov.Tracer("bento").Start(ctx, operationName, opts...)
		m[i] = p.WithContext(pCtx)
	}
	return m, afn, nil
}

func (s *spanInjectBatchInput) Close(ctx context.Context) error {
	return s.rdr.Close(ctx)
}

// spanInjectInput wraps a Input with a mechanism for extracting tracing
// spans from the consumed message using a Bloblang mapping.
type spanInjectInput struct {
	inputName string
	mgr       bundle.NewManagement

	mapping *bloblang.Executor
	rdr     Input
	newSpan bool
}

func (s *spanInjectInput) Connect(ctx context.Context) error {
	return s.rdr.Connect(ctx)
}

func (s *spanInjectInput) Read(ctx context.Context) (*Message, AckFunc, error) {
	m, afn, err := s.rdr.Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanPart, err := m.BloblangQuery(s.mapping)
	if err != nil {
		s.mgr.Logger().Error("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	c, err := getPropMapCarrier(spanPart)
	if err != nil {
		s.mgr.Logger().Error("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	prov := s.mgr.Tracer()
	operationName := "input_" + s.inputName

	textProp := otel.GetTextMapPropagator()

	ctx = textProp.Extract(m.Context(), c)

	var opts []trace.SpanStartOption
	if s.newSpan {
		opts = []trace.SpanStartOption{
			trace.WithNewRoot(),
			trace.WithLinks(trace.LinkFromContext(ctx)),
		}
	}

	pCtx, _ := prov.Tracer("bento").Start(ctx, operationName, opts...)
	m = m.WithContext(pCtx)

	return m, afn, nil
}

func (s *spanInjectInput) Close(ctx context.Context) error {
	return s.rdr.Close(ctx)
}
