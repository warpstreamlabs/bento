package component

import (
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/log"
)

// Observability is an interface implemented by components that provide a range
// of observability APIs to components. This is primarily done the service-wide
// managers.
type Observability interface {
	Metrics() metrics.Type
	Logger() log.Modular
	Tracer() trace.TracerProvider
	Path() []string
	Label() string
}

type mockObs struct{}

func (m mockObs) Metrics() metrics.Type {
	return metrics.Noop()
}

func (m mockObs) Logger() log.Modular {
	return log.Noop()
}

func (m mockObs) Tracer() trace.TracerProvider {
	return noop.NewTracerProvider()
}

func (m mockObs) Path() []string {
	return nil
}

func (m mockObs) Label() string {
	return ""
}

// NoopObservability returns an implementation of Observability that does
// nothing.
func NoopObservability() Observability {
	return mockObs{}
}
