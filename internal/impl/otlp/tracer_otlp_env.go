package otlp

import (
	"cmp"
	"context"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"

	"github.com/warpstreamlabs/bento/internal/bundle"
)

func init() {
	bundle.AllTracers.RegisterEnvFn(NewFromEnv)
}

// NewFromEnv creates an OTLP tracer provider when standard OpenTelemetry
// environment variables are present. Returns (nil, nil) when no endpoint env
// var is set.
//
// Endpoint, TLS, headers, timeout, and compression are all read by the
// underlying OTel SDK exporter packages — this function only gates on the
// presence of an endpoint and selects the transport (gRPC vs HTTP).
//
// Supported env vars: https://opentelemetry.io/docs/specs/otel/protocol/exporter/
func NewFromEnv() (trace.TracerProvider, error) {
	// Only auto-configure when an OTLP endpoint is explicitly set in the
	// environment. Without this gate the SDK defaults to localhost.
	if os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" &&
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return nil, nil
	}

	// Select transport package. The SDK within each package handles endpoint
	// parsing, TLS, headers, etc. from env vars — we just pick grpc vs http.
	protocol := cmp.Or(
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"),
		os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
		"http/protobuf",
	)

	ctx := context.Background()
	var exp tracesdk.SpanExporter
	var err error

	switch protocol {
	case "http/protobuf", "http":
		exp, err = otlptracehttp.New(ctx)
	default:
		exp, err = otlptracegrpc.New(ctx)
	}
	if err != nil {
		return nil, err
	}

	var attrs []attribute.KeyValue
	if svc := os.Getenv("OTEL_SERVICE_NAME"); svc != "" {
		attrs = append(attrs, semconv.ServiceNameKey.String(svc))
	} else {
		attrs = append(attrs, semconv.ServiceNameKey.String("bento"))
	}

	return tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attrs...)),
	), nil
}
