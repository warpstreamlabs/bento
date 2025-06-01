package testutil

import (
	"fmt"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/buffer"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/component/tracer"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/stream"
)

func fromYAMLGenericWithArgs[T any](confStr string, args []any, fromAny func(prov docs.Provider, value any) (T, error)) (T, error) {
	var zero T

	var formattedStr string
	if len(args) > 0 {
		formattedStr = fmt.Sprintf(confStr, args...)
	} else {
		formattedStr = confStr
	}

	node, err := docs.UnmarshalYAML([]byte(formattedStr))
	if err != nil {
		return zero, err
	}
	return fromAny(bundle.GlobalEnvironment, node)
}

func fromYAMLGeneric[T any](confStr string, fromAny func(prov docs.Provider, value any) (T, error)) (T, error) {
	return fromYAMLGenericWithArgs(confStr, nil, fromAny)
}

func BufferFromYAML(confStr string) (buffer.Config, error) {
	return fromYAMLGeneric(confStr, buffer.FromAny)
}

func BufferFromYAMLWithArgs(confStr string, args ...any) (buffer.Config, error) {
	return fromYAMLGenericWithArgs(confStr, args, buffer.FromAny)
}

func CacheFromYAML(confStr string) (cache.Config, error) {
	return fromYAMLGeneric(confStr, cache.FromAny)
}

func CacheFromYAMLWithArgs(confStr string, args ...any) (cache.Config, error) {
	return fromYAMLGenericWithArgs(confStr, args, cache.FromAny)
}

func InputFromYAML(confStr string) (input.Config, error) {
	return fromYAMLGeneric(confStr, input.FromAny)
}

func InputFromYAMLWithArgs(confStr string, args ...any) (input.Config, error) {
	return fromYAMLGenericWithArgs(confStr, args, input.FromAny)
}

func MetricsFromYAML(confStr string) (metrics.Config, error) {
	return fromYAMLGeneric(confStr, metrics.FromAny)
}

func MetricsFromYAMLWithArgs(confStr string, args ...any) (metrics.Config, error) {
	return fromYAMLGenericWithArgs(confStr, args, metrics.FromAny)
}

func OutputFromYAML(confStr string) (output.Config, error) {
	return fromYAMLGeneric(confStr, output.FromAny)
}

func OutputFromYAMLWithArgs(confStr string, args ...any) (output.Config, error) {
	return fromYAMLGenericWithArgs(confStr, args, output.FromAny)
}

func ProcessorFromYAML(confStr string) (processor.Config, error) {
	return fromYAMLGeneric(confStr, processor.FromAny)
}

func ProcessorFromYAMLWithArgs(confStr string, args ...any) (processor.Config, error) {
	return fromYAMLGenericWithArgs(confStr, args, processor.FromAny)
}

func RateLimitFromYAML(confStr string) (ratelimit.Config, error) {
	return fromYAMLGeneric(confStr, ratelimit.FromAny)
}

func RateLimitFromYAMLWithArgs(confStr string, args ...any) (ratelimit.Config, error) {
	return fromYAMLGenericWithArgs(confStr, args, ratelimit.FromAny)
}

func TracerFromYAML(confStr string) (tracer.Config, error) {
	return fromYAMLGeneric(confStr, tracer.FromAny)
}

func TracerFromYAMLWithArgs(confStr string, args ...any) (tracer.Config, error) {
	return fromYAMLGenericWithArgs(confStr, args, tracer.FromAny)
}

func ManagerFromYAML(confStr string) (manager.ResourceConfig, error) {
	return fromYAMLGeneric(confStr, manager.FromAny)
}

func ManagerFromYAMLWithArgs(confStr string, args ...any) (manager.ResourceConfig, error) {
	return fromYAMLGenericWithArgs(confStr, args, manager.FromAny)
}

func StreamFromYAML(confStr string) (stream.Config, error) {
	return StreamFromYAMLWithArgs(confStr)
}

func StreamFromYAMLWithArgs(confStr string, args ...any) (stream.Config, error) {
	node, err := docs.UnmarshalYAML([]byte(confStr))
	if err != nil {
		return stream.Config{}, err
	}

	var rawSource any
	_ = node.Decode(&rawSource)

	pConf, err := stream.Spec().ParsedConfigFromAny(node)
	if err != nil {
		return stream.Config{}, err
	}
	return stream.FromParsed(bundle.GlobalEnvironment, pConf, rawSource)
}

func ConfigFromYAML(confStr string) (config.Type, error) {
	node, err := docs.UnmarshalYAML([]byte(confStr))
	if err != nil {
		return config.Type{}, err
	}

	var rawSource any
	_ = node.Decode(&rawSource)

	pConf, err := config.Spec().ParsedConfigFromAny(node)
	if err != nil {
		return config.Type{}, err
	}
	return config.FromParsed(bundle.GlobalEnvironment, pConf, rawSource)
}

func ConfigFromYAMLWithArgs(confStr string, args ...any) (config.Type, error) {
	node, err := docs.UnmarshalYAML([]byte(confStr))
	if err != nil {
		return config.Type{}, err
	}

	var rawSource any
	_ = node.Decode(&rawSource)

	pConf, err := config.Spec().ParsedConfigFromAny(node)
	if err != nil {
		return config.Type{}, err
	}
	return config.FromParsed(bundle.GlobalEnvironment, pConf, rawSource)
}
