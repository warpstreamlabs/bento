package docs

import (
	pb "github.com/warpstreamlabs/bento/internal/docs/specpb"
)

func FromComponentSpec(comp ComponentSpec) (*pb.ComponentSpec, error) {
	conf, err := FromFieldSpec(comp.Config)
	if err != nil {
		return nil, err
	}

	proto := &pb.ComponentSpec{
		Name:   comp.Name,
		Config: conf,
	}

	switch comp.Type {
	case TypeBuffer:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_BUFFER
	case TypeCache:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_CACHE
	case TypeInput:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_INPUT
	case TypeMetrics:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_METRICS
	case TypeOutput:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_OUTPUT
	case TypeProcessor:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_PROCESSOR
	case TypeRateLimit:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_RATE_LIMIT
	case TypeTracer:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_TRACER
	case TypeScanner:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_SCANNER
	default:
		proto.ComponentType = pb.ComponentType_COMPONENT_TYPE_UNKNOWN
	}

	switch comp.Status {
	case StatusStable:
		proto.ComponentStatus = pb.ComponentStatus_COMPONENT_STATUS_STABLE
	case StatusBeta:
		proto.ComponentStatus = pb.ComponentStatus_COMPONENT_STATUS_BETA
	case StatusExperimental:
		proto.ComponentStatus = pb.ComponentStatus_COMPONENT_STATUS_EXPERIMENTAL
	case StatusDeprecated:
		proto.ComponentStatus = pb.ComponentStatus_COMPONENT_STATUS_DEPRECATED
	default:
		proto.ComponentStatus = pb.ComponentStatus_COMPONENT_STATUS_UNKNOWN
	}

	return proto, nil
}

func ToComponentSpec(proto *pb.ComponentSpec) ComponentSpec {
	comp := ComponentSpec{
		Name:   proto.Name,
		Config: ToFieldSpec(proto.Config),
	}

	switch proto.ComponentType {
	case pb.ComponentType_COMPONENT_TYPE_BUFFER:
		comp.Type = TypeBuffer
	case pb.ComponentType_COMPONENT_TYPE_CACHE:
		comp.Type = TypeCache
	case pb.ComponentType_COMPONENT_TYPE_INPUT:
		comp.Type = TypeInput
	case pb.ComponentType_COMPONENT_TYPE_METRICS:
		comp.Type = TypeMetrics
	case pb.ComponentType_COMPONENT_TYPE_OUTPUT:
		comp.Type = TypeOutput
	case pb.ComponentType_COMPONENT_TYPE_PROCESSOR:
		comp.Type = TypeProcessor
	case pb.ComponentType_COMPONENT_TYPE_RATE_LIMIT:
		comp.Type = TypeRateLimit
	case pb.ComponentType_COMPONENT_TYPE_TRACER:
		comp.Type = TypeTracer
	case pb.ComponentType_COMPONENT_TYPE_SCANNER:
		comp.Type = TypeScanner
	}

	switch proto.ComponentStatus {
	case pb.ComponentStatus_COMPONENT_STATUS_STABLE:
		comp.Status = StatusStable
	case pb.ComponentStatus_COMPONENT_STATUS_BETA:
		comp.Status = StatusBeta
	case pb.ComponentStatus_COMPONENT_STATUS_EXPERIMENTAL:
		comp.Status = StatusExperimental
	case pb.ComponentStatus_COMPONENT_STATUS_DEPRECATED:
		comp.Status = StatusDeprecated
	}

	return comp
}
