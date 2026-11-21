package docs

import (
	pb "github.com/warpstreamlabs/bento/internal/docs/specpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func FromFieldSpec(field FieldSpec) (*pb.FieldSpec, error) {
	var err error

	proto := &pb.FieldSpec{
		Name:         field.Name,
		IsOptional:   field.IsOptional,
		IsSecret:     field.IsSecret,
		Interpolated: field.Interpolated,
		Bloblang:     field.Bloblang,
		Options:      field.Options,
	}

	switch field.Type {
	case FieldTypeString:
		proto.FieldType = pb.FieldType_FIELD_TYPE_STRING
	case FieldTypeInt:
		proto.FieldType = pb.FieldType_FIELD_TYPE_INT
	case FieldTypeFloat:
		proto.FieldType = pb.FieldType_FIELD_TYPE_FLOAT
	case FieldTypeBool:
		proto.FieldType = pb.FieldType_FIELD_TYPE_BOOL
	case FieldTypeObject:
		proto.FieldType = pb.FieldType_FIELD_TYPE_OBJECT
	case FieldTypeInput:
		proto.FieldType = pb.FieldType_FIELD_TYPE_INPUT
	case FieldTypeBuffer:
		proto.FieldType = pb.FieldType_FIELD_TYPE_BUFFER
	case FieldTypeCache:
		proto.FieldType = pb.FieldType_FIELD_TYPE_CACHE
	case FieldTypeProcessor:
		proto.FieldType = pb.FieldType_FIELD_TYPE_PROCESSOR
	case FieldTypeRateLimit:
		proto.FieldType = pb.FieldType_FIELD_TYPE_RATE_LIMIT
	case FieldTypeOutput:
		proto.FieldType = pb.FieldType_FIELD_TYPE_OUTPUT
	case FieldTypeMetrics:
		proto.FieldType = pb.FieldType_FIELD_TYPE_METRICS
	case FieldTypeTracer:
		proto.FieldType = pb.FieldType_FIELD_TYPE_TRACER
	case FieldTypeScanner:
		proto.FieldType = pb.FieldType_FIELD_TYPE_SCANNER
	default:
		proto.FieldType = pb.FieldType_FIELD_TYPE_UNKNOWN
	}

	switch field.Kind {
	case KindArray:
		proto.FieldKind = pb.FieldKind_FIELD_KIND_ARRAY
	case Kind2DArray:
		proto.FieldKind = pb.FieldKind_FIELD_KIND_2D_ARRAY
	case KindMap:
		proto.FieldKind = pb.FieldKind_FIELD_KIND_MAP
	case KindScalar:
		proto.FieldKind = pb.FieldKind_FIELD_KIND_SCALAR
	default:
		proto.FieldKind = pb.FieldKind_FIELD_KIND_UNKNOWN
	}

	if defaultValuePtr := field.Default; defaultValuePtr != nil {
		proto.DefaultValue, err = toFieldValue(*field.Default)
		if err != nil {
			return nil, err
		}
	}

	if len(field.Children) > 0 {
		proto.Children = make([]*pb.FieldSpec, len(field.Children))
		for i, child := range field.Children {
			proto.Children[i], err = FromFieldSpec(child)
			if err != nil {
				return nil, err
			}
		}
	}

	if field.Linter != "" {
		proto.Linter = &field.Linter
	}

	return proto, nil
}

func ToFieldSpec(proto *pb.FieldSpec) FieldSpec {
	field := FieldSpec{
		Name:         proto.Name,
		IsOptional:   proto.IsOptional,
		IsSecret:     proto.IsSecret,
		Interpolated: proto.Interpolated,
		Bloblang:     proto.Bloblang,
		Options:      proto.Options,
	}

	switch proto.FieldType {
	case pb.FieldType_FIELD_TYPE_STRING:
		field.Type = FieldTypeString
	case pb.FieldType_FIELD_TYPE_INT:
		field.Type = FieldTypeInt
	case pb.FieldType_FIELD_TYPE_FLOAT:
		field.Type = FieldTypeFloat
	case pb.FieldType_FIELD_TYPE_BOOL:
		field.Type = FieldTypeBool
	case pb.FieldType_FIELD_TYPE_OBJECT:
		field.Type = FieldTypeObject
	case pb.FieldType_FIELD_TYPE_INPUT:
		field.Type = FieldTypeInput
	case pb.FieldType_FIELD_TYPE_BUFFER:
		field.Type = FieldTypeBuffer
	case pb.FieldType_FIELD_TYPE_CACHE:
		field.Type = FieldTypeCache
	case pb.FieldType_FIELD_TYPE_PROCESSOR:
		field.Type = FieldTypeProcessor
	case pb.FieldType_FIELD_TYPE_RATE_LIMIT:
		field.Type = FieldTypeRateLimit
	case pb.FieldType_FIELD_TYPE_OUTPUT:
		field.Type = FieldTypeOutput
	case pb.FieldType_FIELD_TYPE_METRICS:
		field.Type = FieldTypeMetrics
	case pb.FieldType_FIELD_TYPE_TRACER:
		field.Type = FieldTypeTracer
	case pb.FieldType_FIELD_TYPE_SCANNER:
		field.Type = FieldTypeScanner
	default:
		field.Type = FieldTypeUnknown
	}

	switch proto.FieldKind {
	case pb.FieldKind_FIELD_KIND_ARRAY:
		field.Kind = KindArray
	case pb.FieldKind_FIELD_KIND_2D_ARRAY:
		field.Kind = Kind2DArray
	case pb.FieldKind_FIELD_KIND_MAP:
		field.Kind = KindMap
	default:
		field.Kind = KindScalar
	}

	if proto.DefaultValue != nil {
		val := fromFieldValue(proto.DefaultValue)
		field.Default = &val
	}

	if len(proto.Children) > 0 {
		field.Children = make(FieldSpecs, len(proto.Children))
		for i, child := range proto.Children {
			field.Children[i] = ToFieldSpec(child)
		}
	}

	if proto.Linter != nil {
		field.Linter = *proto.Linter
	}

	return field
}

func toFieldValue(val any) (*pb.FieldValue, error) {
	fv := &pb.FieldValue{}
	switch v := val.(type) {
	case bool:
		fv.FieldType = &pb.FieldValue_BoolType{BoolType: v}
	case string:
		fv.FieldType = &pb.FieldValue_StringType{StringType: v}
	case int:
		fv.FieldType = &pb.FieldValue_IntType{IntType: int32(v)}
	case int32:
		fv.FieldType = &pb.FieldValue_IntType{IntType: v}
	case int64:
		fv.FieldType = &pb.FieldValue_IntType{IntType: int32(v)}
	case uint:
		fv.FieldType = &pb.FieldValue_IntType{IntType: int32(v)}
	case uint32:
		fv.FieldType = &pb.FieldValue_IntType{IntType: int32(v)}
	case float32:
		fv.FieldType = &pb.FieldValue_FloatType{FloatType: v}
	case float64:
		fv.FieldType = &pb.FieldValue_FloatType{FloatType: float32(v)}
	default:
		pbVal, err := structpb.NewValue(val)
		if err != nil {
			return nil, err
		}
		fv.FieldType = &pb.FieldValue_ObjectType{ObjectType: pbVal}
	}

	return fv, nil
}

func fromFieldValue(fv *pb.FieldValue) any {
	switch v := fv.FieldType.(type) {
	case *pb.FieldValue_StringType:
		return v.StringType
	case *pb.FieldValue_IntType:
		return v.IntType
	case *pb.FieldValue_FloatType:
		return float32(v.FloatType)
	case *pb.FieldValue_BoolType:
		return v.BoolType
	case *pb.FieldValue_ObjectType:
		return v.ObjectType.AsInterface()
	}
	return nil
}
