package model

import (
	"encoding/json"
	"errors"

	"github.com/warpstreamlabs/bento/internal/docs"
	pb "github.com/warpstreamlabs/bento/internal/docs/specpb"
	"google.golang.org/protobuf/proto"
)

var errUnsupportedComponentType = errors.New("unsupported component type")

type Type uint32

const (
	Unregistered Type = iota
	ProcessorType
	BatchProcessorType
	// TODO(gregfurman): All types
)

func FromPluginType(typ Type) (docs.Type, error) {
	switch typ {
	case ProcessorType, BatchProcessorType:
		return docs.TypeProcessor, nil
	}

	return "", errUnsupportedComponentType
}

func FromRawPluginSpec(rawSpec []byte) (docs.ComponentSpec, error) {
	var componentSpec docs.ComponentSpec
	if err := json.Unmarshal(rawSpec, &componentSpec); err != nil {
		return docs.ComponentSpec{}, err
	}
	return componentSpec, nil
}

func FromProtoPluginSpec(b []byte) (docs.ComponentSpec, error) {
	protoComponentSpec := &pb.ComponentSpec{}

	if err := proto.Unmarshal(b, protoComponentSpec); err != nil {
		return docs.ComponentSpec{}, err
	}

	return docs.ToComponentSpec(protoComponentSpec), nil
}
