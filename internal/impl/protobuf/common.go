package protobuf

import (
	"context"
	"fmt"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// RegistriesFromMap attempts to parse a map of filenames (relative to import
// directories) and their contents out into a registry of protobuf files and
// protobuf types. These registries can then be used as a mechanism for
// dynamically (un)marshalling the definitions within.
func RegistriesFromMap(filesMap map[string]string) (*protoregistry.Files, *protoregistry.Types, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(
			&protocompile.SourceResolver{
				Accessor: protocompile.SourceAccessorFromMap(filesMap),
			},
		),
	}

	names := make([]string, 0, len(filesMap))
	for k := range filesMap {
		names = append(names, k)
	}

	fds, err := compiler.Compile(context.Background(), names...)
	if err != nil {
		return nil, nil, err
	}

	files, types := &protoregistry.Files{}, &protoregistry.Types{}
	for _, v := range fds {
		if err := files.RegisterFile(v); err != nil {
			return nil, nil, fmt.Errorf("failed to register file '%v': %w", v.Name(), err)
		}
		msgs := v.Messages()
		for mi := range msgs.Len() {
			t := msgs.Get(mi)
			if err := types.RegisterMessage(dynamicpb.NewMessageType(t)); err != nil {
				return nil, nil, fmt.Errorf("failed to register type '%v': %w", t.Name(), err)
			}
			nested := t.Messages()
			for ni := range nested.Len() {
				nt := nested.Get(ni)
				if err := types.RegisterMessage(dynamicpb.NewMessageType(nt)); err != nil {
					return nil, nil, fmt.Errorf("failed to register type '%v': %w", nt.FullName(), err)
				}
			}
		}
	}
	return files, types, nil
}
