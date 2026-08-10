package confluent

import (
	"context"
	"fmt"

	"github.com/warpstreamlabs/bento/internal/jsonschema"
	"github.com/warpstreamlabs/bento/public/service"
)

// jsonSchemaRootName is the synthetic name given to the schema being compiled,
// so that references supplied by the registry resolve as its siblings.
const jsonSchemaRootName = "bento-schema-root.json"

func resolveJSONSchema(ctx context.Context, client *schemaRegistryClient, info SchemaInfo) (*jsonschema.Schema, error) {
	compiler := jsonschema.NewRegistryCompiler()

	if err := client.WalkReferences(ctx, info.References, func(ctx context.Context, name string, info SchemaInfo) error {
		return jsonschema.AddResourceString(compiler, jsonschema.RegistryURL(name), info.Schema)
	}); err != nil {
		return nil, err
	}

	rootURL := jsonschema.RegistryURL(jsonSchemaRootName)
	if err := jsonschema.AddResourceString(compiler, rootURL, info.Schema); err != nil {
		return nil, fmt.Errorf("failed to parse root schema: %w", err)
	}

	return compiler.Compile(rootURL)
}

func (s *schemaRegistryEncoder) getJSONEncoder(ctx context.Context, info SchemaInfo) (schemaEncoder, error) {
	return getJSONTranscoder(ctx, s.client, info)
}

func (s *schemaRegistryDecoder) getJSONDecoder(ctx context.Context, info SchemaInfo) (schemaDecoder, error) {
	return getJSONTranscoder(ctx, s.client, info)
}

func getJSONTranscoder(ctx context.Context, cl *schemaRegistryClient, info SchemaInfo) (func(m *service.Message) error, error) {
	sch, err := resolveJSONSchema(ctx, cl, info)
	if err != nil {
		return nil, err
	}

	// -- we only need to verify if the message is valid since the input format which bento uses (json) is the same
	// -- as the output format
	return func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}

		// -- verify the json message against the schema
		doc, err := jsonschema.UnmarshalBytes(b)
		if err != nil {
			return err
		}

		if err := sch.Validate(doc); err != nil {
			return fmt.Errorf("json message does not conform to schema: %v", jsonschema.FormatError(err))
		}

		return nil
	}, nil
}
