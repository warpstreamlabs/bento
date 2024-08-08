package confluent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/linkedin/goavro/v2"

	"github.com/warpstreamlabs/bento/public/service"
)

type field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type reference struct {
	Type      string  `json:"type"`
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Fields    []field `json:"fields"`
}

func resolveAvroReferences(ctx context.Context, client *schemaRegistryClient, info SchemaInfo) (string, error) {
	if len(info.References) == 0 {
		return info.Schema, nil
	}

	refsMap := map[string]string{}
	if err := client.WalkReferences(ctx, info.References, func(ctx context.Context, name string, info SchemaInfo) error {
		refsMap[name] = info.Schema
		return nil
	}); err != nil {
		return "", nil
	}

	var schemaDry any
	if err := json.Unmarshal([]byte(info.Schema), &schemaDry); err != nil {
		return "", fmt.Errorf("failed to parse root schema as enum: %w", err)
	}

	schemaRaw, _ := json.Marshal(schemaDry)
	schemaString := string(schemaRaw)

	var schema reference
	err := json.Unmarshal(schemaRaw, &schema)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal root schema: %w", err)
	}

	var ref reference

	for {
		initialSchemaDry := schemaString

		for k, v := range refsMap {
			err := json.Unmarshal([]byte(refsMap[k]), &ref)
			if err != nil {
				return "", fmt.Errorf("failed to unmarshal refsMap value %s: %w", k, err)
			}

			if schema.Namespace == ref.Namespace {
				schemaString = strings.ReplaceAll(schemaString, `"type":"`+ref.Name+`"`, `"type":`+v)
			} else {
				schemaString = strings.ReplaceAll(schemaString, `"type":"`+ref.Namespace+`.`+ref.Name+`"`, `"type":`+v)
			}
		}
		if schemaString == initialSchemaDry {
			break
		}
	}

	return schemaString, nil
}

func (s *schemaRegistryEncoder) getAvroEncoder(ctx context.Context, info SchemaInfo) (schemaEncoder, error) {
	schema, err := resolveAvroReferences(ctx, s.client, info)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if s.avroRawJSON {
		if codec, err = goavro.NewCodecForStandardJSONFull(schema); err != nil {
			return nil, err
		}
	} else {
		if codec, err = goavro.NewCodec(schema); err != nil {
			return nil, err
		}
	}

	return func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}

		datum, _, err := codec.NativeFromTextual(b)
		if err != nil {
			return err
		}

		binary, err := codec.BinaryFromNative(nil, datum)
		if err != nil {
			return err
		}

		m.SetBytes(binary)
		return nil
	}, nil
}

func (s *schemaRegistryDecoder) getAvroDecoder(ctx context.Context, info SchemaInfo) (schemaDecoder, error) {
	schema, err := resolveAvroReferences(ctx, s.client, info)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if s.avroRawJSON {
		codec, err = goavro.NewCodecForStandardJSONFull(schema)
	} else {
		codec, err = goavro.NewCodec(schema)
	}
	if err != nil {
		return nil, err
	}

	decoder := func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}

		native, _, err := codec.NativeFromBinary(b)
		if err != nil {
			return err
		}

		jb, err := codec.TextualFromNative(nil, native)
		if err != nil {
			return err
		}
		m.SetBytes(jb)

		return nil
	}

	return decoder, nil
}
