package protobuf

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/warpstreamlabs/bento/public/service"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	fieldOperator       = "operator"
	fieldMessage        = "message"
	fieldImportPaths    = "import_paths"
	fieldDiscardUnknown = "discard_unknown"
	fieldUseProtoNames  = "use_proto_names"

	// BSR Config
	fieldBsrConfig  = "bsr"
	fieldBsrModule  = "module"
	fieldBSRUrl     = "url"
	fieldBsrAPIKey  = "api_key"
	fieldBsrVersion = "version"
)

func protobufProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Parsing").
		Summary(`
Performs conversions to or from a protobuf message. This processor uses reflection, meaning conversions can be made directly from the target .proto files.
`).Description(`
The main functionality of this processor is to map to and from JSON documents, you can read more about JSON mapping of protobuf messages here: [https://developers.google.com/protocol-buffers/docs/proto3#json](https://developers.google.com/protocol-buffers/docs/proto3#json)

Using reflection for processing protobuf messages in this way is less performant than generating and using native code. Therefore when performance is critical it is recommended that you use Bento plugins instead for processing protobuf messages natively, you can find an example of Bento plugins at [https://github.com/warpstreamlabs/bento-plugin-example](https://github.cowarpstreamlabs/bentoento-plugin-example)

## Operators

### `+"`to_json`"+`

Converts protobuf messages into a generic JSON structure. This makes it easier to manipulate the contents of the document within Bento.

### `+"`from_json`"+`

Attempts to create a target protobuf message from a generic JSON structure.
`).Fields(
		service.NewStringEnumField(fieldOperator, "to_json", "from_json").
			Description("The [operator](#operators) to execute"),
		service.NewStringField(fieldMessage).
			Description("The fully qualified name of the protobuf message to convert to/from."),
		service.NewBoolField(fieldDiscardUnknown).
			Description("If `true`, the `from_json` operator discards fields that are unknown to the schema.").
			Default(false),
		service.NewBoolField(fieldUseProtoNames).
			Description("If `true`, the `to_json` operator deserializes fields exactly as named in schema file.").
			Default(false),
		service.NewStringListField(fieldImportPaths).
			Description("A list of directories containing .proto files, including all definitions required for parsing the target message. If left empty the current directory is used. Each directory listed will be walked with all found .proto files imported. Either this field or `bsr` must be populated.").
			Default([]string{}),
		service.NewObjectListField(fieldBsrConfig,
			service.NewStringField(fieldBsrModule).
				Description("Module to fetch from a Buf Schema Registry e.g. 'buf.build/exampleco/mymodule'."),
			service.NewStringField(fieldBSRUrl).
				Description("Buf Schema Registry URL, leave blank to extract from module.").
				Default("").Advanced(),
			service.NewStringField(fieldBsrAPIKey).
				Description("Buf Schema Registry server API key, can be left blank for a public registry.").
				Secret().
				Default(""),
			service.NewStringField(fieldBsrVersion).
				Description("Version to retrieve from the Buf Schema Registry, leave blank for latest.").
				Default("").Advanced(),
		).Description("Buf Schema Registry configuration. Either this field or `import_paths` must be populated. Note that this field is an array, and multiple BSR configurations can be provided.").
			Default([]any{}),
	).LintRule(`
root = match {
this.import_paths.type() == "unknown" && this.bsr.length() == 0 => [ "at least one of `+"`import_paths`"+`and `+"`bsr`"+` must be set" ],
this.import_paths.type() == "array" && this.import_paths.length() > 0 && this.bsr.length() > 0 => [ "both `+"`import_paths`"+` and `+"`bsr`"+` can't be set simultaneously" ],
}`).Example(
		"JSON to Protobuf using Schema from Disk", `
If we have the following protobuf definition within a directory called `+"`testing/schema`"+`:

`+"```protobuf"+`
syntax = "proto3";
package testing;

import "google/protobuf/timestamp.proto";

message Person {
  string first_name = 1;
  string last_name = 2;
  string full_name = 3;
  int32 age = 4;
  int32 id = 5; // Unique ID number for this person.
  string email = 6;

  google.protobuf.Timestamp last_updated = 7;
}
`+"```"+`

And a stream of JSON documents of the form:

`+"```json"+`
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
`+"```"+`

We can convert the documents into protobuf messages with the following config:`, `
pipeline:
  processors:
    - protobuf:
        operator: from_json
        message: testing.Person
        import_paths: [ testing/schema ]
`).Example(
		"Protobuf to JSON using Schema from Disk", `
If we have the following protobuf definition within a directory called `+"`testing/schema`"+`:

`+"```protobuf"+`
syntax = "proto3";
package testing;

import "google/protobuf/timestamp.proto";

message Person {
  string first_name = 1;
  string last_name = 2;
  string full_name = 3;
  int32 age = 4;
  int32 id = 5; // Unique ID number for this person.
  string email = 6;

  google.protobuf.Timestamp last_updated = 7;
}
`+"```"+`

And a stream of protobuf messages of the type `+"`Person`"+`, we could convert them into JSON documents of the format:

`+"```json"+`
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
`+"```"+`

With the following config:`, `
pipeline:
  processors:
    - protobuf:
        operator: to_json
        message: testing.Person
        import_paths: [ testing/schema ]
`).Example(
		"JSON to Protobuf using Buf Schema Registry", `
If we have the following protobuf definition within a BSR module hosted at `+"`buf.build/exampleco/mymodule`"+`:

`+"```protobuf"+`
syntax = "proto3";
package testing;

import "google/protobuf/timestamp.proto";

message Person {
  string first_name = 1;
  string last_name = 2;
  string full_name = 3;
  int32 age = 4;
  int32 id = 5; // Unique ID number for this person.
  string email = 6;

  google.protobuf.Timestamp last_updated = 7;
}
`+"```"+`

And a stream of JSON documents of the form:

`+"```json"+`
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
`+"```"+`

We can convert the documents into protobuf messages with the following config:`, `
pipeline:
  processors:
    - protobuf:
        operator: from_json
        message: testing.Person
        bsr:
          - module: buf.build/exampleco/mymodule
            api_key: xxx
`).Example(
		"Protobuf to JSON using Buf Schema Registry", `
If we have the following protobuf definition within a BSR module hosted at `+"`buf.build/exampleco/mymodule`"+`:
`+"```protobuf"+`
syntax = "proto3";
package testing;

import "google/protobuf/timestamp.proto";

message Person {
  string first_name = 1;
  string last_name = 2;
  string full_name = 3;
  int32 age = 4;
  int32 id = 5; // Unique ID number for this person.
  string email = 6;

  google.protobuf.Timestamp last_updated = 7;
}
`+"```"+`

And a stream of protobuf messages of the type `+"`Person`"+`, we could convert them into JSON documents of the format:

`+"```json"+`
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
`+"```"+`

With the following config:`, `
pipeline:
  processors:
    - protobuf:
        operator: to_json
        message: testing.Person
        bsr:
          - module: buf.build/exampleco/mymodule
            api_key: xxxx
`)
}

func init() {
	err := service.RegisterProcessor("protobuf", protobufProcessorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newProtobuf(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type protobufOperator func(part *service.Message) error

func newProtobufToJSONOperator(f fs.FS, msg string, importPaths []string, useProtoNames bool) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	descriptors, types, err := loadDescriptors(f, importPaths)
	if err != nil {
		return nil, err
	}

	d, err := descriptors.FindDescriptorByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition within '%v'", msg, importPaths)
	}

	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("message descriptor %v was unexpected type %T", msg, d)
	}

	return func(part *service.Message) error {
		partBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(partBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal protobuf message '%v': %w", msg, err)
		}

		opts := protojson.MarshalOptions{
			Resolver:      types,
			UseProtoNames: useProtoNames,
		}
		data, err := opts.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON protobuf message '%v': %w", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func newProtobufToJSONBSROperator(multiModuleWatcher *MultiModuleWatcher, msg string, useProtoNames bool) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	d, err := multiModuleWatcher.FindMessageByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition: %w", msg, err)
	}

	return func(part *service.Message) error {
		partBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(d.Descriptor())
		if err := proto.Unmarshal(partBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal protobuf message '%v': %w", msg, err)
		}

		opts := protojson.MarshalOptions{
			Resolver:      multiModuleWatcher,
			UseProtoNames: useProtoNames,
		}
		data, err := opts.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON protobuf message '%v': %w", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func newProtobufFromJSONOperator(f fs.FS, msg string, importPaths []string, discardUnknown bool) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	_, types, err := loadDescriptors(f, importPaths)
	if err != nil {
		return nil, err
	}

	types.RangeMessages(func(mt protoreflect.MessageType) bool {
		return true
	})

	md, err := types.FindMessageByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition within '%v'", msg, importPaths)
	}

	return func(part *service.Message) error {
		msgBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(md.Descriptor())

		opts := protojson.UnmarshalOptions{
			Resolver:       types,
			DiscardUnknown: discardUnknown,
		}
		if err := opts.Unmarshal(msgBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal JSON message '%v': %w", msg, err)
		}

		data, err := proto.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message '%v': %v", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func newProtobufFromJSONBSROperator(multiModuleWatcher *MultiModuleWatcher, msg string, discardUnknown bool) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	d, err := multiModuleWatcher.FindMessageByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition: %w", msg, err)
	}

	return func(part *service.Message) error {
		msgBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(d.Descriptor())

		opts := protojson.UnmarshalOptions{
			Resolver:       multiModuleWatcher,
			DiscardUnknown: discardUnknown,
		}
		if err := opts.Unmarshal(msgBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal JSON message '%v': %w", msg, err)
		}

		data, err := proto.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message '%v': %v", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func strToProtobufOperator(f fs.FS, opStr, message string, importPaths []string, discardUnknown, useProtoNames bool) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONOperator(f, message, importPaths, useProtoNames)
	case "from_json":
		return newProtobufFromJSONOperator(f, message, importPaths, discardUnknown)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func strToProtobufBSROperator(multiModuleWatcher *MultiModuleWatcher, opStr, message string, discardUnknown, useProtoNames bool) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONBSROperator(multiModuleWatcher, message, useProtoNames)
	case "from_json":
		return newProtobufFromJSONBSROperator(multiModuleWatcher, message, discardUnknown)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func loadDescriptors(f fs.FS, importPaths []string) (*protoregistry.Files, *protoregistry.Types, error) {
	files := map[string]string{}
	for _, importPath := range importPaths {
		if err := fs.WalkDir(f, importPath, func(path string, info fs.DirEntry, ferr error) error {
			if ferr != nil || info.IsDir() {
				return ferr
			}
			if filepath.Ext(info.Name()) == ".proto" {
				rPath, ferr := filepath.Rel(importPath, path)
				if ferr != nil {
					return fmt.Errorf("failed to get relative path: %v", ferr)
				}
				content, ferr := os.ReadFile(path)
				if ferr != nil {
					return fmt.Errorf("failed to read import %v: %v", path, ferr)
				}
				files[rPath] = string(content)
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return RegistriesFromMap(files)
}

// ------------------------------------------------------------------------------

type protobufProc struct {
	operator protobufOperator
	log      *service.Logger
	// Used for loading and reading from multiple Buf Schema Registry repositories
	multiModuleWatcher *MultiModuleWatcher
}

func newProtobuf(conf *service.ParsedConfig, mgr *service.Resources) (*protobufProc, error) {
	p := &protobufProc{
		log: mgr.Logger(),
	}

	operatorStr, err := conf.FieldString(fieldOperator)
	if err != nil {
		return nil, err
	}

	var message string
	if message, err = conf.FieldString(fieldMessage); err != nil {
		return nil, err
	}

	var discardUnknown bool
	if discardUnknown, err = conf.FieldBool(fieldDiscardUnknown); err != nil {
		return nil, err
	}

	var useProtoNames bool
	if useProtoNames, err = conf.FieldBool(fieldUseProtoNames); err != nil {
		return nil, err
	}

	// Load BSR config
	var bsrModules []*service.ParsedConfig
	if bsrModules, err = conf.FieldObjectList(fieldBsrConfig); err != nil {
		return nil, err
	}

	// if BSR config is present, use BSR to discover proto definitions
	if len(bsrModules) > 0 {
		p.multiModuleWatcher, err = newMultiModuleWatcher(bsrModules)
		if err != nil {
			return nil, fmt.Errorf("failed to create MultiModuleWatcher: %w", err)
		}

		if p.operator, err = strToProtobufBSROperator(p.multiModuleWatcher, operatorStr, message, discardUnknown, useProtoNames); err != nil {
			return nil, err
		}
	} else {
		// else read from file paths
		var importPaths []string
		if importPaths, err = conf.FieldStringList(fieldImportPaths); err != nil {
			return nil, err
		}
		if p.operator, err = strToProtobufOperator(mgr.FS(), operatorStr, message, importPaths, discardUnknown, useProtoNames); err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (p *protobufProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	if err := p.operator(msg); err != nil {
		p.log.Debugf("Operator failed: %v", err)
		return nil, err
	}
	return service.MessageBatch{msg}, nil
}

func (p *protobufProc) Close(context.Context) error {
	return nil
}
