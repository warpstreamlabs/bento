package protobuf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	v1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/warpstreamlabs/bento/public/service"
)

const protosPath = "../../../config/test/protobuf/schema"

func TestProtobufFromJSON(t *testing.T) {
	type testCase struct {
		name           string
		message        string
		input          string
		outputContains []string
		discardUnknown bool
	}

	var testCases = []testCase{
		{
			name:           "json to protobuf age",
			message:        "testing.Person",
			input:          `{"firstName":"john","lastName":"oates","age":10}`,
			outputContains: []string{"john"},
		},
		{
			name:           "json to protobuf min",
			message:        "testing.Person",
			input:          `{"firstName":"daryl","lastName":"hall"}`,
			outputContains: []string{"daryl"},
		},
		{
			name:           "json to protobuf email",
			message:        "testing.Person",
			input:          `{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com"}`,
			outputContains: []string{"caleb"},
		},
		{
			name:           "json to protobuf with discard_unknown",
			message:        "testing.Person",
			input:          `{"firstName":"caleb","lastName":"quaye","missingfield":"anyvalue"}`,
			outputContains: []string{"caleb"},
			discardUnknown: true,
		},
		{
			name:           "any: json to protobuf 1",
			message:        "testing.Envelope",
			input:          `{"id":747,"content":{"@type":"type.googleapis.com/testing.Person","first_name":"bob"}}`,
			outputContains: []string{"type.googleapis.com/testing.Person"},
		},
		{
			name:           "any: json to protobuf 2",
			message:        "testing.Envelope",
			input:          `{"id":747,"content":{"@type":"type.googleapis.com/testing.House","address":"123"}}`,
			outputContains: []string{"type.googleapis.com/testing.House"},
		},
		{
			name:           "any: json to protobuf with nested message",
			message:        "testing.House.Mailbox",
			input:          `{"color":"red","identifier":"123"}`,
			outputContains: []string{"red"},
		},
	}
	for _, test := range testCases {
		t.Run(test.name+" import paths", func(t *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: from_json
message: %v
import_paths: [ %v ]
discard_unknown: %t
`, test.message, protosPath, test.discardUnknown), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.NotEqual(t, test.input, string(mBytes))
			for _, exp := range test.outputContains {
				assert.Contains(t, string(mBytes), exp)
			}
			require.NoError(t, msgs[0].GetError())
		})

		t.Run(test.name+" bsr", func(t *testing.T) {
			mockBSRServerAddress := runMockBSRServer(t)

			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: from_json
message: %v
bsr:
  - module: "testing"
    url: %s
discard_unknown: %t
`, test.message, "http://"+mockBSRServerAddress, test.discardUnknown), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.NotEqual(t, test.input, string(mBytes))
			for _, exp := range test.outputContains {
				assert.Contains(t, string(mBytes), exp)
			}
			require.NoError(t, msgs[0].GetError())
		})
	}
}

func TestProtobufToJSONImportPaths(t *testing.T) {

	type testCase struct {
		name          string
		message       string
		input         []byte
		output        string
		useProtoNames bool
	}

	var testCases = []testCase{
		{
			name:    "protobuf to json 1",
			message: "testing.Person",
			input:   []byte{0x0a, 0x04, 0x6a, 0x6f, 0x68, 0x6e, 0x12, 0x05, 0x6f, 0x61, 0x74, 0x65, 0x73, 0x20, 0x0a},
			output:  `{"firstName":"john","lastName":"oates","age":10}`,
		},
		{
			name:    "protobuf to json 2",
			message: "testing.Person",
			input:   []byte{0x0a, 0x05, 0x64, 0x61, 0x72, 0x79, 0x6c, 0x12, 0x04, 0x68, 0x61, 0x6c, 0x6c},
			output:  `{"firstName":"daryl","lastName":"hall"}`,
		},
		{
			name:    "protobuf to json 3",
			message: "testing.Person",
			input: []byte{
				0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
				0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
				0x6d,
			},
			output: `{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com"}`,
		},
		{
			name:          "protobuf to json with use_proto_names",
			message:       "testing.Person",
			useProtoNames: true,
			input: []byte{
				0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
				0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
				0x6d,
			},
			output: `{"first_name":"caleb","last_name":"quaye","email":"caleb@myspace.com"}`,
		},
		{
			name:    "any: protobuf to json 1",
			message: "testing.Envelope",
			input: []byte{
				0x8, 0xeb, 0x5, 0x12, 0x2b, 0xa, 0x22, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
				0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
				0x67, 0x2e, 0x50, 0x65, 0x72, 0x73, 0x6f, 0x6e, 0x12, 0x5, 0xa, 0x3, 0x62, 0x6f, 0x62,
			},
			output: `{"id":747,"content":{"@type":"type.googleapis.com/testing.Person","firstName":"bob"}}`,
		},
		{
			name:    "any: protobuf to json 2",
			message: "testing.Envelope",
			input: []byte{
				0x8, 0xeb, 0x5, 0x12, 0x2a, 0xa, 0x21, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
				0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
				0x67, 0x2e, 0x48, 0x6f, 0x75, 0x73, 0x65, 0x12, 0x5, 0x12, 0x3, 0x31, 0x32, 0x33,
			},
			output: `{"id":747,"content":{"@type":"type.googleapis.com/testing.House","address":"123"}}`,
		},
	}
	for _, test := range testCases {
		t.Run(test.name+" import paths", func(t *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: to_json
message: %v
import_paths: [ %v ]
use_proto_names: %t
`, test.message, protosPath, test.useProtoNames), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(context.Background(), service.NewMessage(test.input))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(mBytes))
			require.NoError(t, msgs[0].GetError())
		})

		t.Run(test.name+" bsr", func(t *testing.T) {
			mockBSRServerAddress := runMockBSRServer(t)

			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: to_json
message: %v
bsr:
  - module: "testing"
    url: %s
use_proto_names: %t
`, test.message, "http://"+mockBSRServerAddress, test.useProtoNames), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(context.Background(), service.NewMessage(test.input))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(mBytes))
			require.NoError(t, msgs[0].GetError())
		})
	}
}

func TestProtobufErrorsImportPaths(t *testing.T) {

	type testCase struct {
		name     string
		operator string
		message  string
		input    string
		output   string
	}

	var testCases = []testCase{
		{
			name:     "json to protobuf unknown field",
			operator: "from_json",
			message:  "testing.Person",
			input:    `{"firstName":"john","lastName":"oates","ageFoo":10}`,
			output:   "unknown field \"ageFoo\"",
		},
		{
			name:     "json to protobuf invalid value",
			operator: "from_json",
			message:  "testing.Person",
			input:    `not valid json`,
			output:   "syntax error (line 1:1): invalid value not",
		},
		{
			name:     "json to protobuf invalid string",
			operator: "from_json",
			message:  "testing.Person",
			input:    `{"firstName":5,"lastName":"quaye","email":"caleb@myspace.com"}`,
			output:   "invalid value for string field firstName: 5",
		},
	}

	for _, test := range testCases {
		t.Run(test.name+" import paths", func(tt *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: %v
message: %v
import_paths: [ %v ]
`, test.operator, test.message, protosPath), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			_, err = proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.Error(t, err)
			require.Contains(t, err.Error(), test.output)
		})

		t.Run(test.name+" bsr", func(tt *testing.T) {
			mockBSRServerAddress := runMockBSRServer(t)

			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: %v
message: %v
bsr:
  - module: "testing"
    url: %s
`, test.operator, test.message, "http://"+mockBSRServerAddress), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			_, err = proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.Error(t, err)
			require.Contains(t, err.Error(), test.output)
		})
	}
}

func TestProcessorConfigLinting(t *testing.T) {

	type testCase struct {
		name        string
		input       string
		errContains string
	}

	var testCases = []testCase{
		{
			name: "valid import_paths config",
			input: `
protobuf:
  operator: to_json
  message: testing.Person
  import_paths: [ ./mypath ]
`,
		},
		{
			name: "valid bsr config",
			input: `
protobuf:
  operator: to_json
  message: testing.Person
  bsr:
    - module: "testing"
`,
		},
		{
			name: "can't set both import_paths and bsr",
			input: `
protobuf:
  operator: to_json
  message: testing.Person
  import_paths: [ ./mypath ]
  bsr:
    - module: "buf.build/exampleco/mymodule"
`,
			errContains: "both `import_paths` and `bsr` can't be set simultaneously",
		},
		{
			name: "require one of import_paths and bsr",
			input: `
protobuf:
  operator: to_json
  message: testing.Person
`,
			errContains: "at least one of `import_paths`and `bsr` must be set",
		},
	}
	env := service.NewEnvironment()
	for _, test := range testCases {
		t.Run(test.name, func(tt *testing.T) {
			strm := env.NewStreamBuilder()
			err := strm.AddProcessorYAML(test.input)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

type fileDescriptorSetServer struct {
	fileDescriptorSet *descriptorpb.FileDescriptorSet
}

func (s *fileDescriptorSetServer) GetFileDescriptorSet(_ context.Context, request *connect.Request[v1beta1.GetFileDescriptorSetRequest]) (*connect.Response[v1beta1.GetFileDescriptorSetResponse], error) {
	response := &v1beta1.GetFileDescriptorSetResponse{FileDescriptorSet: s.fileDescriptorSet, Version: request.Msg.GetVersion()}
	return connect.NewResponse(response), nil
}

func runMockBSRServer(t *testing.T) string {
	// load files into protoregistry.Files
	mockResources := service.MockResources()
	files, _, err := loadDescriptors(mockResources.FS(), []string{protosPath})
	require.NoError(t, err)

	// populate into a FileDescriptorSet
	fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
	standardImportPaths := make(map[string]bool)
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		fileDescriptorSet.File = append(fileDescriptorSet.File, protodesc.ToFileDescriptorProto(fd))
		// find any standard imports used https://protobuf.com/docs/descriptors#standard-imports
		for i := 0; i < fd.Imports().Len(); i++ {
			imp := fd.Imports().Get(i)
			if strings.HasPrefix(imp.Path(), "google/protobuf/") {
				standardImportPaths[imp.Path()] = true
			}
		}
		return true
	})

	// add standard imports to the FileDescriptorSet
	for standardImportPath := range standardImportPaths {
		fd, err := protoregistry.GlobalFiles.FindFileByPath(standardImportPath)
		require.NoError(t, err)
		fileDescriptorSet.File = append(fileDescriptorSet.File, protodesc.ToFileDescriptorProto(fd))
	}

	// run GRPC server on an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	mux := http.NewServeMux()
	fileDescriptorSetServer := &fileDescriptorSetServer{fileDescriptorSet: fileDescriptorSet}
	mux.Handle(reflectv1beta1connect.NewFileDescriptorSetServiceHandler(fileDescriptorSetServer))
	go func() {
		if err := http.Serve(listener, h2c.NewHandler(mux, &http2.Server{})); err != nil && !errors.Is(err, http.ErrServerClosed) {
			require.NoError(t, err)
		}
	}()

	return listener.Addr().String()
}
