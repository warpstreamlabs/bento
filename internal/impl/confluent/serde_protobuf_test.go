package confluent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestProtobufEncodeMultipleMessages(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	thingsSchema := `
syntax = "proto3";
package things;

message foo {
  float a = 1;
  string b = 2;
}

message bar {
  string b = 1;
}
`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
			}), nil
		}
		return nil, nil
	})

	subj, err := service.NewInterpolatedString("${! @subject }")
	require.NoError(t, err)

	tests := []struct {
		name        string
		subject     string
		input       string
		output      string
		errContains []string
	}{
		{
			name:    "things foo exact match",
			subject: "things",
			input:   `{"a":123,    "b":"hello world"}`,
			output:  `{"a":123,"b":"hello world"}`,
		},
		{
			name:    "things bar exact match",
			subject: "things",
			input:   `{"b":"hello world"}`,
			output:  `{"b":"hello world"}`,
		},
		{
			name:    "things neither match",
			subject: "things",
			input:   `{"a":123,    "b":"hello world", "c":"what"}`,
			errContains: []string{
				"unknown field \"c\"",
				"unknown field \"a\"",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, false, &http.Transport{}, time.Minute*10, time.Minute, service.MockResources())
			require.NoError(t, err)

			decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, true, false, &http.Transport{}, false, service.MockResources())
			require.NoError(t, err)

			t.Cleanup(func() {
				_ = encoder.Close(tCtx)
				_ = decoder.Close(tCtx)
			})

			inMsg := service.NewMessage([]byte(test.input))
			inMsg.MetaSetMut("subject", test.subject)

			encodedMsgs, err := encoder.ProcessBatch(tCtx, service.MessageBatch{inMsg})
			require.NoError(t, err)
			require.Len(t, encodedMsgs, 1)
			require.Len(t, encodedMsgs[0], 1)

			encodedMsg := encodedMsgs[0][0]

			if len(test.errContains) > 0 {
				require.Error(t, encodedMsg.GetError())
				for _, errStr := range test.errContains {
					assert.Contains(t, encodedMsg.GetError().Error(), errStr)
				}
				return
			}

			b, err := encodedMsg.AsBytes()
			require.NoError(t, err)

			require.NoError(t, encodedMsg.GetError())
			require.NotEqual(t, test.input, string(b))

			var n any
			require.Error(t, json.Unmarshal(b, &n), "message contents should no longer be valid JSON")

			decodedMsgs, err := decoder.Process(tCtx, encodedMsg)
			require.NoError(t, err)
			require.Len(t, decodedMsgs, 1)

			decodedMsg := decodedMsgs[0]

			b, err = decodedMsg.AsBytes()
			require.NoError(t, err)

			require.NoError(t, decodedMsg.GetError())
			require.JSONEq(t, test.output, string(b))
		})
	}
}

func TestProtobufReferences(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	thingsSchema := `
syntax = "proto3";
package things;

import "stuffs/thething.proto";

message foo {
  float a = 1;
  string b = 2;
  stuffs.bar c = 3;
}
`

	stuffsSchema := `
syntax = "proto3";
package stuffs;

message bar {
  string d = 1;
}
`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
				"references": []any{
					map[string]any{
						"name":    "stuffs/thething.proto",
						"subject": "stuffs/thething.proto",
						"version": 10,
					},
				},
			}), nil
		case "/subjects/stuffs%2Fthething.proto/versions/10", "/schemas/ids/2":
			return mustJBytes(t, map[string]any{
				"id":         2,
				"version":    10,
				"schema":     stuffsSchema,
				"schemaType": "PROTOBUF",
			}), nil
		}
		return nil, nil
	})

	subj, err := service.NewInterpolatedString("things")
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains []string
	}{
		{
			name:   "things foo without bar",
			input:  `{"a":123,    "b":"hello world"}`,
			output: `{"a":123,"b":"hello world"}`,
		},
		{
			name:   "things foo with bar",
			input:  `{"a":123,    "b":"hello world", "c":{"d":"and this"}}`,
			output: `{"a":123, "b":"hello world", "c":{"d":"and this"}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, false, &http.Transport{}, time.Minute*10, time.Minute, service.MockResources())
			require.NoError(t, err)

			decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, true, false, &http.Transport{}, false, service.MockResources())
			require.NoError(t, err)

			t.Cleanup(func() {
				_ = encoder.Close(tCtx)
				_ = decoder.Close(tCtx)
			})

			inMsg := service.NewMessage([]byte(test.input))

			encodedMsgs, err := encoder.ProcessBatch(tCtx, service.MessageBatch{inMsg})
			require.NoError(t, err)
			require.Len(t, encodedMsgs, 1)
			require.Len(t, encodedMsgs[0], 1)

			encodedMsg := encodedMsgs[0][0]

			if len(test.errContains) > 0 {
				require.Error(t, encodedMsg.GetError())
				for _, errStr := range test.errContains {
					assert.Contains(t, encodedMsg.GetError().Error(), errStr)
				}
				return
			}

			b, err := encodedMsg.AsBytes()
			require.NoError(t, err)

			require.NoError(t, encodedMsg.GetError())
			require.NotEqual(t, test.input, string(b))

			var n any
			require.Error(t, json.Unmarshal(b, &n), "message contents should no longer be valid JSON")

			decodedMsgs, err := decoder.Process(tCtx, encodedMsg)
			require.NoError(t, err)
			require.Len(t, decodedMsgs, 1)

			decodedMsg := decodedMsgs[0]

			b, err = decodedMsg.AsBytes()
			require.NoError(t, err)

			require.NoError(t, decodedMsg.GetError())
			require.JSONEq(t, test.output, string(b))
		})
	}
}

func TestProtobufReferencesSpecialCharacters(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	thingsSchema := `
syntax = "proto3";
package things;

import "stuff.Ref0.proto";
import "stuff/Ref1.proto";
import "stuff&Ref2.proto";
import "stuff%Ref3.proto";
import "stuff?Ref4.proto";
import "stuff#Ref5.proto";
import "stuff+Ref6.proto";
import "stuff Ref7.proto";
import "stuffäRef8.proto";

message foo {
  float a = 1;
  string b = 2;
  ref0.Msg0 c = 3;
  ref1.Msg1 d = 4;
  ref2.Msg2 e = 5;
  ref3.Msg3 f = 6;
  ref4.Msg4 g = 7;
  ref5.Msg5 h = 8;
  ref6.Msg6 i = 9;
  ref7.Msg7 j = 10;
  ref8.Msg8 k = 11;
}
`

	refSchemas := map[string]string{
		"stuff.Ref0.proto": `
syntax = "proto3";
package ref0;

message Msg0 {
  string value = 1;
}
`,
		"stuff/Ref1.proto": `
syntax = "proto3";
package ref1;

message Msg1 {
  string value = 1;
}
`,
		"stuff&Ref2.proto": `
syntax = "proto3";
package ref2;

message Msg2 {
  string value = 1;
}
`,
		"stuff%Ref3.proto": `
syntax = "proto3";
package ref3;

message Msg3 {
  string value = 1;
}
`,
		"stuff?Ref4.proto": `
syntax = "proto3";
package ref4;

message Msg4 {
  string value = 1;
}
`,
		"stuff#Ref5.proto": `
syntax = "proto3";
package ref5;

message Msg5 {
  string value = 1;
}
`,
		"stuff+Ref6.proto": `
syntax = "proto3";
package ref6;

message Msg6 {
  string value = 1;
}
`,
		"stuff Ref7.proto": `
syntax = "proto3";
package ref7;

message Msg7 {
  string value = 1;
}
`,
		"stuffäRef8.proto": `
syntax = "proto3";
package ref8;

message Msg8 {
  string value = 1;
}
`,
	}

	type expectedRequest struct {
		subject     string
		path        string
		escapedPath string
	}

	expectedRefRequests := map[string]expectedRequest{
		"/subjects/stuff.Ref0.proto/versions/10": {
			subject:     "stuff.Ref0.proto",
			path:        "/subjects/stuff.Ref0.proto/versions/10",
			escapedPath: "/subjects/stuff.Ref0.proto/versions/10",
		},
		"/subjects/stuff%2FRef1.proto/versions/10": {
			subject:     "stuff/Ref1.proto",
			path:        "/subjects/stuff/Ref1.proto/versions/10",
			escapedPath: "/subjects/stuff%2FRef1.proto/versions/10",
		},
		"/subjects/stuff&Ref2.proto/versions/10": {
			subject:     "stuff&Ref2.proto",
			path:        "/subjects/stuff&Ref2.proto/versions/10",
			escapedPath: "/subjects/stuff&Ref2.proto/versions/10",
		},
		"/subjects/stuff%25Ref3.proto/versions/10": {
			subject:     "stuff%Ref3.proto",
			path:        "/subjects/stuff%Ref3.proto/versions/10",
			escapedPath: "/subjects/stuff%25Ref3.proto/versions/10",
		},
		"/subjects/stuff%3FRef4.proto/versions/10": {
			subject:     "stuff?Ref4.proto",
			path:        "/subjects/stuff?Ref4.proto/versions/10",
			escapedPath: "/subjects/stuff%3FRef4.proto/versions/10",
		},
		"/subjects/stuff%23Ref5.proto/versions/10": {
			subject:     "stuff#Ref5.proto",
			path:        "/subjects/stuff#Ref5.proto/versions/10",
			escapedPath: "/subjects/stuff%23Ref5.proto/versions/10",
		},
		"/subjects/stuff+Ref6.proto/versions/10": {
			subject:     "stuff+Ref6.proto",
			path:        "/subjects/stuff+Ref6.proto/versions/10",
			escapedPath: "/subjects/stuff+Ref6.proto/versions/10",
		},
		"/subjects/stuff%20Ref7.proto/versions/10": {
			subject:     "stuff Ref7.proto",
			path:        "/subjects/stuff Ref7.proto/versions/10",
			escapedPath: "/subjects/stuff%20Ref7.proto/versions/10",
		},
		"/subjects/stuff%C3%A4Ref8.proto/versions/10": {
			subject:     "stuffäRef8.proto",
			path:        "/subjects/stuffäRef8.proto/versions/10",
			escapedPath: "/subjects/stuff%C3%A4Ref8.proto/versions/10",
		},
	}

	seenRefRequests := map[string]int{}

	urlStr := runSchemaRegistryServerURL(t, func(u *url.URL) ([]byte, error) {
		switch u.EscapedPath() {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			assert.Empty(t, u.RawQuery)
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
				"references": []any{
					map[string]any{"name": "stuff.Ref0.proto", "subject": "stuff.Ref0.proto", "version": 10},
					map[string]any{"name": "stuff/Ref1.proto", "subject": "stuff/Ref1.proto", "version": 10},
					map[string]any{"name": "stuff&Ref2.proto", "subject": "stuff&Ref2.proto", "version": 10},
					map[string]any{"name": "stuff%Ref3.proto", "subject": "stuff%Ref3.proto", "version": 10},
					map[string]any{"name": "stuff?Ref4.proto", "subject": "stuff?Ref4.proto", "version": 10},
					map[string]any{"name": "stuff#Ref5.proto", "subject": "stuff#Ref5.proto", "version": 10},
					map[string]any{"name": "stuff+Ref6.proto", "subject": "stuff+Ref6.proto", "version": 10},
					map[string]any{"name": "stuff Ref7.proto", "subject": "stuff Ref7.proto", "version": 10},
					map[string]any{"name": "stuffäRef8.proto", "subject": "stuffäRef8.proto", "version": 10},
				},
			}), nil
		}

		expected, exists := expectedRefRequests[u.EscapedPath()]
		if !exists {
			return nil, nil
		}

		assert.Equal(t, expected.path, u.Path)
		assert.Equal(t, expected.escapedPath, u.EscapedPath())
		assert.Empty(t, u.RawQuery)
		assert.Empty(t, u.Fragment)

		seenRefRequests[u.EscapedPath()]++

		return mustJBytes(t, map[string]any{
			"id":         2,
			"version":    10,
			"schema":     refSchemas[expected.subject],
			"schemaType": "PROTOBUF",
		}), nil
	})

	subj, err := service.NewInterpolatedString("things")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, false, &http.Transport{}, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, true, false, &http.Transport{}, false, service.MockResources())
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = encoder.Close(tCtx)
		_ = decoder.Close(tCtx)
	})

	input := `{
		"a": 123,
		"b": "hello world",
		"c": {"value": "dot"},
		"d": {"value": "slash"},
		"e": {"value": "ampersand"},
		"f": {"value": "percent"},
		"g": {"value": "question"},
		"h": {"value": "hash"},
		"i": {"value": "plus"},
		"j": {"value": "space"},
		"k": {"value": "unicode"}
	}`

	inMsg := service.NewMessage([]byte(input))

	encodedMsgs, err := encoder.ProcessBatch(tCtx, service.MessageBatch{inMsg})
	require.NoError(t, err)
	require.Len(t, encodedMsgs, 1)
	require.Len(t, encodedMsgs[0], 1)

	encodedMsg := encodedMsgs[0][0]
	require.NoError(t, encodedMsg.GetError())

	b, err := encodedMsg.AsBytes()
	require.NoError(t, err)

	var n any
	require.Error(t, json.Unmarshal(b, &n), "message contents should no longer be valid JSON")

	decodedMsgs, err := decoder.Process(tCtx, encodedMsg)
	require.NoError(t, err)
	require.Len(t, decodedMsgs, 1)

	decodedMsg := decodedMsgs[0]
	require.NoError(t, decodedMsg.GetError())

	b, err = decodedMsg.AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, input, string(b))

	for escapedPath := range expectedRefRequests {
		assert.NotZero(t, seenRefRequests[escapedPath], "expected reference request for %s", escapedPath)
	}
}

func runEncoderAgainstInputsMultiple(t testing.TB, urlStr, subject string, inputs [][]byte) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	subj, err := service.NewInterpolatedString(subject)
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, false, &http.Transport{}, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = encoder.Close(tCtx)
	})

	n := 10
	if b, ok := t.(*testing.B); ok {
		b.ReportAllocs()
		b.ResetTimer()
		n = b.N
	}

	for i := 0; i < n; i++ {
		inMsg := service.NewMessage(inputs[i%len(inputs)])
		encodedMsgs, err := encoder.ProcessBatch(tCtx, service.MessageBatch{inMsg})
		require.NoError(t, err)
		require.Len(t, encodedMsgs, 1)
		require.Len(t, encodedMsgs[0], 1)
		require.NoError(t, encodedMsgs[0][0].GetError())
	}
}

func TestProtobufEncodeMultipleMessagesCaching(t *testing.T) {
	thingsSchema := `
syntax = "proto3";
package things;

message foo {
  float a = 1;
  string b = 2;
}

message bar {
  float c = 1;
  string d = 2;
}
`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
			}), nil
		}
		return nil, nil
	})

	t.Run("consistent message", func(t *testing.T) {
		runEncoderAgainstInputsMultiple(t, urlStr, "things", [][]byte{
			[]byte(`{"a":1.23,"b":"foo"}`),
		})
	})

	t.Run("alternating messages", func(t *testing.T) {
		runEncoderAgainstInputsMultiple(t, urlStr, "things", [][]byte{
			[]byte(`{"a":1.23,"b":"foo"}`),
			[]byte(`{"c":2.34,"d":"bar"}`),
		})
	})
}

func BenchmarkProtobufEncodeMultipleMessagesCaching(b *testing.B) {
	thingsSchema := `
syntax = "proto3";
package things;

message foo {
  float a = 1;
  string b = 2;
}

message bar {
  float c = 1;
  string d = 2;
}
`

	urlStr := runSchemaRegistryServer(b, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			return mustJBytes(b, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
			}), nil
		}
		return nil, nil
	})

	b.Run("consistent message", func(b *testing.B) {
		runEncoderAgainstInputsMultiple(b, urlStr, "things", [][]byte{
			[]byte(`{"a":1.23,"b":"foo"}`),
		})
	})

	b.Run("alternating messages", func(b *testing.B) {
		runEncoderAgainstInputsMultiple(b, urlStr, "things", [][]byte{
			[]byte(`{"a":1.23,"b":"foo"}`),
			[]byte(`{"c":2.34,"d":"bar"}`),
		})
	})
}
