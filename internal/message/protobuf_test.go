//go:build !wasm

package message

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/warpstreamlabs/bento/internal/message/messagepb"
)

type protoTestCase struct {
	name  string
	batch Batch
	proto *pb.Batch
}

func TestProtobufMessageRoundTrip(t *testing.T) {
	tests := []protoTestCase{
		{
			name: "batch of bytes",
			batch: QuickBatch([][]byte{
				[]byte("hello"),
				[]byte("cruel"),
				[]byte("world"),
			}),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Raw{Raw: []byte("hello")},
					},
					{
						Content: &pb.Part_Raw{Raw: []byte("cruel")},
					},
					{
						Content: &pb.Part_Raw{Raw: []byte("world")},
					},
				},
			},
		},
		{
			name: "batch with metadata",
			batch: func() Batch {
				b := QuickBatch([][]byte{[]byte("data")})
				b[0].MetaSetMut("key1", "value1")
				b[0].MetaSetMut("key2", "42")
				return b
			}(),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Raw{Raw: []byte("data")},
						Metadata: map[string]string{
							"key1": "value1",
							"key2": "42",
						},
					},
				},
			},
		},
		{
			name: "batch with structured data",
			batch: func() Batch {
				b := Batch{NewPart(nil)}
				b[0].SetStructured(map[string]any{
					"name": "Alice",
					"age":  float64(30),
					"tags": []any{"user", "admin"},
				})
				return b
			}(),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Structured{
							Structured: mustNewValue(t, map[string]any{
								"name": "Alice",
								"age":  float64(30),
								"tags": []any{"user", "admin"},
							}),
						},
					},
				},
			},
		},
		{
			name: "batch with errors",
			batch: func() Batch {
				b := QuickBatch([][]byte{[]byte("data")})
				b[0].ErrorSet(errors.New("processing failed"))
				return b
			}(),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Raw{Raw: []byte("data")},
						Error:   &pb.Error{Message: "processing failed"},
					},
				},
			},
		},
		{
			name:  "empty batch",
			batch: Batch{},
			proto: &pb.Batch{
				Parts: []*pb.Part{},
			},
		},
		{
			name: "batch with empty bytes",
			batch: QuickBatch([][]byte{
				[]byte(""),
				[]byte("nonempty"),
				[]byte(""),
			}),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Raw{Raw: []byte("")},
					},
					{
						Content: &pb.Part_Raw{Raw: []byte("nonempty")},
					},
					{
						Content: &pb.Part_Raw{Raw: []byte("")},
					},
				},
			},
		},
		{
			name: "batch with complex structured data",
			batch: func() Batch {
				b := Batch{NewPart(nil)}
				b[0].SetStructured(map[string]any{
					"nested": map[string]any{
						"deep": map[string]any{
							"value": "found",
						},
					},
					"array": []any{float64(1), float64(2), float64(3)},
					"mixed": []any{
						"string",
						float64(42),
						map[string]any{"key": "value"},
					},
				})
				return b
			}(),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Structured{
							Structured: mustNewValue(t, map[string]any{
								"nested": map[string]any{
									"deep": map[string]any{
										"value": "found",
									},
								},
								"array": []any{float64(1), float64(2), float64(3)},
								"mixed": []any{
									"string",
									float64(42),
									map[string]any{"key": "value"},
								},
							}),
						},
					},
				},
			},
		},
		{
			name: "batch with metadata and error",
			batch: func() Batch {
				b := QuickBatch([][]byte{[]byte("data")})
				b[0].MetaSetMut("source", "kafka")
				b[0].ErrorSet(errors.New("connection timeout"))
				return b
			}(),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Raw{Raw: []byte("data")},
						Metadata: map[string]string{
							"source": "kafka",
						},
						Error: &pb.Error{Message: "connection timeout"},
					},
				},
			},
		},
		{
			name: "batch with structured data and metadata",
			batch: func() Batch {
				b := Batch{NewPart(nil)}
				b[0].SetStructured(map[string]any{"id": float64(123)})
				b[0].MetaSetMut("timestamp", "2024-01-01")
				return b
			}(),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Structured{
							Structured: mustNewValue(t, map[string]any{
								"id": float64(123),
							}),
						},
						Metadata: map[string]string{
							"timestamp": "2024-01-01",
						},
					},
				},
			},
		},
	}

	t.Run("batch: serialize -> deserialize -> serialize", func(t *testing.T) {
		testSerializeRoundTrip(t, tests)
	})

	t.Run("parts: serialize -> deserialize -> serialize", func(t *testing.T) {
		testSerializePartsRoundTrip(t, tests)
	})

	t.Run("batch: deserialize -> serialize -> deserialize", func(t *testing.T) {
		testDeserializeRoundTrip(t, tests)
	})

	t.Run("parts: deserialize -> serialize -> deserialize", func(t *testing.T) {
		testDeserializePartsRoundTrip(t, tests)
	})

}

func testSerializeRoundTrip(t *testing.T, tests []protoTestCase) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodedBatchFirstTrip, err := MarshalBatchToProto(tt.batch)
			require.NoError(t, err)

			decodedBatchFirstTrip, err := UnmarshalBatchFromProto(encodedBatchFirstTrip)
			require.NoError(t, err)
			compareBatches(t, tt.batch, decodedBatchFirstTrip)

			encodedBatchSecondTrip, err := MarshalBatchToProto(decodedBatchFirstTrip)
			require.NoError(t, err)

			decodedBatchSecondTrip, err := UnmarshalBatchFromProto(encodedBatchSecondTrip)
			require.NoError(t, err)

			require.Equal(t, encodedBatchFirstTrip, encodedBatchSecondTrip)
			compareBatches(t, decodedBatchFirstTrip, decodedBatchSecondTrip)
		})
	}
}

func testSerializePartsRoundTrip(t *testing.T, tests []protoTestCase) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var firstBatch, secondBatch Batch
			for _, part := range tt.batch {

				encodedPartFirstTrip, err := MarshalToProto(part)
				require.NoError(t, err)

				decodedPartFirstTrip, err := UnmarshalFromProto(encodedPartFirstTrip)
				require.NoError(t, err)

				encodedPartSecondTrip, err := MarshalToProto(decodedPartFirstTrip)
				require.NoError(t, err)

				decodedPartSecondTrip, err := UnmarshalFromProto(encodedPartSecondTrip)
				require.NoError(t, err)

				require.Equal(t, encodedPartFirstTrip, encodedPartSecondTrip)

				firstBatch = append(firstBatch, decodedPartFirstTrip)
				secondBatch = append(secondBatch, decodedPartSecondTrip)
			}

			compareBatches(t, tt.batch, firstBatch)
			compareBatches(t, firstBatch, secondBatch)

		})
	}
}

func testDeserializeRoundTrip(t *testing.T, tests []protoTestCase) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := proto.MarshalOptions{
				Deterministic: true,
			}

			encodedProtoFirstTrip, err := opts.Marshal(tt.proto)
			require.NoError(t, err)

			decodedBatchFirstTrip, err := UnmarshalBatchFromProto(encodedProtoFirstTrip)
			require.NoError(t, err)

			encodedBatchSecondTrip, err := MarshalBatchToProto(decodedBatchFirstTrip)
			require.NoError(t, err)

			batch := &pb.Batch{}
			err = proto.Unmarshal(encodedBatchSecondTrip, batch)
			require.NoError(t, err)

			require.Equal(t, encodedProtoFirstTrip, encodedBatchSecondTrip)
			require.True(t, proto.Equal(tt.proto, batch), "expected %v, got %v", tt.proto, batch)
		})
	}
}

func testDeserializePartsRoundTrip(t *testing.T, tests []protoTestCase) {
	opts := proto.MarshalOptions{
		Deterministic: true,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.proto.Parts) == 0 {
				return
			}

			for i, protoPart := range tt.proto.Parts {
				encodedPartFirstTrip, err := opts.Marshal(protoPart)
				require.NoError(t, err, "Failed to marshal baseline proto part %d", i)

				decodedPartFirstTrip, err := UnmarshalFromProto(encodedPartFirstTrip)
				require.NoError(t, err, "Failed to unmarshal encoded part %d", i)

				encodedPartSecondTrip, err := MarshalToProto(decodedPartFirstTrip)
				require.NoError(t, err, "Failed to marshal back decoded part %d", i)

				require.Equal(t, encodedPartFirstTrip, encodedPartSecondTrip, "Part %d: Byte serialization mismatch after single round trip", i)

				part := &pb.Part{}
				err = proto.Unmarshal(encodedPartSecondTrip, part)
				require.NoError(t, err)

				require.Equal(t, encodedPartFirstTrip, encodedPartSecondTrip)
				require.True(t, proto.Equal(protoPart, part), "expected %v, got %v", tt.proto, part)
			}

		})
	}
}

func compareBatches(t testing.TB, exp Batch, act Batch) {
	t.Helper()

	require.Len(t, exp, len(act))
	for i := range exp {
		comparePart(t, i, exp[i], act[i])
	}
}

func comparePart(t testing.TB, index int, exp *Part, act *Part) {
	require.Equal(t, exp.AsBytes(), act.AsBytes(), "Part %d: Raw Content (AsBytes) mismatch", index)

	expStructured, expErr := exp.AsStructured()
	actStructured, actErr := act.AsStructured()
	if expErr != nil {
		// NOTE: rather just check the error string here since we don't really have a mechanism
		// for converting proto errors to Go errors and preserving types.
		require.EqualError(t, actErr, expErr.Error())
	} else {
		require.NoError(t, actErr)
	}
	require.Equal(t, expStructured, actStructured, "Part %d: Structured Content mismatch", index)

	require.Equal(t, exp.data.metadata, act.data.metadata)
}

func mustNewValue(t *testing.T, v any) *pb.Value {
	t.Helper()
	val, err := toProtoValue(v)
	require.NoError(t, err)
	return val
}

func TestProtoStructuredValueTypes(t *testing.T) {
	ts := time.Date(2026, 11, 2, 12, 30, 0, 123456789, time.UTC)

	tests := []struct {
		name string
		in   any
		out  any
	}{
		{name: "null", in: nil, out: nil},
		{name: "bool", in: true, out: true},
		{name: "string", in: "hello", out: "hello"},
		{name: "int64 beyond float64 precision", in: int64(9007199254740993), out: int64(9007199254740993)},
		{name: "int", in: 5, out: int64(5)},
		{name: "uint64", in: uint64(math.MaxUint64), out: uint64(math.MaxUint64)},
		{name: "float64", in: 1.5, out: 1.5},
		{name: "float32", in: float32(0.5), out: 0.5},
		{name: "bytes", in: []byte("hi"), out: []byte("hi")},
		{name: "decimal", in: json.Number("0.1000000000000000055511151231257827"), out: json.Number("0.1000000000000000055511151231257827")},
		{name: "timestamp", in: ts, out: ts},
		{name: "timestamp in another zone", in: ts.In(time.FixedZone("UTC+2", 2*60*60)), out: ts},
		{
			name: "nested",
			in:   map[string]any{"list": []any{int64(1), "two", map[string]any{"three": []byte("3")}}},
			out:  map[string]any{"list": []any{int64(1), "two", map[string]any{"three": []byte("3")}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			part := NewPart(nil)
			part.SetStructured(test.in)

			encoded, err := MarshalToProto(part)
			require.NoError(t, err)

			decoded, err := UnmarshalFromProto(encoded)
			require.NoError(t, err)

			v, err := decoded.AsStructured()
			require.NoError(t, err)
			require.Equal(t, test.out, v)
		})
	}
}

func TestProtoStructuredValueUnsupported(t *testing.T) {
	part := NewPart(nil)
	part.SetStructured(map[string]any{"ch": make(chan int)})

	_, err := MarshalToProto(part)
	require.EqualError(t, err, "unsupported structured value of type chan int")
}

func TestProtoMetadataAsStrings(t *testing.T) {
	part := NewPart([]byte("data"))
	part.MetaSetMut("int", int64(1000000))
	part.MetaSetMut("float", 1.5)
	part.MetaSetMut("bool", true)

	encoded, err := MarshalToProto(part)
	require.NoError(t, err)

	decoded, err := UnmarshalFromProto(encoded)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"int": "1000000", "float": "1.5", "bool": "true"}, decoded.data.metadata)
}
