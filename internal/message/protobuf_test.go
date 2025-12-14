package message

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pb "github.com/warpstreamlabs/bento/internal/message/messagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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
				b[0].MetaSetMut("key2", float64(42))
				return b
			}(),
			proto: &pb.Batch{
				Parts: []*pb.Part{
					{
						Content: &pb.Part_Raw{Raw: []byte("data")},
						Metadata: mustNewStruct(t, map[string]any{
							"key1": "value1",
							"key2": float64(42),
						}),
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
						Error:   "processing failed",
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
						Metadata: mustNewStruct(t, map[string]any{
							"source": "kafka",
						}),
						Error: "connection timeout",
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
						Metadata: mustNewStruct(t, map[string]any{
							"timestamp": "2024-01-01",
						}),
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

func mustNewStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	require.NoError(t, err)
	return s
}

func mustNewValue(t *testing.T, v any) *structpb.Value {
	t.Helper()
	val, err := structpb.NewValue(v)
	require.NoError(t, err)
	return val
}
