package field_test

import (
	"testing"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/field"
	"github.com/warpstreamlabs/bento/internal/message"
)

func benchExpression(b *testing.B, expr string) *field.Expression {
	b.Helper()
	e, err := bloblang.GlobalEnvironment().NewField(expr)
	if err != nil {
		b.Fatalf("parse %q: %v", expr, err)
	}
	return e
}

func benchMessage() field.Message {
	p := message.NewPart([]byte(`{"id":"0b3f2a9c-7e41-4d6a-9f2b-1c8d5e7a3b40","user":{"name":"bob"},"n":42}`))
	p.MetaSetMut("kafka_key", "user-10045")
	p.MetaSetMut("kafka_topic", "events.ingest")
	return message.Batch{p}
}

// benchExpressions are shaped after the interpolations that components
// actually resolve once per message, such as output topics, keys and paths.
var benchExpressions = []struct {
	name string
	expr string
}{
	{"StaticOnly", `events-ingest-topic`},
	{"MetaOnly", `${! meta("kafka_key") }`},
	{"JSONFieldOnly", `${! json("id") }`},
	{"PrefixedMeta", `topic-${! meta("kafka_topic") }`},
	{"MultiPart", `${! meta("kafka_topic") }/${! json("user.name") }/${! json("id") }.json`},
	{"NumberField", `${! json("n") }`},
}

func BenchmarkExpressionString(b *testing.B) {
	msg := benchMessage()
	for _, tc := range benchExpressions {
		b.Run(tc.name, func(b *testing.B) {
			e := benchExpression(b, tc.expr)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := e.String(0, msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExpressionBytes(b *testing.B) {
	msg := benchMessage()
	for _, tc := range benchExpressions {
		b.Run(tc.name, func(b *testing.B) {
			e := benchExpression(b, tc.expr)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := e.Bytes(0, msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// LongPath is a realistic object-storage path: its static segments alone
// exceed the 64 byte bootstrap buffer inside bytes.Buffer.
func BenchmarkExpressionLongPath(b *testing.B) {
	msg := benchMessage()
	const expr = `warehouse/ingest/v2/region=eu-west-1/topic=${! meta("kafka_topic") }/user=${! json("user.name") }/dt=2026-10-22/part-${! json("id") }-${! json("n") }.json.gz`
	b.Run("String", func(b *testing.B) {
		e := benchExpression(b, expr)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := e.String(0, msg); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Bytes", func(b *testing.B) {
		e := benchExpression(b, expr)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := e.Bytes(0, msg); err != nil {
				b.Fatal(err)
			}
		}
	})
}
