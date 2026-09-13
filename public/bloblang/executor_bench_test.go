package bloblang

import (
	"testing"
)

// benchValue is a nested map[string]any activation of the shape produced by decoding a JSON
// document: string keys throughout, including keys that look numeric but are map keys rather
// than array indices.
func benchValue() map[string]any {
	return map[string]any{
		"rec": map[string]any{
			"983": map[string]any{"002": "BSK", "011": "A", "202": "H304", "010": "DE000X"},
			"985": map[string]any{"307": "RE"},
			"986": map[string]any{"010": "DE000X", "249": "2", "234": "x"},
		},
		"barrier": map[string]any{"SP": map[string]any{"224": "31.12.2030"}},
		"ulcount": 3,
	}
}

// Mappings covering the shapes that dominate a compile-once/evaluate-many workload: a compiled
// mapping run against millions of values via the public Executor.
var benchMappings = []struct {
	name    string
	mapping string
}{
	{"field_read", `root = this.rec."983"."002"`},
	{"predicate", `root = this.rec."983"."002" != "FUT"`},
	{"and2", `root = this.rec."985"."307" == "RE" && this.rec."983"."202" == "H304"`},
	{"and3", `root = this.rec."986"."010" == this.rec."983"."010" && this.rec."986"."249" == "2" && this.rec."986"."234" != ""`},
	{"contains", `root = ["A","E"].contains(this.rec."983"."011")`},
	{"exists", `root = this.barrier.exists("SP")`},
	{"multi_assign", `root.a = this.rec."983"."002"
root.b = this.rec."985"."307"
root.c = this.rec."986"."249"`},
}

func BenchmarkExecutorQuery(b *testing.B) {
	for _, bm := range benchMappings {
		exe, err := GlobalEnvironment().Parse(bm.mapping)
		if err != nil {
			b.Fatalf("%v: %v", bm.name, err)
		}
		val := benchValue()
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exe.Query(val); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExecutorOverlay(b *testing.B) {
	exe, err := GlobalEnvironment().Parse(`root.a = this.rec."983"."002"`)
	if err != nil {
		b.Fatal(err)
	}
	val := benchValue()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		onto := any(map[string]any{})
		if err := exe.Overlay(val, &onto); err != nil {
			b.Fatal(err)
		}
	}
}

// A mapping that does declare variables, so the vars-map allocation cannot simply be removed
// without a regression showing up here.
func BenchmarkExecutorQueryWithVars(b *testing.B) {
	exe, err := GlobalEnvironment().Parse(`let t = this.rec."983"."002"
root = $t != "FUT"`)
	if err != nil {
		b.Fatal(err)
	}
	val := benchValue()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exe.Query(val); err != nil {
			b.Fatal(err)
		}
	}
}
