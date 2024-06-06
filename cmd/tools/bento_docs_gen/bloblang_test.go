package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/warpstreamlabs/bento/v1/internal/bloblang"
	"github.com/warpstreamlabs/bento/v1/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/v1/internal/message"
	"github.com/warpstreamlabs/bento/v1/internal/tracing"

	_ "github.com/warpstreamlabs/bento/v1/public/components/all"
)

func TestFunctionExamples(t *testing.T) {
	tmpJSONFile, err := os.CreateTemp("", "bento_bloblang_functions_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(tmpJSONFile.Name())
	})

	_, err = tmpJSONFile.WriteString(`{"foo":"bar"}`)
	require.NoError(t, err)

	key := "BENTO_TEST_BLOBLANG_FILE"
	t.Setenv(key, tmpJSONFile.Name())

	for _, spec := range query.FunctionDocs() {
		spec := spec
		t.Run(spec.Name, func(t *testing.T) {
			t.Parallel()
			for i, e := range spec.Examples {
				if e.SkipTesting {
					continue
				}
				m, err := bloblang.GlobalEnvironment().NewMapping(e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := message.Batch{message.NewPart([]byte(io[0]))}
					textMap := map[string]any{
						"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					}
					otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
					require.NoError(t, tracing.InitSpansFromParentTextMap(noop.NewTracerProvider(), "test", textMap, msg))

					p, err := m.MapPart(0, msg)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else {
						require.NoError(t, err)
						assert.Equal(t, exp, string(p.AsBytes()), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
		})
	}
}

func TestMethodExamples(t *testing.T) {
	tmpJSONFile, err := os.CreateTemp("", "bento_bloblang_methods_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(tmpJSONFile.Name())
	})

	_, err = tmpJSONFile.WriteString(`
{
  "type":"object",
  "properties":{
    "foo":{
      "type":"string"
    }
  }
}`)
	require.NoError(t, err)

	key := "BENTO_TEST_BLOBLANG_SCHEMA_FILE"
	t.Setenv(key, tmpJSONFile.Name())

	for _, spec := range query.MethodDocs() {
		spec := spec
		t.Run(spec.Name, func(t *testing.T) {
			t.Parallel()
			for i, e := range spec.Examples {
				if e.SkipTesting {
					continue
				}
				m, err := bloblang.GlobalEnvironment().NewMapping(e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := message.QuickBatch([][]byte{[]byte(io[0])})
					p, err := m.MapPart(0, msg)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else if exp == "<Message deleted>" {
						require.NoError(t, err)
						require.Nil(t, p)
					} else {
						require.NoError(t, err)
						assert.Equal(t, exp, string(p.AsBytes()), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
			for _, target := range spec.Categories {
				for i, e := range target.Examples {
					if e.SkipTesting {
						continue
					}
					m, err := bloblang.GlobalEnvironment().NewMapping(e.Mapping)
					require.NoError(t, err)

					for j, io := range e.Results {
						msg := message.QuickBatch([][]byte{[]byte(io[0])})
						p, err := m.MapPart(0, msg)
						exp := io[1]
						if strings.HasPrefix(exp, "Error(") {
							exp = exp[7 : len(exp)-2]
							require.EqualError(t, err, exp, fmt.Sprintf("%v-%v-%v", target.Category, i, j))
						} else {
							require.NoError(t, err)
							assert.Equal(t, exp, string(p.AsBytes()), fmt.Sprintf("%v-%v-%v", target.Category, i, j))
						}
					}
				}
			}
		})
	}
}
