package io_test

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

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/internal/impl/io"
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

	env := bloblang.GlobalEnvironment()
	env.WalkFunctions(func(name string, view *bloblang.FunctionView) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			spec := view.TemplateData()
			for i, e := range spec.Examples {
				if e.SkipTesting {
					continue
				}

				m, err := env.Parse(e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := service.NewMessage([]byte(io[0]))
					textMap := propagation.MapCarrier{
						"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					}
					otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))

					textProp := otel.GetTextMapPropagator()
					otelCtx := textProp.Extract(msg.Context(), textMap)
					pCtx, _ := noop.NewTracerProvider().Tracer("blobby").Start(otelCtx, "test")
					msg = msg.WithContext(pCtx)

					p, err := msg.BloblangQuery(m)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else {
						require.NoError(t, err)

						pBytes, err := p.AsBytes()
						require.NoError(t, err)

						assert.Equal(t, exp, string(pBytes), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
		})
	})
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

	env := bloblang.GlobalEnvironment()
	env.WalkMethods(func(name string, view *bloblang.MethodView) {
		spec := view.TemplateData()
		t.Run(spec.Name, func(t *testing.T) {
			t.Parallel()
			for i, e := range spec.Examples {
				if e.SkipTesting {
					continue
				}

				m, err := env.Parse(e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := service.NewMessage([]byte(io[0]))
					p, err := msg.BloblangQuery(m)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else if exp == "<Message deleted>" {
						require.NoError(t, err)
						require.Nil(t, p)
					} else {
						require.NoError(t, err)

						pBytes, err := p.AsBytes()
						require.NoError(t, err)

						assert.Equal(t, exp, string(pBytes), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
			for _, target := range spec.Categories {
				for i, e := range target.Examples {
					if e.SkipTesting {
						continue
					}

					m, err := env.Parse(e.Mapping)
					require.NoError(t, err)

					for j, io := range e.Results {
						msg := service.NewMessage([]byte(io[0]))
						p, err := msg.BloblangQuery(m)
						exp := io[1]
						if strings.HasPrefix(exp, "Error(") {
							exp = exp[7 : len(exp)-2]
							require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
						} else if exp == "<Message deleted>" {
							require.NoError(t, err)
							require.Nil(t, p)
						} else {
							require.NoError(t, err)

							pBytes, err := p.AsBytes()
							require.NoError(t, err)

							assert.Equal(t, exp, string(pBytes), fmt.Sprintf("%v-%v", i, j))
						}
					}
				}
			}
		})
	})
}
