package jsonschema_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	jsonschema "github.com/xeipuuv/gojsonschema"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
	ijschema "github.com/warpstreamlabs/bento/internal/jsonschema"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func testEnvWithPlugins(t testing.TB) *bundle.Environment {
	t.Helper()

	env := bundle.GlobalEnvironment.Clone()

	require.NoError(t, env.InputAdd(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return nil, errors.New("nope")
	}, docs.ComponentSpec{
		Name: "testinput",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("woof", "", "WOOF"),
		),
	}))

	require.NoError(t, env.ProcessorAdd(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		return nil, errors.New("nope")
	}, docs.ComponentSpec{
		Name: "testprocessor",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldBloblang("mapfield", ""),
		),
	}))

	return env
}

func TestJSONSchema(t *testing.T) {
	env := testEnvWithPlugins(t)

	testSchema, err := ijschema.Marshal(config.Spec(), env)
	require.NoError(t, err)

	schema, err := jsonschema.NewSchema(jsonschema.NewStringLoader(string(testSchema)))
	require.NoError(t, err)

	res, err := schema.Validate(jsonschema.NewGoLoader(map[string]any{
		"input": map[string]any{
			"testinput": map[string]any{
				"woof": "uhhhhh, woof!",
			},
			"processors": []any{
				map[string]any{
					"testprocessor": map[string]any{
						"mapfield": "hello world",
					},
				},
			},
		},
	}))
	require.NoError(t, err)
	require.Empty(t, res.Errors())
}
