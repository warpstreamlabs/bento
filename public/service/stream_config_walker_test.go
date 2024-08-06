package service_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/public/service"
)

func getMockEnv(t testing.TB) *service.Environment {
	t.Helper()

	svc := service.NewEmptyEnvironment()

	require.NoError(t, svc.RegisterInput("kafka", service.NewConfigSpec().Fields(
		service.NewStringListField("address"),
		service.NewStringListField("topics"),
	), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return nil, errors.New("nope")
	}))

	require.NoError(t, svc.RegisterInput("generate", service.NewConfigSpec().Fields(
		service.NewStringField("mapping"),
	), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return nil, errors.New("nope")
	}))

	require.NoError(t, svc.RegisterInput("dynamic", service.NewConfigSpec().Fields(
		service.NewInputMapField("inputs"),
	), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return nil, errors.New("nope")
	}))

	require.NoError(t, svc.RegisterOutput("nats", service.NewConfigSpec().Fields(
		service.NewStringListField("urls"),
		service.NewStringField("subject"),
	), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
		return nil, 0, errors.New("nope")
	}))

	require.NoError(t, svc.RegisterProcessor("compress", service.NewConfigSpec().Fields(
		service.NewStringField("algorithm"),
	), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return nil, errors.New("nope")
	}))

	require.NoError(t, svc.RegisterProcessor("switch", service.NewConfigSpec().Fields(
		service.NewObjectListField("",
			service.NewStringField("check"),
			service.NewProcessorListField("processors"),
		),
	), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return nil, errors.New("nope")
	}))

	return svc
}

func TestStreamConfigWalkerYAML(t *testing.T) {
	mockEnv := getMockEnv(t)

	tests := []struct {
		name   string
		input  string
		output map[string][2]string
	}{
		{
			name: "simple input and output",
			input: `
input:
  label: a
  kafka:
    addresses: [ "foo", "bar" ]
    topics: [ "baz" ]

output:
  label: b
  nats:
    urls: [ nats://127.0.0.1:4222 ]
    subject: benthos_messages
    max_in_flight: 1
`,
			output: map[string][2]string{
				"input":  {"a", "kafka"},
				"output": {"b", "nats"},
			},
		},
		{
			name: "switch processor",
			input: `
pipeline:
  processors:
  - label: a
    switch:
      - check: 'root = "foobar"'
        processors:
          - label: b
            compress:
              algorithm: meow1
      - check: 'root = "foobar2"'
        processors:
          - label: c
            compress:
              algorithm: meow2
          - label: d
            compress:
              algorithm: meow3
`,
			output: map[string][2]string{
				"pipeline.processors.0":                       {"a", "switch"},
				"pipeline.processors.0.switch.0.processors.0": {"b", "compress"},
				"pipeline.processors.0.switch.1.processors.0": {"c", "compress"},
				"pipeline.processors.0.switch.1.processors.1": {"d", "compress"},
			},
		},
		{
			name: "nested inputs and processors",
			input: `
input:
  label: a
  dynamic:
    inputs:
      foo:
        label: b
        kafka:
          addresses: [ "foo", "bar" ]
          topics: [ "baz" ]
        processors:
         - label: c
           compress:
             algorithm: meow1
      bar:
        label: d
        kafka:
          addresses: [ "foo", "bar" ]
          topics: [ "baz" ]
  processors:
    - label: e
      switch:
      - check: 'root = "foobar"'
        processors:
          - label: f
            compress:
              algorithm: meow1
      - check: 'root = "foobar2"'
        processors:
          - label: g
            compress:
              algorithm: meow2
          - label: h
            compress:
              algorithm: meow3
`,
			output: map[string][2]string{
				"input":                                    {"a", "dynamic"},
				"input.dynamic.inputs.foo":                 {"b", "kafka"},
				"input.dynamic.inputs.foo.processors.0":    {"c", "compress"},
				"input.dynamic.inputs.bar":                 {"d", "kafka"},
				"input.processors.0":                       {"e", "switch"},
				"input.processors.0.switch.0.processors.0": {"f", "compress"},
				"input.processors.0.switch.1.processors.0": {"g", "compress"},
				"input.processors.0.switch.1.processors.1": {"h", "compress"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := map[string][2]string{}

			require.NoError(t, mockEnv.CoreConfigSchema("", "").
				NewStreamConfigWalker().
				WalkComponentsYAML([]byte(test.input), func(w *service.WalkedComponent) error {
					res[w.Path] = [2]string{w.Label, w.Name}
					return nil
				}))

			assert.Equal(t, test.output, res)
		})
	}
}

func TestWalkYAMLFragmented(t *testing.T) {
	mockEnv := getMockEnv(t)

	input := `
input:
  label: a
  dynamic:
    inputs:
      foo:
        label: b
        kafka:
          addresses: [ "foo", "bar" ]
          topics: [ "baz" ]
        processors:
         - label: c
           compress:
             algorithm: meow1
      bar:
        label: d
        kafka:
          addresses: [ "foo", "bar" ]
          topics: [ "baz" ]
  processors:
    - label: e
      switch:
      - check: 'root = "foobar"'
        processors:
          - label: f
            compress:
              algorithm: meow1
      - check: 'root = "foobar2"'
        processors:
          - label: g
            compress:
              algorithm: meow2
          - label: h
            compress:
              algorithm: meow3
`

	res := map[string][2]string{}

	var walkFunc func(w *service.WalkedComponent) error
	walkFunc = func(w *service.WalkedComponent) error {
		res[w.Path] = [2]string{w.Label, w.Name}
		if err := w.WalkComponentsYAML(walkFunc); err != nil {
			return err
		}
		return docs.ErrSkipChildComponents
	}

	require.NoError(t, mockEnv.CoreConfigSchema("", "").
		NewStreamConfigWalker().
		WalkComponentsYAML([]byte(input), walkFunc))

	assert.Equal(t, map[string][2]string{
		"input":                                    {"a", "dynamic"},
		"input.dynamic.inputs.foo":                 {"b", "kafka"},
		"input.dynamic.inputs.foo.processors.0":    {"c", "compress"},
		"input.dynamic.inputs.bar":                 {"d", "kafka"},
		"input.processors.0":                       {"e", "switch"},
		"input.processors.0.switch.0.processors.0": {"f", "compress"},
		"input.processors.0.switch.1.processors.0": {"g", "compress"},
		"input.processors.0.switch.1.processors.1": {"h", "compress"},
	}, res)
}

func TestWalkYAMLLines(t *testing.T) {
	mockEnv := getMockEnv(t)

	input := `
input:
  label: a
  dynamic:
    inputs:
      foo:
        label: b
        kafka:
          addresses: [ "foo", "bar" ]
          topics: [ "baz" ]
        processors:
         - label: c
           compress:
             algorithm: meow1
      bar:
        label: d
        kafka:
          addresses: [ "foo", "bar" ]
          topics: [ "baz" ]
  processors:
    - label: e
      switch:
      - check: 'root = "foobar"'
        processors:
          - label: f
            compress:
              algorithm: meow1
      - check: 'root = "foobar2"'
        processors:
          - label: g
            compress:
              algorithm: meow2
          - label: h
            compress:
              algorithm: meow3
`

	res := map[string][2]int{}

	require.NoError(t, mockEnv.CoreConfigSchema("", "").
		NewStreamConfigWalker().
		WalkComponentsYAML([]byte(input), func(w *service.WalkedComponent) error {
			res[w.Path] = [2]int{w.LineStart, w.LineEnd}
			return nil
		}))

	assert.Equal(t, map[string][2]int{
		"input":                                    {3, 35},
		"input.dynamic.inputs.foo":                 {7, 14},
		"input.dynamic.inputs.foo.processors.0":    {12, 14},
		"input.dynamic.inputs.bar":                 {16, 19},
		"input.processors.0":                       {21, 35},
		"input.processors.0.switch.0.processors.0": {25, 27},
		"input.processors.0.switch.1.processors.0": {30, 32},
		"input.processors.0.switch.1.processors.1": {33, 35},
	}, res)
}
