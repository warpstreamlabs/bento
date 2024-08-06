package docs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/docs"
)

func getMockProv(t testing.TB) *docs.MappedDocsProvider {
	t.Helper()

	mockProv := docs.NewMappedDocsProvider()
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "kafka",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("addresses", "").Array(),
			docs.FieldString("topics", "").Array(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "generate",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("mapping", ""),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "dynamic",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInput("inputs", "").Map(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "nats",
		Type: docs.TypeOutput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("urls", "").Array(),
			docs.FieldString("subject", ""),
			docs.FieldInt("max_in_flight", ""),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "compress",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("algorithm", ""),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "workflow",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("order", "").ArrayOfArrays(),
		),
	})
	mockProv.RegisterDocs(docs.ComponentSpec{
		Name: "switch",
		Type: docs.TypeProcessor,
		Config: docs.FieldComponent().Array().WithChildren(
			docs.FieldString("check", ""),
			docs.FieldProcessor("processors", "").Array(),
		),
	})
	return mockProv
}

func TestWalkYAML(t *testing.T) {
	mockProv := getMockProv(t)

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
			input := &yaml.Node{}
			require.NoError(t, yaml.Unmarshal([]byte(test.input), input))

			res := map[string][2]string{}

			require.NoError(t, configSpec.WalkComponentsYAML(docs.WalkComponentConfig{
				Provider: mockProv,
				Func: func(c docs.WalkedComponent) error {
					res[c.Path] = [2]string{c.Label, c.Name}
					return nil
				},
			}, input))

			assert.Equal(t, test.output, res)
		})
	}
}

func TestWalkYAMLFragmented(t *testing.T) {
	mockProv := getMockProv(t)

	input := &yaml.Node{}
	require.NoError(t, yaml.Unmarshal([]byte(`
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
`), input))

	res := map[string][2]string{}

	var walkFunc docs.WalkComponentFunc
	walkFunc = func(c docs.WalkedComponent) error {
		res[c.Path] = [2]string{c.Label, c.Name}
		if err := c.WalkComponentsYAML(walkFunc); err != nil {
			return err
		}
		return docs.ErrSkipChildComponents
	}

	require.NoError(t, configSpec.WalkComponentsYAML(docs.WalkComponentConfig{
		Provider: mockProv,
		Func:     walkFunc,
	}, input))

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
	mockProv := getMockProv(t)

	input := &yaml.Node{}
	require.NoError(t, yaml.Unmarshal([]byte(`
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
`), input))

	res := map[string][2]int{}

	require.NoError(t, configSpec.WalkComponentsYAML(docs.WalkComponentConfig{
		Provider: mockProv,
		Func: func(c docs.WalkedComponent) error {
			res[c.Path] = [2]int{c.LineStart, c.LineEnd}
			return nil
		},
	}, input))

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
