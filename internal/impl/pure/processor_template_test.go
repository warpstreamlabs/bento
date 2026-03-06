package pure

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func testTemplateProc(confStr string) (service.Processor, error) {
	pConf, err := templateProcSpec().ParseYAML(confStr, nil)
	if err != nil {
		return nil, err
	}
	return newTemplateProcessor(pConf, service.MockResources())
}

func TestTemplateProcessor(t *testing.T) {
	tests := []struct {
		name          string
		template      string
		input         []byte
		metaKV        map[string]string
		expected      string
		expectedError bool
		errorValue    string
	}{
		{
			name:     "basic template",
			template: `text: "{{ .foo }} - {{ meta \"meta_foo\" }}"`,
			input:    []byte(`{"foo":"bar"}`),
			metaKV:   map[string]string{"meta_foo": "meta_bar"},
			expected: "bar - meta_bar",
		},
		{
			name:     "range template",
			template: `text: "{{ range .items }}{{ .name }}: {{ .value }}{{ end }}"`,
			input:    []byte(`{"items":[{"name":"foo","value":1},{"name":"bar","value":2}]}`),
			expected: "foo: 1bar: 2",
		},
		{
			name:     "meta access with values",
			template: `text: "{{ meta \"key1\" }} - {{ meta \"key2\" }}"`,
			input:    []byte(`{}`),
			metaKV:   map[string]string{"key1": "value1", "key2": "value2"},
			expected: "value1 - value2",
		},
		{
			name:     "meta access with nonexistent key",
			template: `text: "{{ meta \"nonexistent\" }}"`,
			input:    []byte(`{}`),
			expected: "<no value>",
		},
		{
			name:     "field access with nonexistent key",
			template: `text: "{{ .nonexistent }}"`,
			input:    []byte(`{}`),
			expected: "<no value>",
		},
		{
			name:          "invalid template syntax",
			template:      `text: "{{ invalid syntax"`,
			expectedError: true,
			errorValue:    "Failed to parse template",
		},
		{
			name: "template functions - basic usage",
			template: `
text: "{{ .foo }} - {{ uppercase .bar }}"
functions:
  uppercase: |
    root = this.uppercase()
`,
			input:    []byte(`{"foo":"hello","bar":"world"}`),
			expected: "hello - WORLD",
		},
		{
			name:          "template functions - error handling",
			template:      `text: "{{ nonexistent_function .foo }}"`,
			input:         []byte(`{"foo":"hello","bar":"world"}`),
			expectedError: true,
			errorValue:    "function \"nonexistent_function\" not defined",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc, err := testTemplateProc(test.template)
			if test.expectedError {
				require.ErrorContains(t, err, test.errorValue)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, proc.Close(context.Background())) })

			msg := service.NewMessage(test.input)
			for k, v := range test.metaKV {
				msg.MetaSetMut(k, v)
			}

			batch, err := proc.Process(t.Context(), msg)
			require.NoError(t, err)
			require.Len(t, batch, 1)

			msg = batch[0]

			for k, v := range test.metaKV {
				m, ok := msg.MetaGetMut(k)
				require.True(t, ok)
				assert.Equal(t, m, v)
			}

			result, err := batch[0].AsBytes()
			require.NoError(t, err)
			assert.Equal(t, test.expected, string(result))
		})
	}
}
