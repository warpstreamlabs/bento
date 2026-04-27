package pure_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"

	ipure "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func testTemplateProc(confStr string) (service.Processor, *service.Resources, error) {
	pConf, err := ipure.TemplateProcessorSpec().ParseYAML(confStr, nil)
	if err != nil {
		return nil, nil, err
	}
	mgr := service.MockResources()
	proc, err := ipure.NewTemplateProcessorFromConfig(pConf, mgr)
	return proc, mgr, err
}

func TestTemplateProcessor_Templating(t *testing.T) {
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
			name: "template functions",
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
			name: "template sub template",
			template: `
text: "hello {{ template \"hello\" . }}"
sub_templates:
  hello:
    text: "{{ .foo }}"
`,
			input:    []byte(`{"foo":"world"}`),
			expected: "hello world",
		},
		{
			name:          "template nonexistent functions",
			template:      `text: "{{ nonexistent_function .foo }}"`,
			input:         []byte(`{"foo":"hello","bar":"world"}`),
			expectedError: true,
			errorValue:    "function \"nonexistent_function\" not defined",
		},
		{
			name:     "sprig functions",
			template: `text: "{{ $inc := .inc }}{{ $inc = add1 $inc }}{{$inc}}{{ $inc = add1 $inc }}{{$inc}}{{ $inc = add1 $inc }}{{$inc}}"`,
			input:    []byte(`{"inc":0}`),
			expected: "123",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc, _, err := testTemplateProc(test.template)
			if test.expectedError {
				require.ErrorContains(t, err, test.errorValue)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, proc.Close(t.Context())) })

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

func TestTemplateProcessor_AsFile(t *testing.T) {
	tmpDir := t.TempDir()

	templatePath := filepath.Join(tmpDir, "template.txt")

	err := os.WriteFile(templatePath, []byte("hello {{ template \"hello\" . }}"), 0o644)
	require.NoError(t, err)

	subTemplatePath := filepath.Join(tmpDir, "sub_template.txt")

	err = os.WriteFile(subTemplatePath, []byte("{{ .foo }}"), 0o644)
	require.NoError(t, err)

	proc, _, err := testTemplateProc(`
text: ` + templatePath + `
from_file: true
sub_templates:
  hello:
    text: ` + subTemplatePath + `
    from_file: true
`)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proc.Close(t.Context())) })

	msg := service.NewMessage([]byte(`{"foo":"world"}`))

	batch, err := proc.Process(t.Context(), msg)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	result, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(result))
}
