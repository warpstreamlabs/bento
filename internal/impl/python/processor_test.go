package python

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestPythonProcessor(t *testing.T) {
	type testCase struct {
		name    string
		script  string
		imports []string
		input   string
		output  string
		wantErr bool
		errMsg  string
	}

	tests := []testCase{
		{
			name: "simple passthrough",
			script: `
root = this
`,
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar"}`,
		},
		{
			name: "add field",
			script: `
root = this
root["added"] = "value"
`,
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar", "added": "value"}`,
		},
		{
			name: "transform field",
			script: `
root = this
root["name"] = this["first_name"] + " " + this["last_name"]
`,
			input:  `{"first_name": "John", "last_name": "Doe"}`,
			output: `{"first_name": "John", "last_name": "Doe", "name": "John Doe"}`,
		},
		{
			name: "filter message - set None",
			script: `
if this.get("status") == "active":
    root = this
else:
    root = None
`,
			input:  `{"status": "inactive"}`,
			output: `null`,
		},
		{
			name: "filter message - keep active",
			script: `
if this.get("status") == "active":
    root = this
else:
    root = None
`,
			input:  `{"status": "active", "id": 123}`,
			output: `{"status": "active", "id": 123}`,
		},
		{
			name:    "math import",
			imports: []string{"math"},
			script: `
root = this
root["rounded"] = math.ceil(this["value"])
`,
			input:  `{"value": 3.2}`,
			output: `{"value": 3.2, "rounded": 4}`,
		},
		{
			name:    "json import",
			imports: []string{"json"},
			script: `
root = {"parsed": json.loads(this["data"])}
`,
			input:  `{"data": "{\"nested\": true}"}`,
			output: `{"parsed": {"nested": true}}`,
		},
		{
			name: "array operations",
			script: `
root = this
root["count"] = len(this["items"])
root["first"] = this["items"][0] if this["items"] else None
`,
			input:  `{"items": [1, 2, 3]}`,
			output: `{"items": [1, 2, 3], "count": 3, "first": 1}`,
		},
		{
			name: "empty array",
			script: `
root = this
root["count"] = len(this["items"])
root["first"] = this["items"][0] if this["items"] else None
`,
			input:  `{"items": []}`,
			output: `{"items": [], "count": 0, "first": null}`,
		},
		{
			name: "handle missing key gracefully",
			script: `
root = this
root["value"] = this.get("missing", "default")
`,
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar", "value": "default"}`,
		},
		{
			name: "string operations",
			script: `
root = this
root["upper"] = this["text"].upper()
root["length"] = len(this["text"])
`,
			input:  `{"text": "hello"}`,
			output: `{"text": "hello", "upper": "HELLO", "length": 5}`,
		},
		{
			name: "error - key error",
			script: `
root = this[0]
`,
			input:   `{"foo": "bar"}`,
			wantErr: true,
			errMsg:  "KeyError",
		},
		{
			name: "error - type error",
			script: `
root = this["value"] + "text"
`,
			input:   `{"value": 123}`,
			wantErr: true,
			errMsg:  "TypeError: unsupported operand type(s) for +",
		},
		{
			name: "error - undefined variable",
			script: `
root = undefined_var
`,
			input:   `{"foo": "bar"}`,
			wantErr: true,
			errMsg:  "NameError",
		},
		{
			name: "number input",
			script: `
root = this * 2
`,
			input:  `5`,
			output: `10`,
		},
		{
			name: "string input",
			script: `
root = this.upper()
`,
			input:  `"hello"`,
			output: `"HELLO"`,
		},
		{
			name: "array input",
			script: `
root = [x * 2 for x in this]
`,
			input:  `[1, 2, 3]`,
			output: `[2, 4, 6]`,
		},
		{
			name: "complex transformation",
			script: `
root = {
    "total": sum(item["price"] for item in this["items"]),
    "count": len(this["items"]),
    "items": [item["name"] for item in this["items"]]
}
`,
			input:  `{"items": [{"name": "apple", "price": 1.5}, {"name": "banana", "price": 0.5}]}`,
			output: `{"total": 2.0, "count": 2, "items": ["apple", "banana"]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			indent := "  "
			indentedScript := indent + strings.ReplaceAll(strings.TrimSpace(test.script), "\n", "\n"+indent)
			yamlConfig := fmt.Sprintf(`
script: |
%s
imports: %v
`, indentedScript, test.imports)

			conf, err := pythonProcessorSpec().ParseYAML(yamlConfig, nil)
			require.NoError(t, err)

			proc, err := newPythonProcessor(conf, service.MockResources())
			require.NoError(t, err)
			defer proc.Close(context.Background())

			msgs, err := proc.Process(context.Background(), service.NewMessage([]byte(test.input)))
			require.NoError(t, err)
			require.Len(t, msgs, 1)

			if test.wantErr {
				errMsg := msgs[0].GetError()
				require.Error(t, errMsg)
				if test.errMsg != "" {
					assert.Contains(t, errMsg.Error(), test.errMsg)
				}
				return
			}

			require.NoError(t, msgs[0].GetError())

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(mBytes))
		})
	}
}
