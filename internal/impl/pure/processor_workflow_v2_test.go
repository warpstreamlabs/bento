package pure_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestWorkflowV2CyclicalDAG(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
workflow_v2:
  branches:
    A:
      dependency_list: ["B"]
      processors:
          - noop: {} 
    B:
      dependency_list: ["A"]
      processors:
          - noop: {} 
`)
	require.NoError(t, err)

	branchConf, err := testutil.ProcessorFromYAML(`
branch:
  request_map: root = this
  processors:
      - bloblang: root = this
  result_map: root = this
`)
	require.NoError(t, err)

	mgr := newMockProcProvider(t, map[string]processor.Config{
		"baz": branchConf,
	})

	_, err = mgr.NewProcessor(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to init processor <no label>: dependency_lists have a cyclical dependency")
}

func TestWorkflowV2DependencyListRefNonExistentBranch(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
workflow_v2:
  branches:
    A:
      processors:
          - noop: {} 
    B:
      dependency_list: ["C"]
      processors:
          - noop: {} 
`)
	require.NoError(t, err)

	branchConf, err := testutil.ProcessorFromYAML(`
branch:
  request_map: root = this
  processors:
    - bloblang: root = this
  result_map: root = this
`)
	require.NoError(t, err)

	mgr := newMockProcProvider(t, map[string]processor.Config{
		"baz": branchConf,
	})

	_, err = mgr.NewProcessor(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to init processor <no label>: dependency \"C\" in the dependency_list of branch \"B\" is not a branch")
}

func TestWorkflowsV2(t *testing.T) {
	msg := func(content string, meta ...string) mockMsg {
		t.Helper()
		m := mockMsg{
			content: content,
			meta:    map[string]string{},
		}
		for i, v := range meta {
			if i%2 == 1 {
				m.meta[meta[i-1]] = v
			}
		}
		return m
	}

	// To make configs simpler break branches down into five mappings, the
	// branch name, dependency_list, request_map, bloblang mapping, result map.
	tests := []struct {
		branches [][5]string
		input    []mockMsg
		output   []mockMsg
		err      string
	}{
		{
			branches: [][5]string{
				{
					"A",
					`[]`,
					`root = this
					root.english = "hello"`,
					"root = this",
					"root = this",
				},
				{
					"B",
					`["A"]`,
					`root = this
					root.french = "bonjour"`,
					"root = this",
					"root = this",
				},
				{
					"C",
					`["A"]`,
					`root = this
					root.thai = "สวัสดี"`,
					"root = this",
					"root = this",
				},
				{
					"D",
					`["B", "C"]`,
					`root = this
					root.chinese = "你好"`,
					"root = this",
					"root = this",
				},
			},
			input: []mockMsg{
				msg(`{}`),
			},
			output: []mockMsg{
				msg(`{"chinese":"你好","english":"hello","french":"bonjour","meta":{"workflow_v2":{"succeeded":["A","B","C","D"]}},"thai":"สวัสดี"}`),
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			confStr := `
workflow_v2:
  branches:
`
			for _, mappings := range test.branches {
				confStr += fmt.Sprintf(`
    %v:
      dependency_list: %v
      request_map: |
        %v
      processors:
        - bloblang: |
            %v
      result_map: |
        %v
`,
					mappings[0],
					mappings[1],
					strings.ReplaceAll(mappings[2], "\n", "\n        "),
					strings.ReplaceAll(mappings[3], "\n", "\n            "),
					strings.ReplaceAll(mappings[4], "\n", "\n        "),
				)
			}

			conf, err := testutil.ProcessorFromYAML(confStr)
			require.NoError(t, err)

			p, err := mock.NewManager().NewProcessor(conf)
			require.NoError(t, err)

			inputMsg := message.QuickBatch(nil)
			for _, m := range test.input {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.MetaSetMut(k, v)
					}
				}
				if m.err != nil {
					part.ErrorSet(m.err)
				}
				inputMsg = append(inputMsg, part)
			}

			msgs, res := p.ProcessBatch(context.Background(), inputMsg.ShallowCopy())
			if test.err != "" {
				require.Error(t, res)
				require.EqualError(t, res, test.err)
			} else {
				require.Len(t, msgs, 1)
				assert.Equal(t, len(test.output), msgs[0].Len())
				for i, out := range test.output {
					comparePart := mockMsg{
						content: string(msgs[0].Get(i).AsBytes()),
						meta:    map[string]string{},
					}

					_ = msgs[0].Get(i).MetaIterStr(func(k, v string) error {
						comparePart.meta[k] = v
						return nil
					})

					if out.err != nil {
						assert.EqualError(t, msgs[0].Get(i).ErrorGet(), out.err.Error())
					} else {
						assert.NoError(t, msgs[0].Get(i).ErrorGet())
					}
					msgs[0].Get(i).ErrorSet(nil)
					out.err = nil

					assert.Equal(t, out, comparePart, "part: %v", i)
				}
			}

			// Ensure nothing changed
			for i, m := range test.input {
				assert.Equal(t, m.content, string(inputMsg.Get(i).AsBytes()))
			}

			ctx, done := context.WithTimeout(context.Background(), time.Second*30)
			defer done()
			assert.NoError(t, p.Close(ctx))
		})
	}
}
