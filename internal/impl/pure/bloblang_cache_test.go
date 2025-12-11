package pure

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestCacheResourceBloblang(t *testing.T) {
	tests := []struct {
		name        string
		pConf       string
		input       [][]byte
		expected    []string
		expectError bool
	}{
		{
			name: "err when cache does not exist",
			pConf: `
mapping: |
   root = cache_add(resource: "dne", key: this.key, value: this.value + "_cached")`,
			input: [][]byte{
				[]byte(`{"key":"key_1","value":"val_1"}`),
			},
			expectError: true,
		},
		{
			name: "err when key does not exist",
			pConf: `
mapping: |
   root = cache_get(resource: "local", key: "dne")`,
			input: [][]byte{
				[]byte(`{}`),
			},
			expectError: true,
		},
		{
			name: "cache_add and cache_get",
			pConf: `
mapping: |
    _ = cache_add(resource: "local", key: this.key, value: this.value + "_cached")
    root = cache_get(resource: "local", key: this.key)`,
			input: [][]byte{
				[]byte(`{"key":"key_1","value":"val_1"}`),
				[]byte(`{"key":"key_2","value":"val_2"}`),
			},
			expected: []string{"val_1_cached", "val_2_cached"},
		},
		{
			name: "cache_set overwrite",
			pConf: `
mapping: |
   _ = cache_add(resource: "local", key: this.key, value: "original")
   _ = cache_set(resource: "local", key: this.key, value: this.value + "_updated")
   root = cache_get(resource: "local", key: this.key)`,
			input: [][]byte{
				[]byte(`{"key":"update_key","value":"new_val"}`),
			},
			expected: []string{"new_val_updated"},
		},
		{
			name: "cache_delete",
			pConf: `
mapping: |
   _ = cache_add(resource: "local", key: this.key, value: this.value)
   _ = cache_delete(resource: "local", key: this.key)
   root = cache_get(resource: "local", key: this.key) | "deleted"`,
			input: [][]byte{
				[]byte(`{"key":"delete_key","value":"delete_val"}`),
			},
			expected: []string{"deleted"},
		},
		{
			name: "cache_add returns value on success",
			pConf: `
mapping: |
   root = cache_add(resource: "local", key: "test_key", value: "test_value")`,
			input: [][]byte{
				[]byte(`{}`),
			},
			expected: []string{"null"},
		},
		{
			name: "cache_get nonexistent key",
			pConf: `
mapping: |
   root = cache_get(resource: "local", key: "nonexistent") | "not_found"`,
			input: [][]byte{
				[]byte(`{}`),
			},
			expected: []string{"not_found"},
		},
		{
			name: "complete CRUD operations",
			pConf: `
mapping: |
   root.add_result = cache_add(resource: "local", key: this.key, value: this.value)
   root.get_result = cache_get(resource: "local", key: this.key).string()
   root.set_result = cache_set(resource: "local", key: this.key, value: this.value + "_updated")
   root.get_after_set = cache_get(resource: "local", key: this.key).string()
   root.delete_result = cache_delete(resource: "local", key: this.key)
   root.get_after_delete = cache_get(resource: "local", key: this.key).string() | "deleted"`,
			input: [][]byte{
				[]byte(`{"key":"crud_key","value":"crud_val"}`),
			},
			expected: []string{`{"add_result":null,"delete_result":null,"get_after_delete":"deleted","get_after_set":"crud_val_updated","get_result":"crud_val","set_result":null}`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgrConf, err := testutil.ManagerFromYAML(`
cache_resources:
  - label: local
    memory: {}
`)
			require.NoError(t, err)

			pConf, err := testutil.ProcessorFromYAML(tt.pConf)
			require.NoError(t, err)

			mgr, err := manager.New(mgrConf)
			require.NoError(t, err)

			proc, err := mgr.NewProcessor(pConf)
			require.NoError(t, err)

			ctx := context.Background()
			batch := message.QuickBatch(tt.input)

			out, err := proc.ProcessBatch(ctx, batch)
			if tt.expectError {
				require.Error(t, out[0].Get(0).ErrorGet())
				return
			}

			require.NoError(t, err)
			require.Len(t, out, 1)
			require.Len(t, out[0], len(tt.expected))

			for i, expected := range tt.expected {
				require.Equal(t, expected, string(out[0][i].AsBytes()))
			}
		})
	}
}
