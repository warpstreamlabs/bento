package service

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/bloblang"
)

func TestMessageBatchExecutorMapping(t *testing.T) {
	partOne := NewMessage(nil)
	partOne.SetStructured(map[string]any{
		"content": "hello world 1",
	})

	partTwo := NewMessage(nil)
	partTwo.SetStructured(map[string]any{
		"content": "hello world 2",
	})

	blobl, err := bloblang.Parse(`root.new_content = json("content").from_all().join(" - ")`)
	require.NoError(t, err)

	res, err := MessageBatch{partOne, partTwo}.BloblangExecutor(blobl).Query(0)
	require.NoError(t, err)

	resI, err := res.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"new_content": "hello world 1 - hello world 2",
	}, resI)
}

func TestMessageBatchExecutorQueryValue(t *testing.T) {
	partOne := NewMessage(nil)
	partOne.SetStructured(map[string]any{
		"content": "hello world 1",
	})

	partTwo := NewMessage(nil)
	partTwo.SetStructured(map[string]any{
		"content": "hello world 2",
	})

	tests := map[string]struct {
		mapping    string
		batchIndex int
		exp        any
		err        string
	}{
		"returns string": {
			mapping: `root = json("content")`,
			exp:     "hello world 1",
		},
		"returns integer": {
			mapping: `root = json("content").length()`,
			exp:     int64(13),
		},
		"returns float": {
			mapping: `root = json("content").length() / 2`,
			exp:     float64(6.5),
		},
		"returns bool": {
			mapping: `root = json("content").length() > 0`,
			exp:     true,
		},
		"returns bytes": {
			mapping: `root = content()`,
			exp:     []byte(`{"content":"hello world 1"}`),
		},
		"returns nil": {
			mapping: `root = null`,
			exp:     nil,
		},
		"returns null string": {
			mapping: `root = "null"`,
			exp:     "null",
		},
		"returns an array": {
			mapping: `root = [ json("content") ]`,
			exp:     []any{"hello world 1"},
		},
		"returns an object": {
			mapping: `root.new_content = json("content")`,
			exp:     map[string]any{"new_content": "hello world 1"},
		},
		"supports batch-wide queries": {
			mapping: `root.new_content = json("content").from_all().join(" - ")`,
			exp:     map[string]any{"new_content": "hello world 1 - hello world 2"},
		},
		"handles the specified message index correctly": {
			mapping:    `root = json("content")`,
			batchIndex: 1,
			exp:        "hello world 2",
		},
		"returns an error if the mapping throws": {
			mapping: `root = throw("kaboom")`,
			exp:     nil,
			err:     "failed assignment (line 1): kaboom",
		},
		"returns an error if the root is deleted": {
			mapping: `root = deleted()`,
			exp:     nil,
			err:     "root was deleted",
		},
		"doesn't error out if a field is deleted": {
			mapping: `root.foo = deleted()`,
			exp:     map[string]any{},
			err:     "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			blobl, err := bloblang.Parse(test.mapping)
			require.NoError(t, err)

			res, err := MessageBatch{partOne, partTwo}.BloblangExecutor(blobl).QueryValue(test.batchIndex)
			if test.err != "" {
				require.ErrorContains(t, err, test.err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, test.exp, res)
		})
	}
}

func TestInterpolationExecutor(t *testing.T) {
	batch := MessageBatch{
		NewMessage([]byte("foo")),
		NewMessage([]byte("bar")),
	}

	interp, err := NewInterpolatedString("${! content().uppercase().from(0) + content() }")
	require.NoError(t, err)

	exec := batch.InterpolationExecutor(interp)

	s, err := exec.TryString(0)
	require.NoError(t, err)
	assert.Equal(t, "FOOfoo", s)

	b, err := exec.TryBytes(0)
	require.NoError(t, err)
	assert.Equal(t, "FOOfoo", string(b))

	s, err = exec.TryString(1)
	require.NoError(t, err)
	assert.Equal(t, "FOObar", s)

	b, err = exec.TryBytes(1)
	require.NoError(t, err)
	assert.Equal(t, "FOObar", string(b))
}

func BenchmarkMessageBatchTryInterpolatedString(b *testing.B) {
	batch := make(MessageBatch, 1000)
	for i := range batch {
		msg := NewMessage(nil)
		msg.SetStructured(map[string]any{
			"content": "hello world",
		})
		msg.MetaSet("id", fmt.Sprintf("%d", i))
		batch[i] = msg
	}

	expressions := []struct {
		name string
		expr string
	}{
		{"Simple", "${! content() }"},
		{"WithMeta", "${! content() } - ${! meta(\"id\") }"},
		{"Complex", "${! \"ID: \" + meta(\"id\") + \" Content: \" + content() }"},
	}

	for _, expr := range expressions {
		b.Run(expr.name, func(b *testing.B) {
			interp, err := NewInterpolatedString(expr.expr)
			if err != nil {
				b.Fatalf("Failed to create interpolated string: %v", err)
			}
			for b.Loop() {
				_, _ = batch.TryInterpolatedString(500, interp)
			}
		})
	}
}
