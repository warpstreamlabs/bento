package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testModule returns a WASM module with a custom section followed by an export
// section with the given function exports.
func testModule(exports ...string) []byte {
	b := []byte("\x00asm\x01\x00\x00\x00")

	// A custom section, which must be skipped.
	b = append(b, 0, 3, 1, 'x', 0)

	var section []byte
	section = append(section, byte(len(exports)))
	for i, name := range exports {
		section = append(section, byte(len(name)))
		section = append(section, name...)
		section = append(section, 0, byte(i)) // func kind, index
	}
	b = append(b, 7, byte(len(section)))
	return append(b, section...)
}

func TestCheckABI(t *testing.T) {
	tests := []struct {
		name    string
		wasm    []byte
		version int
		warns   bool
		err     string
	}{
		{name: "v1", wasm: testModule("malloc", "bento_abi_v1", "process"), version: 1},
		{name: "no marker", wasm: testModule("malloc", "process"), version: 1, warns: true},
		{name: "no exports", wasm: []byte("\x00asm\x01\x00\x00\x00"), version: 1, warns: true},
		{name: "unsupported", wasm: testModule("bento_abi_v2"), err: "built for ABI v2, this version of Bento supports v1"},
		{name: "two markers", wasm: testModule("bento_abi_v1", "bento_abi_v2"), err: "more than one ABI version"},
		{name: "invalid marker ignored", wasm: testModule("bento_abi_vx"), version: 1, warns: true},
		{name: "not wasm", wasm: []byte("hello world"), err: "not a wasm module"},
		{name: "truncated", wasm: testModule("bento_abi_v1")[:20], err: "unexpected end"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			version, warnings, err := CheckABI(test.wasm)
			if test.err != "" {
				require.ErrorContains(t, err, test.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.version, version)
			assert.Equal(t, test.warns, len(warnings) > 0)
		})
	}
}
