package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ABIMarkerPrefix is the prefix of the export a plugin uses to declare the
// version of the ABI it was built against, such as bento_abi_v1.
const ABIMarkerPrefix = "bento_abi_v"

// SupportedABIVersions are the ABI versions this build of Bento can run.
var SupportedABIVersions = []int{1}

// CheckABI reads the ABI version a WASM module declares and returns an error
// if it isn't supported. Modules built before the marker existed are assumed
// to be version 1 and produce a warning.
func CheckABI(wasm []byte) (version int, warnings []string, err error) {
	exports, err := wasmExportNames(wasm)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read wasm module: %w", err)
	}

	var found []int
	for _, name := range exports {
		v, ok := strings.CutPrefix(name, ABIMarkerPrefix)
		if !ok {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			continue
		}
		found = append(found, n)
	}

	switch len(found) {
	case 0:
		return 1, []string{"plugin does not declare an ABI version (export " + ABIMarkerPrefix + "<N>), assuming v1; rebuild it with a newer plugin kit"}, nil
	case 1:
		version = found[0]
	default:
		return 0, nil, fmt.Errorf("plugin declares more than one ABI version: %v", found)
	}

	if slices.Contains(SupportedABIVersions, version) {
		return version, nil, nil
	}
	return version, nil, fmt.Errorf("plugin was built for ABI v%v, this version of Bento supports %v", version, formatVersions(SupportedABIVersions))
}

func formatVersions(vs []int) string {
	s := make([]string, len(vs))
	for i, v := range vs {
		s[i] = "v" + strconv.Itoa(v)
	}
	return strings.Join(s, ", ")
}

// wasmExportNames returns the names of all exports of a WASM module by
// reading its export section, without compiling it.
func wasmExportNames(wasm []byte) ([]string, error) {
	if len(wasm) < 8 || !bytes.Equal(wasm[:4], []byte("\x00asm")) {
		return nil, errors.New("not a wasm module")
	}

	r := wasmReader{b: wasm[8:]}
	for len(r.b) > 0 {
		id, err := r.byte()
		if err != nil {
			return nil, err
		}
		size, err := r.u32()
		if err != nil {
			return nil, err
		}
		section, err := r.bytes(size)
		if err != nil {
			return nil, err
		}
		if id != 7 { // export section
			continue
		}

		sr := wasmReader{b: section}
		count, err := sr.u32()
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, count)
		for range count {
			nameLen, err := sr.u32()
			if err != nil {
				return nil, err
			}
			name, err := sr.bytes(nameLen)
			if err != nil {
				return nil, err
			}
			if _, err := sr.byte(); err != nil { // kind
				return nil, err
			}
			if _, err := sr.u32(); err != nil { // index
				return nil, err
			}
			names = append(names, string(name))
		}
		return names, nil
	}
	return nil, nil
}

type wasmReader struct{ b []byte }

var errTruncated = errors.New("unexpected end of wasm module")

func (r *wasmReader) byte() (byte, error) {
	if len(r.b) == 0 {
		return 0, errTruncated
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c, nil
}

func (r *wasmReader) bytes(n uint32) ([]byte, error) {
	if uint64(n) > uint64(len(r.b)) {
		return nil, errTruncated
	}
	b := r.b[:n]
	r.b = r.b[n:]
	return b, nil
}

// u32 reads an unsigned LEB128 encoded integer.
func (r *wasmReader) u32() (uint32, error) {
	var result uint32
	for shift := 0; shift < 35; shift += 7 {
		c, err := r.byte()
		if err != nil {
			return 0, err
		}
		result |= uint32(c&0x7f) << shift
		if c&0x80 == 0 {
			return result, nil
		}
	}
	return 0, errors.New("invalid integer in wasm module")
}
