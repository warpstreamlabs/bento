package wazerov1

import (
	"embed"
	"slices"
	"testing"

	"github.com/eliben/watgo/wasmir"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
)

//go:embed testdata/*/*.wat
var watFiles embed.FS

const watManifest = `
name: wat
type: processor
runtime:
  wasm: {%s}
`

func setBuffer(t *testing.T, mod *wasmir.Module, global string, data []byte) {
	t.Helper()

	i := slices.IndexFunc(mod.Globals, func(g wasmir.Global) bool { return g.Name == global })
	require.NotEqual(t, -1, i, "module has no %s global", global)

	mod.Data = append(mod.Data, wasmir.DataSegment{
		Mode:       wasmir.DataSegmentModeActive,
		OffsetType: wasmir.ValueTypeI32,
		Init:       data,
	})
	mod.Globals[i].Init = []wasmir.Instruction{{
		Kind:     wasmir.InstrI64Const,
		I64Const: int64(wasm.NewBuffer(0, uint32(len(data)))),
	}}
}
