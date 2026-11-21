package wazerov1

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/eliben/watgo"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	pb "github.com/warpstreamlabs/bento/internal/message/messagepb"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
	"github.com/warpstreamlabs/bento/internal/value"
)

func TestProcessor(t *testing.T) {
	tests := []struct {
		name string
		wat  string
		res  proto.Message
		out  []message.Batch
	}{
		{
			name: "process",
			wat:  "processor/process.wat",
			res:  &pb.Process_Response{Batch: toProto(newPart("a"), newMetaPart("b", map[string]string{"k": "v"}))},
			out:  []message.Batch{{newPart("a"), newMetaPart("b", map[string]string{"k": "v"})}},
		},
		{
			name: "process error",
			wat:  "processor/process.wat",
			res:  &pb.Process_Response{Error: &pb.Error{Message: "failed"}},
			out:  []message.Batch{{newErrPart("hello", errors.New("failed"))}},
		},
		{
			name: "process drop",
			wat:  "processor/process.wat",
		},
		{
			name: "process batch",
			wat:  "processor/process_batch.wat",
			res: &pb.ProcessBatch_Response{Batches: &pb.Batches{Batches: []*pb.Batch{
				toProto(newPart("a")),
				toProto(newPart("b"), newErrPart("c", errors.New("marked"))),
			}}},
			out: []message.Batch{{newPart("a")}, {newPart("b"), newErrPart("c", errors.New("marked"))}},
		},
		{
			name: "process batch error",
			wat:  "processor/process_batch.wat",
			res:  &pb.ProcessBatch_Response{Error: &pb.Error{Message: "failed"}},
			out:  []message.Batch{{newErrPart("hello", value.NewDetailedError(errors.New("failed"), "wat", "my_wat"))}},
		},
		{
			name: "process batch drop",
			wat:  "processor/process_batch.wat",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			src, err := watFiles.ReadFile("testdata/" + test.wat)
			require.NoError(t, err)

			mod, err := watgo.ParseAndValidateWAT(src)
			require.NoError(t, err)

			if test.res != nil {
				res, err := proto.Marshal(test.res)
				require.NoError(t, err)
				setBuffer(t, mod, "$response", res)
			}

			bin, err := watgo.EncodeWASM(mod)
			require.NoError(t, err)

			manifest, _, err := runtime.ReadManifestYAML(fmt.Appendf(nil, watManifest, ""))
			require.NoError(t, err)

			rt := NewPluginRuntime()
			t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })

			plugin, err := rt.Register(manifest, runtime.ByteSource(bin))
			require.NoError(t, err)

			env := bundle.NewEnvironment()
			require.NoError(t, plugin.RegisterWith(env))

			mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetEnvironment(env))
			require.NoError(t, err)

			conf := processor.NewConfig()
			conf.Label = "my_wat"
			conf.Type = "wat"
			conf.Plugin = map[string]any{}

			proc, err := mgr.NewProcessor(conf)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, proc.Close(context.Background())) })

			batches, err := proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello")}))
			require.NoError(t, err)
			require.Equal(t, test.out, batches)
		})
	}
}

func TestProcessorErrors(t *testing.T) {
	tests := []struct {
		name       string
		wat        string
		initErr    string
		processErr string
	}{
		{
			name:    "missing export",
			wat:     "processor/missing_export.wat",
			initErr: "wasm guest plugin missing required exports: [free]",
		},
		{
			name:       "trap",
			wat:        "processor/process_trap.wat",
			processErr: "wasm error: unreachable",
		},
		{
			name:       "init error",
			wat:        "processor/init_error.wat",
			processErr: "initialization failed",
		},
		{
			name:    "invalid signature",
			wat:     "processor/invalid_signature.wat",
			initErr: "wasm guest plugin exports with invalid signatures: [process]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			src, err := watFiles.ReadFile("testdata/" + test.wat)
			require.NoError(t, err)

			bin, err := watgo.CompileWATToWASM(src)
			require.NoError(t, err)

			manifest, _, err := runtime.ReadManifestYAML(fmt.Appendf(nil, watManifest, ""))
			require.NoError(t, err)

			rt := NewPluginRuntime()
			t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })

			plugin, err := rt.Register(manifest, runtime.ByteSource(bin))
			require.NoError(t, err)

			env := bundle.NewEnvironment()
			require.NoError(t, plugin.RegisterWith(env))

			mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetEnvironment(env))
			require.NoError(t, err)

			conf := processor.NewConfig()
			conf.Type = "wat"
			conf.Plugin = map[string]any{}

			proc, err := mgr.NewProcessor(conf)
			if test.initErr != "" {
				require.ErrorContains(t, err, test.initErr)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, proc.Close(context.Background())) })

			batches, err := proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello")}))
			require.NoError(t, err)
			require.ErrorContains(t, batches[0][0].ErrorGet(), test.processErr)
		})
	}
}

// ------------------------------------------------------------------------------

func newPart(content string) *message.Part {
	return message.NewPart([]byte(content))
}

func newMetaPart(content string, meta map[string]string) *message.Part {
	part := message.NewPart([]byte(content))
	for k, v := range meta {
		part.MetaSetMut(k, v)
	}
	return part
}

func newErrPart(content string, err error) *message.Part {
	part := message.NewPart([]byte(content))
	part.ErrorSet(err)
	return part
}

func toProto(parts ...*message.Part) *pb.Batch {
	batch, err := message.BatchToProto(parts)
	if err != nil {
		panic(err)
	}
	return batch
}
