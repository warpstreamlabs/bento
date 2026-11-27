package oci_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/plugin"
	"github.com/warpstreamlabs/bento/internal/plugin/oci"
	pluginruntime "github.com/warpstreamlabs/bento/internal/plugin/runtime"
	wazerov1 "github.com/warpstreamlabs/bento/internal/plugin/runtime/wazero_v1"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

// buildUppercase compiles the uppercase example plugin into a directory, with
// a manifest that requires a mount.
func buildUppercase(t *testing.T) string {
	t.Helper()

	_, file, _, _ := runtime.Caller(0)
	example := filepath.Join(filepath.Dir(file), "..", "..", "..", "public", "wasm", "examples", "uppercase")
	dir := t.TempDir()

	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", filepath.Join(dir, "plugin.wasm"), ".")
	cmd.Dir = example
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	manifest, err := os.ReadFile(filepath.Join(example, "testdata", "plugin.yaml"))
	require.NoError(t, err)
	manifest = append(manifest, []byte("\nrequires:\n  mounts:\n    - name: data\n      guest_path: /data\n")...)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "plugin.yaml"), manifest, 0o644))
	return dir
}

func TestRegistryIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))
	resource, err := pool.Run(t.Context(), "registry",
		dockertest.WithTag("3"),
		dockertest.WithoutReuse(),
		dockertest.WithHostConfig(func(config *container.HostConfig) {
			config.RestartPolicy = container.RestartPolicy{Name: container.RestartPolicyDisabled}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resource.Close(context.Background()) })
	registryAddr := "localhost:" + resource.GetPort("5000/tcp")
	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		resp, err := http.Get(fmt.Sprintf("http://%v/v2/", registryAddr))
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}))

	ref := registryAddr + "/plugins/uppercase:test"
	remote := oci.NewRegistries(nil, nil).Remote()

	// Build and push.
	builder, err := oci.OpenStore(t.Context(), t.TempDir(), remote)
	require.NoError(t, err)
	built, err := builder.Build(t.Context(), buildUppercase(t), []string{ref}, nil)
	require.NoError(t, err)
	_, err = builder.Push(t.Context(), ref)
	require.NoError(t, err)

	// Load into a fresh store, as another machine would.
	storeDir := t.TempDir()
	run := func(t *testing.T, source string, policy oci.PullPolicy) {
		store, err := oci.OpenStore(t.Context(), storeDir, remote)
		require.NoError(t, err)

		env := bundle.GlobalEnvironment.Clone()
		rt := wazerov1.NewPluginRuntime()
		t.Cleanup(func() { _ = rt.Close(context.Background()) })

		loader := plugin.NewLoader(env, rt)
		loader.SetRemote(oci.SourcePrefix, func(ctx context.Context, source string) ([]byte, []byte, error) {
			return store.Fetch(ctx, source, policy)
		})
		_, _, err = loader.Load(t.Context(), plugin.Entry{Source: source, Grant: pluginruntime.Grant{
			Mounts: map[string]pluginruntime.MountGrant{"data": {HostPath: t.TempDir()}},
		}})
		require.NoError(t, err)

		mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetEnvironment(env))
		require.NoError(t, err)
		conf, err := testutil.ProcessorFromYAML(`
uppercase:
  prefix: ">> "
`)
		require.NoError(t, err)
		p, err := mgr.NewProcessor(conf)
		require.NoError(t, err)

		res, err := p.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello")}))
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, [][]byte{[]byte(">> HELLO")}, message.GetAllBytes(res[0]))
	}

	run(t, oci.SourcePrefix+ref, oci.PullMissing)

	// With the registry gone, a plugin pinned by digest still loads from the
	// local store.
	require.NoError(t, resource.Close(t.Context()))
	run(t, oci.SourcePrefix+registryAddr+"/plugins/uppercase@"+built.Descriptor.Digest.String(), oci.PullMissing)
}
