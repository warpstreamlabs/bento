package kubernetes

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestBuildKubeconfigUsesEnv(t *testing.T) {
	dir := t.TempDir()

	kubeconfigPath := writeKubeconfig(t, dir, "https://example-env.invalid")
	t.Setenv("KUBECONFIG", kubeconfigPath)

	conf := parseAuthConfig(t, "")

	clientCfg, err := buildKubeconfigClient(conf)
	require.NoError(t, err)
	require.Equal(t, "https://example-env.invalid", clientCfg.Host)
}

func TestBuildKubeconfigExplicitOverridesEnv(t *testing.T) {
	dir := t.TempDir()

	envKubeconfigPath := writeKubeconfig(t, dir, "https://example-env.invalid")
	explicitKubeconfigPath := writeKubeconfig(t, dir, "https://example-explicit.invalid")

	t.Setenv("KUBECONFIG", envKubeconfigPath)

	conf := parseAuthConfig(t, fmt.Sprintf("kubeconfig: %s\n", explicitKubeconfigPath))

	clientCfg, err := buildKubeconfigClient(conf)
	require.NoError(t, err)
	require.Equal(t, "https://example-explicit.invalid", clientCfg.Host)
}

func parseAuthConfig(t *testing.T, yamlConfig string) *service.ParsedConfig {
	spec := service.NewConfigSpec().Fields(AuthFields()...)
	conf, err := spec.ParseYAML(yamlConfig, nil)
	require.NoError(t, err)
	return conf
}

func writeKubeconfig(t *testing.T, dir, server string) string {
	path := filepath.Join(dir, fmt.Sprintf("kubeconfig-%s.yaml", sanitizeServer(server)))
	data := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: %s
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user:
    token: dummy
`, server)
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))
	return path
}

func sanitizeServer(server string) string {
	out := make([]byte, 0, len(server))
	for i := 0; i < len(server); i++ {
		c := server[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}
