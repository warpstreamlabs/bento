package kubernetes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKubernetesWatchConfigParse(t *testing.T) {
	spec := kubernetesWatchInputConfig()

	tests := []struct {
		name        string
		config      string
		expectError bool
	}{
		{
			name: "watch pods",
			config: `
resource: pods
`,
			expectError: false,
		},
		{
			name: "watch deployments in namespace",
			config: `
resource: deployments
namespaces:
  - production
label_selector: "app=backend"
`,
			expectError: false,
		},
		{
			name: "watch custom resource",
			config: `
custom_resource:
  group: stable.example.com
  version: v1
  resource: crontabs
`,
			expectError: false,
		},
		{
			name: "filter event types",
			config: `
resource: pods
event_types:
  - ADDED
  - DELETED
include_initial_list: false
`,
			expectError: false,
		},
		{
			name: "all namespaces with resource",
			config: `
resource: services
namespaces: []
`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(tt.config, nil)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, conf)
			}
		})
	}
}

func TestStandardResourcesMap(t *testing.T) {
	// Verify all expected resources are in the map
	expectedResources := []string{
		"pods", "services", "configmaps", "secrets",
		"deployments", "replicasets", "statefulsets", "daemonsets",
		"jobs", "cronjobs", "ingresses", "networkpolicies",
		"namespaces", "nodes", "persistentvolumes", "persistentvolumeclaims",
	}

	for _, resource := range expectedResources {
		gvr, ok := standardResources[resource]
		require.True(t, ok, "expected resource %s to be in standardResources", resource)
		require.NotEmpty(t, gvr.Resource, "expected resource %s to have non-empty Resource field", resource)
	}
}
