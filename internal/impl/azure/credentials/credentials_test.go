package credentials

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func testSpec() *service.ConfigSpec {
	return service.NewConfigSpec().Field(Fields())
}

func credFromYAML(t *testing.T, conf string) (azcore.TokenCredential, error) {
	t.Helper()
	pConf, err := testSpec().ParseYAML(conf, nil)
	require.NoError(t, err)
	return GetTokenCredential(pConf)
}

func TestGetTokenCredentialTypes(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))

	tests := []struct {
		name     string
		conf     string
		expected any
	}{
		{
			name:     "empty uses default chain",
			conf:     `{}`,
			expected: &azidentity.DefaultAzureCredential{},
		},
		{
			name: "default chain with tenant",
			conf: `
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  additionally_allowed_tenants: [ "*" ]
`,
			expected: &azidentity.DefaultAzureCredential{},
		},
		{
			name: "client secret",
			conf: `
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 11111111-1111-1111-1111-111111111111
  client_secret: shhh
`,
			expected: &azidentity.ClientSecretCredential{},
		},
		{
			name: "client secret with sovereign cloud",
			conf: `
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 11111111-1111-1111-1111-111111111111
  client_secret: shhh
  authority_host: https://login.microsoftonline.us/
`,
			expected: &azidentity.ClientSecretCredential{},
		},
		{
			name: "workload identity",
			conf: `
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 11111111-1111-1111-1111-111111111111
  federated_token_file: ` + tokenFile + `
`,
			expected: &azidentity.WorkloadIdentityCredential{},
		},
		{
			name: "system assigned managed identity",
			conf: `
credentials:
  from_managed_identity: true
`,
			expected: &azidentity.ManagedIdentityCredential{},
		},
		{
			name: "user assigned managed identity by client id",
			conf: `
credentials:
  from_managed_identity: true
  client_id: 11111111-1111-1111-1111-111111111111
`,
			expected: &azidentity.ManagedIdentityCredential{},
		},
		{
			name: "user assigned managed identity by resource id",
			conf: `
credentials:
  from_managed_identity: true
  managed_identity_resource_id: /subscriptions/a/resourceGroups/b/providers/Microsoft.ManagedIdentity/userAssignedIdentities/c
`,
			expected: &azidentity.ManagedIdentityCredential{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cred, err := credFromYAML(t, test.conf)
			require.NoError(t, err)
			assert.IsType(t, test.expected, cred)
		})
	}
}

func TestGetTokenCredentialErrors(t *testing.T) {
	tests := []struct {
		name string
		conf string
	}{
		{
			name: "client secret without tenant",
			conf: `
credentials:
  client_id: 11111111-1111-1111-1111-111111111111
  client_secret: shhh
`,
		},
		{
			name: "missing certificate file",
			conf: `
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 11111111-1111-1111-1111-111111111111
  client_certificate_path: /does/not/exist.pem
`,
		},
		{
			name: "both managed identity selectors",
			conf: `
credentials:
  from_managed_identity: true
  client_id: 11111111-1111-1111-1111-111111111111
  managed_identity_resource_id: /subscriptions/a
`,
		},
		{
			name: "non https authority host",
			conf: `
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 11111111-1111-1111-1111-111111111111
  client_secret: shhh
  authority_host: http://login.example.com/
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := credFromYAML(t, test.conf)
			require.Error(t, err)
		})
	}
}

func TestGetTokenCredentialMissingField(t *testing.T) {
	pConf, err := service.NewConfigSpec().Field(service.NewStringField("foo").Default("")).ParseYAML(`{}`, nil)
	require.NoError(t, err)

	cred, err := GetTokenCredential(pConf)
	require.NoError(t, err)
	assert.IsType(t, &azidentity.DefaultAzureCredential{}, cred)
}

func TestCredentialsLint(t *testing.T) {
	env := service.NewEmptyEnvironment()
	require.NoError(t, env.RegisterInput("azure_creds_test",
		service.NewConfigSpec().
			Field(service.NewStringField("connection_string").Default("")).
			Field(Fields()).
			LintRule(`root = if this.connection_string.or("") != "" && `+IsSetBloblang+` { [ "credentials ignored" ] }`),
		func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
			return nil, nil
		}))
	linter := env.FullConfigSchema("", "").NewStreamConfigLinter()

	tests := []struct {
		name     string
		conf     string
		expected []string
	}{
		{
			name: "no credentials",
			conf: `{}`,
		},
		{
			name: "valid client secret",
			conf: `
credentials:
  tenant_id: a
  client_id: b
  client_secret: c
`,
		},
		{
			name: "valid managed identity",
			conf: `
credentials:
  from_managed_identity: true
  client_id: b
`,
		},
		{
			name: "client secret missing tenant",
			conf: `
credentials:
  client_id: b
  client_secret: c
`,
			expected: []string{"client_secret requires both tenant_id and client_id to be set"},
		},
		{
			name: "secret and certificate",
			conf: `
credentials:
  tenant_id: a
  client_id: b
  client_secret: c
  client_certificate_path: d
`,
			expected: []string{"only one of client_secret and client_certificate_path may be set"},
		},
		{
			name: "dangling client id",
			conf: `
credentials:
  client_id: b
`,
			expected: []string{"client_id is only used together with"},
		},
		{
			name: "both managed identity selectors",
			conf: `
credentials:
  from_managed_identity: true
  client_id: b
  managed_identity_resource_id: c
`,
			expected: []string{"only one of client_id and managed_identity_resource_id may be set"},
		},
		{
			name: "credentials with connection string",
			conf: `
connection_string: foo
credentials:
  from_managed_identity: true
`,
			expected: []string{"credentials ignored"},
		},
		{
			name: "connection string with empty credentials",
			conf: `
connection_string: foo
credentials:
  client_id: ""
  additionally_allowed_tenants: []
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := "input:\n  azure_creds_test:\n" + indent(test.conf)
			lints, err := linter.LintYAML([]byte(conf))
			require.NoError(t, err)

			var whats []string
			for _, l := range lints {
				whats = append(whats, l.What)
			}
			require.Len(t, whats, len(test.expected), whats)
			for i, exp := range test.expected {
				assert.Contains(t, whats[i], exp)
			}
		})
	}
}

func indent(s string) string {
	var out strings.Builder
	for line := range strings.SplitSeq(s, "\n") {
		if line != "" {
			out.WriteString("    " + line + "\n")
		}
	}
	return out.String()
}
