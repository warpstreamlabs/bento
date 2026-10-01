package azure_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/public/components/azure" // ensure init runs
)

func TestAzureComponentsCredentialsLint(t *testing.T) {
	tests := []struct {
		name    string
		section string
		conf    string
		lintErr string
	}{
		{
			name:    "blob storage input with service principal",
			section: "input",
			conf: `azure_blob_storage:
  storage_account: foo
  container: bar
  credentials:
    tenant_id: a
    client_id: b
    client_secret: c
`,
		},
		{
			name:    "blob storage output with managed identity",
			section: "output",
			conf: `azure_blob_storage:
  storage_account: foo
  container: bar
  credentials:
    from_managed_identity: true
`,
		},
		{
			name:    "queue storage input with workload identity",
			section: "input",
			conf: `azure_queue_storage:
  storage_account: foo
  queue_name: bar
  credentials:
    tenant_id: a
    client_id: b
    federated_token_file: /var/run/secrets/azure/tokens/azure-identity-token
`,
		},
		{
			name:    "table storage output with credentials and connection string",
			section: "output",
			conf: `azure_table_storage:
  storage_connection_string: "AccountName=foo;AccountKey=bar"
  table_name: baz
  credentials:
    from_managed_identity: true
`,
			lintErr: "credentials are ignored when storage_connection_string, storage_access_key or storage_sas_token is set",
		},
		{
			name:    "table storage input with invalid credentials",
			section: "input",
			conf: `azure_table_storage:
  storage_account: foo
  table_name: baz
  credentials:
    client_secret: c
`,
			lintErr: "client_secret requires both tenant_id and client_id to be set",
		},
		{
			name:    "service bus with credentials",
			section: "input",
			conf: `azure_service_bus_queue:
  namespace: test.servicebus.windows.net
  queue: foo
  credentials:
    from_managed_identity: true
    client_id: b
`,
		},
		{
			name:    "service bus with credentials and connection string",
			section: "input",
			conf: `azure_service_bus_queue:
  connection_string: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
  queue: foo
  credentials:
    from_managed_identity: true
`,
			lintErr: "credentials are ignored when connection_string is set",
		},
		{
			name:    "cosmosdb with credentials",
			section: "output",
			conf: `azure_cosmosdb:
  endpoint: https://foo.documents.azure.com:443/
  database: foo
  container: bar
  partition_keys_map: root = "blobfish"
  credentials:
    tenant_id: a
    client_id: b
    client_secret: c
`,
		},
		{
			name:    "cosmosdb with credentials and account key",
			section: "output",
			conf: `azure_cosmosdb:
  endpoint: https://foo.documents.azure.com:443/
  account_key: Zm9v
  database: foo
  container: bar
  partition_keys_map: root = "blobfish"
  credentials:
    from_managed_identity: true
`,
			lintErr: "is ignored when",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := service.NewEnvironment().NewStreamBuilder()
			var err error
			if test.section == "input" {
				err = builder.AddInputYAML(test.conf)
			} else {
				err = builder.AddOutputYAML(test.conf)
			}
			if test.lintErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.lintErr)
		})
	}
}
