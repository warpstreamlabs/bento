package azure

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/warpstreamlabs/bento/internal/impl/azure/credentials"
	"github.com/warpstreamlabs/bento/public/service"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
)

const (
	// Common fields for blob storage components
	bscFieldStorageAccount          = "storage_account"
	bscFieldStorageAccessKey        = "storage_access_key"
	bscFieldStorageSASToken         = "storage_sas_token"
	bscFieldStorageConnectionString = "storage_connection_string"
)

const legacyDiscouraged = " Prefer `" + credentials.FieldCredentials + "` (Microsoft Entra ID) where possible, shared keys and connection strings are long-lived secrets."

// storageAuthDocs documents authentication for all Azure Storage components.
var storageAuthDocs = credentials.Docs("The `" + bscFieldStorageAccount + "` field must be set when authenticating with `" + credentials.FieldCredentials + "`.\n\nAlternatively, the following shared key methods are supported, in order of priority: `" + bscFieldStorageConnectionString + "`, `" + bscFieldStorageAccount + "` with `" + bscFieldStorageAccessKey + "`, and `" + bscFieldStorageAccount + "` with `" + bscFieldStorageSASToken + "`. If `" + bscFieldStorageConnectionString + "` does not contain the `AccountName` parameter then it must be specified with the `" + bscFieldStorageAccount + "` field.")

func azureComponentSpec(forBlobStorage bool) *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services", "Azure").
		Fields(
			service.NewStringField(bscFieldStorageAccount).
				Description("The storage account to access. Required unless `"+bscFieldStorageConnectionString+"` is set, in which case it is only used if the connection string does not contain the `AccountName` parameter.").
				Example("mystorageaccount").
				Default(""),
			service.NewStringField(bscFieldStorageAccessKey).
				Description("The storage account access key. This field is ignored if `"+bscFieldStorageConnectionString+"` is set."+legacyDiscouraged).
				Default("").
				Secret(),
			service.NewStringField(bscFieldStorageConnectionString).
				Description("A storage account connection string. When set it takes priority over every other authentication method."+legacyDiscouraged).
				Default("").
				Secret(),
		)
	spec = spec.Field(service.NewStringField(bscFieldStorageSASToken).
		Description("The storage account SAS token. This field is ignored if `" + bscFieldStorageConnectionString + "` or `" + bscFieldStorageAccessKey + "` are set." + legacyDiscouraged).
		Default("").
		Secret()).
		Field(credentials.Fields()).
		LintRule(`
let hasLegacy = this.storage_connection_string.or("") != "" || this.storage_access_key.or("") != "" || this.storage_sas_token.or("") != ""
root = [
  if this.storage_connection_string.or("") != "" && !this.storage_connection_string.contains("AccountName=") && !this.storage_connection_string.contains("UseDevelopmentStorage=true;") && this.storage_account.or("") == "" { "storage_account must be set if storage_connection_string does not contain the \"AccountName\" parameter" },
  if $hasLegacy && ` + credentials.IsSetBloblang + ` { "credentials are ignored when storage_connection_string, storage_access_key or storage_sas_token is set" },
].filter(v -> v != null)
`)
	return spec
}

func blobStorageClientFromParsed(pConf *service.ParsedConfig, container string) (*azblob.Client, bool, error) {
	connectionString, err := pConf.FieldString(bscFieldStorageConnectionString)
	if err != nil {
		return nil, false, err
	}
	storageAccount, err := pConf.FieldString(bscFieldStorageAccount)
	if err != nil {
		return nil, false, err
	}
	storageAccessKey, err := pConf.FieldString(bscFieldStorageAccessKey)
	if err != nil {
		return nil, false, err
	}
	storageSASToken, err := pConf.FieldString(bscFieldStorageSASToken)
	if err != nil {
		return nil, false, err
	}
	if storageAccount == "" && connectionString == "" {
		return nil, false, errors.New("invalid azure storage account credentials")
	}
	return getBlobStorageClient(connectionString, storageAccount, storageAccessKey, storageSASToken, container, tokenCredentialFn(pConf))
}

// tokenCredentialFn returns a lazy constructor for the Entra ID credential
// configured within pConf, so that it's only built when no shared key
// authentication method is set.
func tokenCredentialFn(pConf *service.ParsedConfig) func() (azcore.TokenCredential, error) {
	return func() (azcore.TokenCredential, error) {
		return credentials.GetTokenCredential(pConf)
	}
}

const (
	blobEndpointExp = "https://%s.blob.core.windows.net"
)

func getBlobStorageClient(storageConnectionString, storageAccount, storageAccessKey, storageSASToken, container string, getCred func() (azcore.TokenCredential, error)) (*azblob.Client, bool, error) {
	var client *azblob.Client
	var err error
	var containerSASToken bool
	if storageConnectionString != "" {
		storageConnectionString := parseStorageConnectionString(storageConnectionString, storageAccount)
		client, err = azblob.NewClientFromConnectionString(storageConnectionString, nil)
	} else if storageAccessKey != "" {
		cred, credErr := azblob.NewSharedKeyCredential(storageAccount, storageAccessKey)
		if credErr != nil {
			return nil, false, fmt.Errorf("error creating shared key credential: %w", credErr)
		}
		serviceURL := fmt.Sprintf(blobEndpointExp, storageAccount)
		client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	} else if storageSASToken != "" {
		var serviceURL string
		if strings.HasPrefix(storageSASToken, "sp=") {
			// container SAS token
			containerSASToken = true
			serviceURL = fmt.Sprintf("%s/%s?%s", fmt.Sprintf(blobEndpointExp, storageAccount), container, storageSASToken)
		} else {
			// storage account SAS token
			serviceURL = fmt.Sprintf("%s/%s", fmt.Sprintf(blobEndpointExp, storageAccount), storageSASToken)
		}
		client, err = azblob.NewClientWithNoCredential(serviceURL, nil)
	} else {
		cred, credErr := getCred()
		if credErr != nil {
			return nil, false, fmt.Errorf("error getting Azure credentials: %w", credErr)
		}
		serviceURL := fmt.Sprintf(blobEndpointExp, storageAccount)
		client, err = azblob.NewClient(serviceURL, cred, nil)
	}
	if err != nil {
		return nil, false, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	return client, containerSASToken, err
}

// getEmulatorConnectionString returns the Azurite connection string for the provided service ports
// Details here: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#http-connection-strings
func getEmulatorConnectionString(blobServicePort, queueServicePort, tableServicePort string) string {
	return fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:%s/devstoreaccount1;QueueEndpoint=http://127.0.0.1:%s/devstoreaccount1;TableEndpoint=http://127.0.0.1:%s/devstoreaccount1;",
		blobServicePort, queueServicePort, tableServicePort,
	)
}

const (
	azuriteBlobPortEnv  = "AZURITE_BLOB_ENDPOINT_PORT"
	azuriteQueuePortEnv = "AZURITE_QUEUE_ENDPOINT_PORT"
	azuriteTablePortEnv = "AZURITE_TABLE_ENDPOINT_PORT"
)

func parseStorageConnectionString(storageConnectionString, storageAccount string) string {
	if strings.Contains(storageConnectionString, "UseDevelopmentStorage=true;") {
		azuriteDefaultPorts := map[string]string{
			azuriteBlobPortEnv:  "10000",
			azuriteQueuePortEnv: "10001",
			azuriteTablePortEnv: "10002",
		}
		for name := range azuriteDefaultPorts {
			port := os.Getenv(name)
			if port != "" {
				azuriteDefaultPorts[name] = port
			}
		}
		storageConnectionString = getEmulatorConnectionString(
			azuriteDefaultPorts[azuriteBlobPortEnv],
			azuriteDefaultPorts[azuriteQueuePortEnv],
			azuriteDefaultPorts[azuriteTablePortEnv],
		)
	}
	// The Shared Access Signature UI doesn't add the AccountName parameter to the Connection String for some reason...
	// However, in the Access Keys UI, the Connection String does have the AccountName parameter embedded in it.
	// I think it's worth maintaining this hack in here to help users who try to use SAS tokens in Connection String
	// format.
	if !strings.Contains(storageConnectionString, "AccountName=") {
		storageConnectionString = storageConnectionString + ";" + "AccountName=" + storageAccount
	}
	return storageConnectionString
}

//------------------------------------------------------------------------------

const (
	azQueueEndpointExp = "https://%s.queue.core.windows.net"
)

func queueServiceClientFromParsed(pConf *service.ParsedConfig) (*azqueue.ServiceClient, error) {
	connectionString, err := pConf.FieldString(bscFieldStorageConnectionString)
	if err != nil {
		return nil, err
	}
	storageAccount, err := pConf.FieldString(bscFieldStorageAccount)
	if err != nil {
		return nil, err
	}
	storageAccessKey, err := pConf.FieldString(bscFieldStorageAccessKey)
	if err != nil {
		return nil, err
	}
	storageSASToken, err := pConf.FieldString(bscFieldStorageSASToken)
	if err != nil {
		return nil, err
	}
	if storageAccount == "" && connectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	return getQueueServiceClient(storageAccount, storageAccessKey, connectionString, storageSASToken, tokenCredentialFn(pConf))
}

func getQueueServiceClient(storageAccount, storageAccessKey, storageConnectionString, storageSASToken string, getCred func() (azcore.TokenCredential, error)) (*azqueue.ServiceClient, error) {
	if storageAccount == "" && storageConnectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	var client *azqueue.ServiceClient
	var err error
	if storageConnectionString != "" {
		connStr := parseStorageConnectionString(storageConnectionString, storageAccount)
		client, err = azqueue.NewServiceClientFromConnectionString(connStr, nil)
	} else if storageAccessKey != "" {
		cred, credErr := azqueue.NewSharedKeyCredential(storageAccount, storageAccessKey)
		if credErr != nil {
			return nil, fmt.Errorf("error creating shared key credential: %w", credErr)
		}
		serviceURL := fmt.Sprintf(azQueueEndpointExp, storageAccount)
		client, err = azqueue.NewServiceClientWithSharedKeyCredential(serviceURL, cred, nil)
	} else if storageSASToken != "" {
		serviceURL := fmt.Sprintf("%s/%s", fmt.Sprintf(azQueueEndpointExp, storageAccount), storageSASToken)
		client, err = azqueue.NewServiceClientWithNoCredential(serviceURL, nil)
	} else {
		cred, credErr := getCred()
		if credErr != nil {
			return nil, fmt.Errorf("error getting Azure credentials: %w", credErr)
		}
		serviceURL := fmt.Sprintf(azQueueEndpointExp, storageAccount)
		client, err = azqueue.NewServiceClient(serviceURL, cred, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %w", err)
	}

	return client, err
}

//------------------------------------------------------------------------------

func tablesServiceClientFromParsed(pConf *service.ParsedConfig) (*aztables.ServiceClient, error) {
	connectionString, err := pConf.FieldString(bscFieldStorageConnectionString)
	if err != nil {
		return nil, err
	}
	storageAccount, err := pConf.FieldString(bscFieldStorageAccount)
	if err != nil {
		return nil, err
	}
	storageAccessKey, err := pConf.FieldString(bscFieldStorageAccessKey)
	if err != nil {
		return nil, err
	}
	storageSASToken, err := pConf.FieldString(bscFieldStorageSASToken)
	if err != nil {
		return nil, err
	}
	if storageAccount == "" && connectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	return getTablesServiceClient(storageAccount, storageAccessKey, connectionString, storageSASToken, tokenCredentialFn(pConf))
}

const (
	tableEndpointExp = "https://%s.table.core.windows.net"
)

func getTablesServiceClient(account, accessKey, connectionString, storageSASToken string, getCred func() (azcore.TokenCredential, error)) (*aztables.ServiceClient, error) {
	var err error
	if account == "" && connectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	var client *aztables.ServiceClient
	if connectionString != "" {
		storageConnectionString := parseStorageConnectionString(connectionString, account)
		client, err = aztables.NewServiceClientFromConnectionString(storageConnectionString, &aztables.ClientOptions{})
	} else if accessKey != "" {
		cred, credErr := aztables.NewSharedKeyCredential(account, accessKey)
		if credErr != nil {
			return nil, fmt.Errorf("invalid azure storage account credentials: %w", credErr)
		}
		client, err = aztables.NewServiceClientWithSharedKey(fmt.Sprintf(tableEndpointExp, account), cred, nil)
	} else if storageSASToken != "" {
		serviceURL := fmt.Sprintf("%s/%s", fmt.Sprintf(tableEndpointExp, account), storageSASToken)
		client, err = aztables.NewServiceClientWithNoCredential(serviceURL, nil)
	} else {
		cred, credErr := getCred()
		if credErr != nil {
			return nil, fmt.Errorf("error getting Azure credentials: %w", credErr)
		}
		serviceURL := fmt.Sprintf(tableEndpointExp, account)
		client, err = aztables.NewServiceClient(serviceURL, cred, nil)
	}
	return client, err
}
