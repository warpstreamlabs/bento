// Package credentials provides a re-usable `credentials` config block for Azure
// components, backed by the azidentity Microsoft Entra ID credential types.
package credentials

import (
	"errors"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// FieldCredentials is the name of the credentials object field.
	FieldCredentials = "credentials"

	fieldTenantID                   = "tenant_id"
	fieldClientID                   = "client_id"
	fieldClientSecret               = "client_secret"
	fieldClientCertificatePath      = "client_certificate_path"
	fieldClientCertificatePassword  = "client_certificate_password"
	fieldFederatedTokenFile         = "federated_token_file"
	fieldFromManagedIdentity        = "from_managed_identity"
	fieldManagedIdentityResourceID  = "managed_identity_resource_id"
	fieldAuthorityHost              = "authority_host"
	fieldAdditionallyAllowedTenants = "additionally_allowed_tenants"
)

func envNote(name string) string {
	return " Equivalent environment variable: `" + name + "`, which is read by the default credential chain when this field is empty and no explicit credential type is selected."
}

// Fields returns the `credentials` object field shared by all Azure
// components.
func Fields() *service.ConfigField {
	return service.NewObjectField(FieldCredentials,
		service.NewStringField(fieldTenantID).
			Description("The Microsoft Entra ID (Azure AD) tenant ID to authenticate against. Required with `"+fieldClientSecret+"` or `"+fieldClientCertificatePath+"`."+envNote("AZURE_TENANT_ID")).
			Example("00000000-0000-0000-0000-000000000000").
			Default(""),
		service.NewStringField(fieldClientID).
			Description("The client (application) ID of an app registration, or the client ID of a user-assigned managed identity when `"+fieldFromManagedIdentity+"` is `true`. Must be combined with `"+fieldClientSecret+"`, `"+fieldClientCertificatePath+"`, `"+fieldFederatedTokenFile+"` or `"+fieldFromManagedIdentity+"`; the default credential chain only reads the `AZURE_CLIENT_ID` environment variable.").
			Example("00000000-0000-0000-0000-000000000000").
			Default(""),
		service.NewStringField(fieldClientSecret).
			Description("A client secret for the service principal identified by `"+fieldClientID+"`. When set, Bento authenticates as that service principal."+envNote("AZURE_CLIENT_SECRET")).
			Default("").
			Secret(),
		service.NewStringField(fieldClientCertificatePath).
			Description("Path to a PEM or PKCS#12 file holding a certificate and private key for the service principal identified by `"+fieldClientID+"`. When set, Bento authenticates as that service principal."+envNote("AZURE_CLIENT_CERTIFICATE_PATH")).
			Example("/etc/bento/sp-cert.pem").
			Default("").
			Advanced(),
		service.NewStringField(fieldClientCertificatePassword).
			Description("The password protecting the certificate file in `"+fieldClientCertificatePath+"`, if any."+envNote("AZURE_CLIENT_CERTIFICATE_PASSWORD")).
			Default("").
			Secret().
			Advanced(),
		service.NewStringField(fieldFederatedTokenFile).
			Description("Path to a file containing a federated (OIDC) token, used for [workload identity](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview), for example on AKS. When set, Bento authenticates with workload identity using `"+fieldTenantID+"` and `"+fieldClientID+"`."+envNote("AZURE_FEDERATED_TOKEN_FILE")+" The AKS workload identity webhook sets these environment variables automatically.").
			Example("/var/run/secrets/azure/tokens/azure-identity-token").
			Default("").
			Advanced(),
		service.NewBoolField(fieldFromManagedIdentity).
			Description("Authenticate with the [managed identity](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview) of the Azure host (VM, App Service, Container Apps, Functions, etc). Uses the system-assigned identity unless `"+fieldClientID+"` or `"+fieldManagedIdentityResourceID+"` selects a user-assigned identity. There is no environment variable that enables this, but the default credential chain tries managed identity anyway when no other credential applies.").
			Default(false),
		service.NewStringField(fieldManagedIdentityResourceID).
			Description("The full resource ID of a user-assigned managed identity, used with `"+fieldFromManagedIdentity+"`. This is an alternative to setting `"+fieldClientID+"`; don't set both.").
			Example("/subscriptions/<subscription>/resourceGroups/<group>/providers/Microsoft.ManagedIdentity/userAssignedIdentities/<name>").
			Default("").
			Advanced(),
		service.NewStringField(fieldAuthorityHost).
			Description("The Microsoft Entra authority host. Only change this for sovereign clouds, such as `https://login.microsoftonline.us/` for Azure Government or `https://login.chinacloudapi.cn/` for Azure China. Defaults to the Azure public cloud. Equivalent environment variable: `AZURE_AUTHORITY_HOST`, which is read by every credential type except managed identity when this field is empty.").
			Example("https://login.microsoftonline.us/").
			Default("").
			Advanced(),
		service.NewStringListField(fieldAdditionallyAllowedTenants).
			Description("Tenants, besides `"+fieldTenantID+"`, that tokens may be requested for. Use `*` to allow any tenant."+envNote("AZURE_ADDITIONALLY_ALLOWED_TENANTS")+" The environment variable takes a semicolon-separated list.").
			Default([]any{}).
			Advanced(),
	).
		Description(`Optional configuration of [Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity/) credentials. These are used whenever no connection string, account key or SAS token is set for this component. Bento uses the first of the following that applies:

1. ` + "`" + fieldClientSecret + "`" + ` is set: a service principal with a client secret.
2. ` + "`" + fieldClientCertificatePath + "`" + ` is set: a service principal with a certificate.
3. ` + "`" + fieldFederatedTokenFile + "`" + ` is set: workload identity.
4. ` + "`" + fieldFromManagedIdentity + "`" + ` is ` + "`true`" + `: the host's managed identity.
5. Otherwise the [DefaultAzureCredential](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential) chain. This tries environment variables (` + "`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_CLIENT_CERTIFICATE_PATH`" + `, etc.), then workload identity, managed identity, the Azure CLI, the Azure Developer CLI and Azure PowerShell, in that order. Set the ` + "`AZURE_TOKEN_CREDENTIALS`" + ` environment variable to ` + "`prod`" + ` (environment, workload identity, managed identity), ` + "`dev`" + ` (the developer tools) or the name of a single credential type (e.g. ` + "`ManagedIdentityCredential`" + `) to narrow the chain.

Learn more [in this document](/docs/guides/cloud/azure).`).
		LintRule(lintRule).
		Version("1.22.0").
		Advanced()
}

const lintRule = `
let tenant = this.tenant_id.or("")
let client = this.client_id.or("")
let secret = this.client_secret.or("")
let cert = this.client_certificate_path.or("")
root = [
  if $secret != "" && ($tenant == "" || $client == "") { "client_secret requires both tenant_id and client_id to be set" },
  if $cert != "" && ($tenant == "" || $client == "") { "client_certificate_path requires both tenant_id and client_id to be set" },
  if $secret != "" && $cert != "" { "only one of client_secret and client_certificate_path may be set" },
  if $client != "" && this.managed_identity_resource_id.or("") != "" { "only one of client_id and managed_identity_resource_id may be set" },
  if $client != "" && $secret == "" && $cert == "" && this.federated_token_file.or("") == "" && !this.from_managed_identity.or(false) { "client_id is only used together with client_secret, client_certificate_path, federated_token_file or from_managed_identity, set one of these or use the AZURE_CLIENT_ID environment variable with the default credential chain" },
].filter(v -> v != null)
`

// IsSetBloblang is a Bloblang query, evaluated against a component config,
// that returns true when any field within `credentials` has been set to a
// non-default value. It's intended for use within component lint rules.
const IsSetBloblang = `this.credentials.or({}).values().any(v -> v != "" && v != false && v != [])`

// GetTokenCredential returns an azcore.TokenCredential built from the
// `credentials` field of the provided config. When the field is absent or
// empty the DefaultAzureCredential chain is returned.
func GetTokenCredential(parsedConf *service.ParsedConfig) (azcore.TokenCredential, error) {
	if !parsedConf.Contains(FieldCredentials) {
		return azidentity.NewDefaultAzureCredential(nil)
	}
	conf := parsedConf.Namespace(FieldCredentials)

	tenantID, _ := conf.FieldString(fieldTenantID)
	clientID, _ := conf.FieldString(fieldClientID)
	clientSecret, _ := conf.FieldString(fieldClientSecret)
	certPath, _ := conf.FieldString(fieldClientCertificatePath)
	certPassword, _ := conf.FieldString(fieldClientCertificatePassword)
	tokenFile, _ := conf.FieldString(fieldFederatedTokenFile)
	fromMI, _ := conf.FieldBool(fieldFromManagedIdentity)
	miResourceID, _ := conf.FieldString(fieldManagedIdentityResourceID)
	authorityHost, _ := conf.FieldString(fieldAuthorityHost)
	allowedTenants, _ := conf.FieldStringList(fieldAdditionallyAllowedTenants)

	clientOpts := policy.ClientOptions{}
	if authorityHost != "" {
		clientOpts.Cloud = cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost}
	}

	switch {
	case clientSecret != "":
		if tenantID == "" || clientID == "" {
			return nil, errors.New("credentials.client_secret requires credentials.tenant_id and credentials.client_id")
		}
		return azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, &azidentity.ClientSecretCredentialOptions{
			ClientOptions:              clientOpts,
			AdditionallyAllowedTenants: allowedTenants,
		})

	case certPath != "":
		if tenantID == "" || clientID == "" {
			return nil, errors.New("credentials.client_certificate_path requires credentials.tenant_id and credentials.client_id")
		}
		certData, err := os.ReadFile(certPath)
		if err != nil {
			return nil, fmt.Errorf("reading client certificate: %w", err)
		}
		var password []byte
		if certPassword != "" {
			password = []byte(certPassword)
		}
		certs, key, err := azidentity.ParseCertificates(certData, password)
		if err != nil {
			return nil, fmt.Errorf("parsing client certificate: %w", err)
		}
		return azidentity.NewClientCertificateCredential(tenantID, clientID, certs, key, &azidentity.ClientCertificateCredentialOptions{
			ClientOptions:              clientOpts,
			AdditionallyAllowedTenants: allowedTenants,
		})

	case tokenFile != "":
		return azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientOptions:              clientOpts,
			AdditionallyAllowedTenants: allowedTenants,
			ClientID:                   clientID,
			TenantID:                   tenantID,
			TokenFilePath:              tokenFile,
		})

	case fromMI:
		if clientID != "" && miResourceID != "" {
			return nil, errors.New("only one of credentials.client_id and credentials.managed_identity_resource_id may be set")
		}
		opts := &azidentity.ManagedIdentityCredentialOptions{ClientOptions: clientOpts}
		if clientID != "" {
			opts.ID = azidentity.ClientID(clientID)
		} else if miResourceID != "" {
			opts.ID = azidentity.ResourceID(miResourceID)
		}
		return azidentity.NewManagedIdentityCredential(opts)
	}

	return azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
		ClientOptions:              clientOpts,
		AdditionallyAllowedTenants: allowedTenants,
		TenantID:                   tenantID,
	})
}

// Docs is a shared documentation section describing authentication for Azure
// components. The placeholder legacy describes the component specific legacy
// (shared key) authentication fields.
func Docs(legacy string) string {
	return `

## Authentication

Azure components authenticate with [Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity/) via the ` + "`credentials`" + ` field, and this is the recommended approach. Because tokens are short-lived and scoped by Azure RBAC role assignments, there is no long-lived secret to leak or rotate when using a managed identity or workload identity.

When no credentials are configured at all, Bento uses the [DefaultAzureCredential](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential) chain, which picks up the standard ` + "`AZURE_*`" + ` environment variables, workload identity, managed identity and finally developer tools such as the Azure CLI. For example, to authenticate as a service principal:

` + "```yml" + `
credentials:
  tenant_id: ${AZURE_TENANT_ID}
  client_id: ${AZURE_CLIENT_ID}
  client_secret: ${AZURE_CLIENT_SECRET}
` + "```" + `

Or using the managed identity of the host:

` + "```yml" + `
credentials:
  from_managed_identity: true
` + "```" + `

` + legacy + ` These take precedence over ` + "`credentials`" + ` when set, but are discouraged: they are long-lived secrets that grant broad access and must be rotated manually.

The identity used needs an appropriate Azure RBAC data-plane role assignment on the target resource. Find out more [in this document](/docs/guides/cloud/azure).
`
}
