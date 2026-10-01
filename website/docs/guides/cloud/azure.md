---
title: Microsoft Azure
description: Find out about authenticating with Azure in Bento.
---

There are many components within Bento which utilise Azure services:

- [`azure_blob_storage` input](/docs/components/inputs/azure_blob_storage) and [output](/docs/components/outputs/azure_blob_storage)
- [`azure_queue_storage` input](/docs/components/inputs/azure_queue_storage) and [output](/docs/components/outputs/azure_queue_storage)
- [`azure_table_storage` input](/docs/components/inputs/azure_table_storage) and [output](/docs/components/outputs/azure_table_storage)
- [`azure_service_bus_queue` input](/docs/components/inputs/azure_service_bus_queue)
- [`azure_cosmosdb` input](/docs/components/inputs/azure_cosmosdb), [output](/docs/components/outputs/azure_cosmosdb) and [processor](/docs/components/processors/azure_cosmosdb)

Each of these components contains a configuration section under the field `credentials`, which configures authentication with [Microsoft Entra ID][entra] (formerly Azure Active Directory), of the format:

```yml
credentials:
  tenant_id: ""
  client_id: ""
  client_secret: ""
  client_certificate_path: ""
  client_certificate_password: ""
  federated_token_file: ""
  from_managed_identity: false
  managed_identity_resource_id: ""
  authority_host: ""
  additionally_allowed_tenants: []
```

This document explains what each field is responsible for and how it might be used.

## Why Entra ID over connection strings

Most Azure components also accept connection strings, account keys or SAS tokens (`storage_connection_string`, `storage_access_key`, `storage_sas_token`, `connection_string`, `account_key`). These still work, and take priority over `credentials` when set, but they are discouraged:

- They are long-lived secrets that must be distributed to every host running Bento and rotated manually.
- Account keys grant full control of the entire account rather than the specific resources Bento needs.
- Access granted with them can't be audited per identity, or revoked without rotating the key for everybody.

Entra ID tokens are short-lived, refreshed automatically, and scoped by [Azure RBAC][rbac] role assignments. Managed identity and workload identity remove the need to handle a secret at all.

## None of these fields are compulsory

All of the `credentials` fields are optional. When they are all left blank, Bento uses the [DefaultAzureCredential][default-azure-credential] chain, which tries each of the following in order and uses the first that succeeds:

1. **Environment variables**: a service principal configured with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and either `AZURE_CLIENT_SECRET` or `AZURE_CLIENT_CERTIFICATE_PATH` (plus optional `AZURE_CLIENT_CERTIFICATE_PASSWORD` and `AZURE_CLIENT_SEND_CERTIFICATE_CHAIN`).
2. **Workload identity**: when `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_FEDERATED_TOKEN_FILE` are set, as done by the AKS workload identity webhook.
3. **Managed identity**: the identity of the Azure host. Set `AZURE_CLIENT_ID` to select a user-assigned identity.
4. **Azure CLI**: the account logged in with `az login`.
5. **Azure Developer CLI**: the account logged in with `azd auth login`.
6. **Azure PowerShell**: the account logged in with `Connect-AzAccount`.

This means that a Bento config with no credentials at all often just works, both locally (via `az login`) and when deployed to Azure (via managed identity).

The `AZURE_TOKEN_CREDENTIALS` environment variable narrows the chain. This makes authentication more predictable and avoids slow fall-through in production:

| Value | Credentials tried |
|-------|-------------------|
| `prod` | Environment variables, workload identity, managed identity |
| `dev` | Azure CLI, Azure Developer CLI, Azure PowerShell |
| A credential type name, e.g. `ManagedIdentityCredential`, `AzureCLICredential` | Only that credential |

The `tenant_id`, `authority_host` and `additionally_allowed_tenants` fields are also applied to the default chain when set.

## Explicit Credentials

By explicitly setting credentials at the component level it's possible to connect to resources using different identities within the same Bento process. Bento picks the credential type from the fields you set, using the first of these that applies:

| Fields set | Credential used |
|------------|-----------------|
| `client_secret` | Service principal with a client secret |
| `client_certificate_path` | Service principal with a certificate |
| `federated_token_file` | Workload identity |
| `from_managed_identity: true` | Managed identity |
| none of the above | The default chain described above |

The fields are linted, so configuring for example a `client_secret` without a `tenant_id` will produce a lint error.

### Service Principal with a Secret

Create an [app registration][app-registration] with a client secret, then set:

```yml
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 11111111-1111-1111-1111-111111111111
  client_secret: ${AZURE_CLIENT_SECRET}
```

Avoid placing secrets directly in config files. Use [environment variable interpolation](/docs/configuration/interpolation) as above, or simply leave the `credentials` block empty and set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET`, which the default chain picks up.

### Service Principal with a Certificate

Certificates are preferred to client secrets for service principals. The file may be PEM or PKCS#12 and must contain both the certificate and its private key:

```yml
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 11111111-1111-1111-1111-111111111111
  client_certificate_path: /etc/bento/sp-cert.pem
  client_certificate_password: ${CERT_PASSWORD} # only if the file is encrypted
```

### Managed Identity

When running on an Azure host with a [managed identity][managed-identity] (Virtual Machines, Container Apps, App Service, Functions, AKS node pools, etc.), use:

```yml
credentials:
  from_managed_identity: true
```

This uses the system-assigned identity. To use a user-assigned identity, select it with either its client ID or its resource ID, but not both:

```yml
credentials:
  from_managed_identity: true
  client_id: 11111111-1111-1111-1111-111111111111
```

```yml
credentials:
  from_managed_identity: true
  managed_identity_resource_id: /subscriptions/<subscription>/resourceGroups/<group>/providers/Microsoft.ManagedIdentity/userAssignedIdentities/<name>
```

### Workload Identity (AKS)

With [workload identity][workload-identity] enabled on an AKS cluster and the pod's service account annotated with `azure.workload.identity/client-id`, the webhook injects `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_FEDERATED_TOKEN_FILE` into the pod. The default chain picks these up with no Bento config at all. To configure workload identity explicitly instead:

```yml
credentials:
  tenant_id: 00000000-0000-0000-0000-000000000000
  client_id: 11111111-1111-1111-1111-111111111111
  federated_token_file: /var/run/secrets/azure/tokens/azure-identity-token
```

### Local Development

Run `az login` and leave `credentials` empty. The default chain uses your Azure CLI account. Your user needs the same role assignments as the production identity.

## Sovereign Clouds

For Azure Government, Azure China or other national clouds, set the Entra authority host (or the `AZURE_AUTHORITY_HOST` environment variable):

```yml
credentials:
  authority_host: https://login.microsoftonline.us/
```

Bear in mind that the resource endpoints (e.g. a Cosmos DB `endpoint`, a Service Bus `namespace`) must also point at the sovereign cloud.

## Multi-tenant

By default a credential only acquires tokens for its own `tenant_id`. To allow it to request tokens for other tenants, list them (or `*` for any) in `additionally_allowed_tenants`, or set the `AZURE_ADDITIONALLY_ALLOWED_TENANTS` environment variable to a semicolon-separated list.

## Field Reference

| Field | Environment variable | Description |
|-------|----------------------|-------------|
| `tenant_id` | `AZURE_TENANT_ID` | Entra tenant ID |
| `client_id` | `AZURE_CLIENT_ID` | App registration client ID, or user-assigned managed identity client ID |
| `client_secret` | `AZURE_CLIENT_SECRET` | Service principal client secret |
| `client_certificate_path` | `AZURE_CLIENT_CERTIFICATE_PATH` | Path to a PEM/PKCS#12 certificate with private key |
| `client_certificate_password` | `AZURE_CLIENT_CERTIFICATE_PASSWORD` | Password for the certificate file |
| `federated_token_file` | `AZURE_FEDERATED_TOKEN_FILE` | Path to a federated token for workload identity |
| `from_managed_identity` | none | Use the host's managed identity |
| `managed_identity_resource_id` | none | Resource ID of a user-assigned managed identity |
| `authority_host` | `AZURE_AUTHORITY_HOST` | Entra authority host for sovereign clouds |
| `additionally_allowed_tenants` | `AZURE_ADDITIONALLY_ALLOWED_TENANTS` | Extra tenants tokens may be requested for |
| none | `AZURE_TOKEN_CREDENTIALS` | Narrows the default chain (`prod`, `dev` or a credential type name) |
| none | `AZURE_CLIENT_SEND_CERTIFICATE_CHAIN` | Send the certificate chain with environment variable certificate auth (for subject name/issuer auth) |

Config fields take precedence over environment variables. Note that `client_id` in the config only applies alongside one of `client_secret`, `client_certificate_path`, `federated_token_file` or `from_managed_identity`. The default chain only reads `AZURE_CLIENT_ID` from the environment.

## Required Roles

Entra ID authentication uses data-plane RBAC roles, which are separate from management roles such as `Owner` or `Contributor`. Assign the identity the following roles (or equivalent custom roles) on the target resource:

| Component | Read | Write |
|-----------|------|-------|
| `azure_blob_storage` | `Storage Blob Data Reader` | `Storage Blob Data Contributor` |
| `azure_queue_storage` | `Storage Queue Data Message Processor` | `Storage Queue Data Message Sender` |
| `azure_table_storage` | `Storage Table Data Reader` | `Storage Table Data Contributor` |
| `azure_service_bus_queue` | `Azure Service Bus Data Receiver` | n/a |
| `azure_cosmosdb` | `Cosmos DB Built-in Data Reader` | `Cosmos DB Built-in Data Contributor` |

Role assignments can take several minutes to propagate. Cosmos DB data-plane roles are assigned with `az cosmosdb sql role assignment create` rather than through the regular Access control (IAM) blade.

When authenticating with `credentials`, storage components still need the `storage_account` field, Service Bus needs `namespace`, and Cosmos DB needs `endpoint`, so that Bento knows which resource to connect to.

[entra]: https://learn.microsoft.com/en-us/entra/identity/
[rbac]: https://learn.microsoft.com/en-us/azure/role-based-access-control/overview
[default-azure-credential]: https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential
[app-registration]: https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app
[managed-identity]: https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview
[workload-identity]: https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview
