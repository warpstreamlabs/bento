package gcp

import (
	"context"
	"fmt"

	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	gcpFieldCredentials               = "credentials"
	gcpFieldImpersonateServiceAccount = "impersonate_service_account"
	gcpFieldImpersonateDelegates      = "impersonate_delegates"

	// gcpImpersonationScope grants the impersonated token access to all GCP
	// APIs, mirroring the scope granted to Application Default Credentials.
	gcpImpersonationScope = "https://www.googleapis.com/auth/cloud-platform"
)

// gcpCredentialsField defines a re-usable set of config fields for
// customising the credentials used by GCP components. By default components
// rely on Application Default Credentials.
func gcpCredentialsField() *service.ConfigField {
	return service.NewObjectField(gcpFieldCredentials,
		service.NewStringField(gcpFieldImpersonateServiceAccount).
			Description("The email address of a service account to impersonate. The base credentials (Application Default Credentials) must be granted `roles/iam.serviceAccountTokenCreator` on this service account, or on the first delegate when `"+gcpFieldImpersonateDelegates+"` is set.").
			Example("target@my-project.iam.gserviceaccount.com").
			Default(""),
		service.NewStringListField(gcpFieldImpersonateDelegates).
			Description("An optional chain of service account email addresses to delegate through when impersonating. Each service account must be granted `roles/iam.serviceAccountTokenCreator` on the next service account in the chain, with the final one granted on `"+gcpFieldImpersonateServiceAccount+"`.").
			Default([]any{}),
	).
		Description("Optional manual configuration of GCP credentials to use. More information can be found [in this document](/docs/guides/cloud/gcp).").
		Advanced().
		Version("1.22.0").
		LintRule(`root = if this.impersonate_delegates.length() > 0 && this.impersonate_service_account == "" { "impersonate_delegates requires impersonate_service_account to be set" }`)
}

// gcpClientOptionsFromParsed returns client options that apply the credentials
// configuration to a GCP client. When no credentials are configured the
// returned options are empty and clients fall back to Application Default
// Credentials.
func gcpClientOptionsFromParsed(pConf *service.ParsedConfig) ([]option.ClientOption, error) {
	if !pConf.Contains(gcpFieldCredentials) {
		return nil, nil
	}
	cConf := pConf.Namespace(gcpFieldCredentials)

	target, err := cConf.FieldString(gcpFieldImpersonateServiceAccount)
	if err != nil {
		return nil, err
	}
	if target == "" {
		return nil, nil
	}

	delegates, err := cConf.FieldStringList(gcpFieldImpersonateDelegates)
	if err != nil {
		return nil, err
	}

	ts, err := impersonate.CredentialsTokenSource(context.Background(), impersonate.CredentialsConfig{
		TargetPrincipal: target,
		Delegates:       delegates,
		Scopes:          []string{gcpImpersonationScope},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create impersonated credentials for %v: %w", target, err)
	}
	return []option.ClientOption{option.WithTokenSource(ts)}, nil
}
