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

	// Same scope as Application Default Credentials.
	gcpImpersonationScope = "https://www.googleapis.com/auth/cloud-platform"
)

// gcpCredentialsField returns the shared `credentials` config field for GCP components.
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

// gcpClientOptionsFromParsed builds client options from the `credentials` config.
// Returns nil for clients to fall back to Application Default Credentials when unset.
// impersonateOpts are passed to the impersonation client (used by tests).
func gcpClientOptionsFromParsed(pConf *service.ParsedConfig, impersonateOpts ...option.ClientOption) ([]option.ClientOption, error) {
	if !pConf.Contains(gcpFieldCredentials) {
		return nil, nil
	}
	cConf := pConf.Namespace(gcpFieldCredentials)

	target, err := cConf.FieldString(gcpFieldImpersonateServiceAccount)
	if err != nil {
		return nil, err
	}
	delegates, err := cConf.FieldStringList(gcpFieldImpersonateDelegates)
	if err != nil {
		return nil, err
	}
	if target == "" {
		// Lint rule doesn't cover programmatically built configs.
		if len(delegates) > 0 {
			return nil, fmt.Errorf("%v requires %v to be set", gcpFieldImpersonateDelegates, gcpFieldImpersonateServiceAccount)
		}
		return nil, nil
	}

	ts, err := impersonate.CredentialsTokenSource(context.Background(), impersonate.CredentialsConfig{
		TargetPrincipal: target,
		Delegates:       delegates,
		Scopes:          []string{gcpImpersonationScope},
	}, impersonateOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create impersonated credentials for %v: %w", target, err)
	}
	return []option.ClientOption{option.WithTokenSource(ts)}, nil
}
