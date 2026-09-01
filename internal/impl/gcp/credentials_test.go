package gcp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/warpstreamlabs/bento/public/service"
)

func testCredentialsSpec() *service.ConfigSpec {
	return service.NewConfigSpec().Field(gcpCredentialsField())
}

// setFakeADC points GOOGLE_APPLICATION_CREDENTIALS at a syntactically valid
// service account key so that base credentials can be resolved offline.
func setFakeADC(t *testing.T) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	saJSON, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "test-project",
		"private_key_id": "abc123",
		"private_key":    string(pemKey),
		"client_email":   "base@test-project.iam.gserviceaccount.com",
		"client_id":      "123456789",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "sa.json")
	require.NoError(t, os.WriteFile(path, saJSON, 0o600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
}

func TestGCPCredentialsClientOptions(t *testing.T) {
	setFakeADC(t)

	tests := []struct {
		name        string
		conf        string
		wantOpts    int
		errContains string
	}{
		{
			name:     "empty config falls back to ADC",
			conf:     `{}`,
			wantOpts: 0,
		},
		{
			name: "explicit empty target falls back to ADC",
			conf: `
credentials:
  impersonate_service_account: ""
`,
			wantOpts: 0,
		},
		{
			name: "target set yields token source option",
			conf: `
credentials:
  impersonate_service_account: target@test-project.iam.gserviceaccount.com
`,
			wantOpts: 1,
		},
		{
			name: "target with delegates yields token source option",
			conf: `
credentials:
  impersonate_service_account: target@test-project.iam.gserviceaccount.com
  impersonate_delegates:
    - hop@test-project.iam.gserviceaccount.com
`,
			wantOpts: 1,
		},
		{
			name: "delegates without target is an error",
			conf: `
credentials:
  impersonate_delegates:
    - hop@test-project.iam.gserviceaccount.com
`,
			errContains: "impersonate_delegates requires impersonate_service_account",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := testCredentialsSpec().ParseYAML(test.conf, nil)
			require.NoError(t, err)

			opts, err := gcpClientOptionsFromParsed(pConf)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
				return
			}
			require.NoError(t, err)
			require.Len(t, opts, test.wantOpts)
		})
	}
}

// redirectTransport rewrites every request to the given test server host,
// since the impersonate library hardcodes the IAM Credentials API URL.
type redirectTransport struct {
	host string
}

func (rt redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.URL.Scheme = "http"
	r.URL.Host = rt.host
	return http.DefaultTransport.RoundTrip(r)
}

// TestGCPCredentialsImpersonationFailure verifies how a component behaves when
// the IAM Credentials API denies impersonation: construction succeeds (tokens
// are minted lazily) and the first operation fails with a clear error. The
// entire flow is served by a local test server.
func TestGCPCredentialsImpersonationFailure(t *testing.T) {
	// Fake IAM Credentials API that denies every generateAccessToken request.
	iamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Permission 'iam.serviceAccounts.getAccessToken' denied","status":"PERMISSION_DENIED"}}`))
	}))
	defer iamSrv.Close()

	setFakeADC(t)

	pConf, err := testCredentialsSpec().ParseYAML(`
credentials:
  impersonate_service_account: target@test-project.iam.gserviceaccount.com
`, nil)
	require.NoError(t, err)

	iamHost := strings.TrimPrefix(iamSrv.URL, "http://")
	iamClient := &http.Client{Transport: redirectTransport{host: iamHost}}

	// Component construction must succeed: impersonated tokens are minted on
	// first use, not at startup.
	opts, err := gcpClientOptionsFromParsed(pConf, option.WithHTTPClient(iamClient))
	require.NoError(t, err)
	require.Len(t, opts, 1)

	// The first client operation triggers token minting and must surface the
	// denial as a clear error. The endpoint override keeps the client from
	// reaching real GCS (the request fails at auth, before any data call).
	client, err := storage.NewClient(context.Background(), append(opts, option.WithEndpoint(iamSrv.URL))...)
	require.NoError(t, err)
	defer client.Close()

	_, err = client.Bucket("any-bucket").Attrs(context.Background())
	require.ErrorContains(t, err, "impersonate")
	require.ErrorContains(t, err, "403")
}

func TestGCPCredentialsLint(t *testing.T) {
	linter := service.GlobalEnvironment().FullConfigSchema("", "").NewStreamConfigLinter()

	tests := []struct {
		name         string
		conf         string
		lintContains string
	}{
		{
			name: "target only is valid",
			conf: `
input:
  gcp_pubsub:
    project: foo
    subscription: bar
    credentials:
      impersonate_service_account: target@test-project.iam.gserviceaccount.com
`,
		},
		{
			name: "target with delegates is valid",
			conf: `
input:
  gcp_pubsub:
    project: foo
    subscription: bar
    credentials:
      impersonate_service_account: target@test-project.iam.gserviceaccount.com
      impersonate_delegates:
        - hop@test-project.iam.gserviceaccount.com
`,
		},
		{
			name: "delegates without target lints",
			conf: `
input:
  gcp_pubsub:
    project: foo
    subscription: bar
    credentials:
      impersonate_delegates:
        - hop@test-project.iam.gserviceaccount.com
`,
			lintContains: "impersonate_delegates requires impersonate_service_account",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lints, err := linter.LintYAML([]byte(test.conf))
			require.NoError(t, err)
			if test.lintContains == "" {
				require.Empty(t, lints)
				return
			}
			require.Len(t, lints, 1)
			require.Contains(t, lints[0].What, test.lintContains)
		})
	}
}
