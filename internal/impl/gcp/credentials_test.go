package gcp

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

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

func TestGCPCredentialsDefaultNoOptions(t *testing.T) {
	pConf, err := testCredentialsSpec().ParseYAML(`{}`, nil)
	require.NoError(t, err)

	opts, err := gcpClientOptionsFromParsed(pConf)
	require.NoError(t, err)
	require.Empty(t, opts)
}

func TestGCPCredentialsImpersonation(t *testing.T) {
	setFakeADC(t)

	pConf, err := testCredentialsSpec().ParseYAML(`
credentials:
  impersonate_service_account: target@test-project.iam.gserviceaccount.com
  impersonate_delegates:
    - hop@test-project.iam.gserviceaccount.com
`, nil)
	require.NoError(t, err)

	opts, err := gcpClientOptionsFromParsed(pConf)
	require.NoError(t, err)
	require.Len(t, opts, 1)
}

func TestGCPCredentialsTargetOnlyLintHappy(t *testing.T) {
	err := service.NewStreamBuilder().SetYAML(`
input:
  gcp_pubsub:
    project: foo
    subscription: bar
    credentials:
      impersonate_service_account: target@test-project.iam.gserviceaccount.com
output:
  drop: {}
`)
	require.NoError(t, err)
}

func TestGCPCredentialsDelegatesWithoutTargetLint(t *testing.T) {
	err := service.NewStreamBuilder().SetYAML(`
input:
  gcp_pubsub:
    project: foo
    subscription: bar
    credentials:
      impersonate_delegates:
        - hop@test-project.iam.gserviceaccount.com
output:
  drop: {}
`)
	require.ErrorContains(t, err, "impersonate_delegates requires impersonate_service_account")
}
