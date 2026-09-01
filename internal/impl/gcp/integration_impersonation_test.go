package gcp_test

// Live-cloud opt-in test for service account impersonation. There is no
// emulator for the IAM Credentials API, so this needs a real GCP project and
// is skipped unless these are set:
//
//	GCP_E2E_BUCKET        bucket writable by the storage SA only
//	GCP_E2E_STORAGE_SA    SA with roles/storage.objectAdmin on the bucket
//	GCP_E2E_PROJECT       project id                          (split-SA test only)
//	GCP_E2E_TOPIC         topic the input SA can publish to   (split-SA test only)
//	GCP_E2E_SUBSCRIPTION  subscription the input SA can read  (split-SA test only)
//	GCP_E2E_INPUT_SA      SA with roles/pubsub.subscriber on the subscription
//	                      and roles/pubsub.publisher on the topic (split-SA test only)
//	GCP_E2E_HOP_SA        intermediate SA for delegated impersonation: the base
//	                      ADC must hold roles/iam.serviceAccountTokenCreator on
//	                      it, and it on the storage SA  (delegated test only)
//
// The base ADC must hold roles/iam.serviceAccountTokenCreator on both SAs and,
// for the result to prove impersonation, no data permissions of its own.
// resources/impersonation_e2e.sh provisions all of the above. From the repo root:
//
//	export PROJECT_ID=<your-gcp-project>
//	./internal/impl/gcp/resources/impersonation_e2e.sh setup     # then wait ~5 min for IAM propagation
//	./internal/impl/gcp/resources/impersonation_e2e.sh adc       # base ADC = a powerless runtime SA
//	<the go test command printed by setup>
//	./internal/impl/gcp/resources/impersonation_e2e.sh teardown  # afterwards; deletes everything
//	gcloud auth application-default revoke                       # restore your normal ADC

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub" //nolint:staticcheck
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func impersonatedCredsOpt(t *testing.T, targetSA string) option.ClientOption {
	t.Helper()
	ts, err := impersonate.CredentialsTokenSource(context.Background(), impersonate.CredentialsConfig{
		TargetPrincipal: targetSA,
		Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform"},
	})
	require.NoError(t, err)
	return option.WithTokenSource(ts)
}

func TestIntegrationGCPImpersonationLive(t *testing.T) {
	integration.CheckSkip(t)

	bucket := os.Getenv("GCP_E2E_BUCKET")
	storageSA := os.Getenv("GCP_E2E_STORAGE_SA")
	if bucket == "" || storageSA == "" {
		t.Skip("live GCP test: set GCP_E2E_BUCKET and GCP_E2E_STORAGE_SA (see provisioning steps in this file's doc comment)")
	}

	// Scenario 1: a stream reads and writes a real bucket that only the
	// impersonated SA can access; the base ADC alone would be denied.
	t.Run("gcs_roundtrip_impersonated", func(t *testing.T) {
		template := `
output:
  gcp_cloud_storage:
    bucket: $VAR1
    path: impersonation-e2e/$ID/${!count("$ID")}.txt
    max_in_flight: 1
    timeout: 15s # the first upload includes minting the impersonated token
    credentials:
      impersonate_service_account: $VAR2

input:
  gcp_cloud_storage:
    bucket: $VAR1
    prefix: impersonation-e2e/$ID/
    credentials:
      impersonate_service_account: $VAR2
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(5),
		).Run(
			t, template,
			integration.StreamTestOptVarSet("VAR1", bucket),
			integration.StreamTestOptVarSet("VAR2", storageSA),
		)
	})

	// Delegated impersonation: the token for the storage SA is minted through
	// an intermediate hop SA (base -> hop -> storage) rather than directly.
	t.Run("gcs_roundtrip_delegated", func(t *testing.T) {
		hopSA := os.Getenv("GCP_E2E_HOP_SA")
		if hopSA == "" {
			t.Skip("set GCP_E2E_HOP_SA")
		}

		template := `
output:
  gcp_cloud_storage:
    bucket: $VAR1
    path: impersonation-e2e-delegated/$ID/${!count("$ID")}.txt
    max_in_flight: 1
    timeout: 15s # the first upload includes minting the impersonated token
    credentials:
      impersonate_service_account: $VAR2
      impersonate_delegates: [ $VAR3 ]

input:
  gcp_cloud_storage:
    bucket: $VAR1
    prefix: impersonation-e2e-delegated/$ID/
    credentials:
      impersonate_service_account: $VAR2
      impersonate_delegates: [ $VAR3 ]
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
		).Run(
			t, template,
			integration.StreamTestOptVarSet("VAR1", bucket),
			integration.StreamTestOptVarSet("VAR2", storageSA),
			integration.StreamTestOptVarSet("VAR3", hopSA),
		)
	})

	// Scenario 2: one stream where input and output impersonate DIFFERENT
	// SAs, each holding only its own side's permissions.
	t.Run("split_sa_pubsub_to_gcs", func(t *testing.T) {
		project := os.Getenv("GCP_E2E_PROJECT")
		topic := os.Getenv("GCP_E2E_TOPIC")
		sub := os.Getenv("GCP_E2E_SUBSCRIPTION")
		inputSA := os.Getenv("GCP_E2E_INPUT_SA")
		if project == "" || topic == "" || sub == "" || inputSA == "" {
			t.Skip("set GCP_E2E_PROJECT, GCP_E2E_TOPIC, GCP_E2E_SUBSCRIPTION and GCP_E2E_INPUT_SA")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		prefix := fmt.Sprintf("impersonation-e2e-split/%v", time.Now().UnixNano())
		conf := fmt.Sprintf(`
input:
  gcp_pubsub:
    project: %v
    subscription: %v
    credentials:
      impersonate_service_account: %v

output:
  gcp_cloud_storage:
    bucket: %v
    path: '%v/${!uuid_v4()}.json'
    max_in_flight: 1
    timeout: 15s # the first upload includes minting the impersonated token
    credentials:
      impersonate_service_account: %v
`, project, sub, inputSA, bucket, prefix, storageSA)

		sb := service.NewStreamBuilder()
		require.NoError(t, sb.SetYAML(conf))
		stream, err := sb.Build()
		require.NoError(t, err)

		streamDone := make(chan error, 1)
		go func() { streamDone <- stream.Run(ctx) }()

		// Publish as the input SA (needs roles/pubsub.publisher on the topic;
		// scaffolding only — the assertion is about the stream's identities).
		pubClient, err := pubsub.NewClient(ctx, project, impersonatedCredsOpt(t, inputSA))
		require.NoError(t, err)
		defer pubClient.Close()

		const numMessages = 3
		tp := pubClient.Topic(topic)
		defer tp.Stop()
		for i := 0; i < numMessages; i++ {
			_, err := tp.Publish(ctx, &pubsub.Message{Data: []byte(fmt.Sprintf(`{"n":%v}`, i))}).Get(ctx)
			require.NoError(t, err)
		}

		// Verify delivery by listing the bucket as the storage SA.
		stClient, err := storage.NewClient(ctx, impersonatedCredsOpt(t, storageSA))
		require.NoError(t, err)
		defer stClient.Close()

		require.Eventually(t, func() bool {
			count := 0
			it := stClient.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix + "/"})
			for {
				if _, err := it.Next(); err != nil {
					if err != iterator.Done {
						t.Logf("listing bucket: %v", err)
					}
					break
				}
				count++
			}
			return count >= numMessages
		}, 90*time.Second, 2*time.Second, "expected %v objects under %v/", numMessages, prefix)

		cancel()
		<-streamDone
	})
}
