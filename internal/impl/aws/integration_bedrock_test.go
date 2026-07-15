package aws

import (
	"context"
	"os"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// TestIntegrationBedrockEmbeddings exercises the aws_bedrock_embeddings
// processor against a live Amazon Bedrock endpoint, confirming that the
// provider-specific request/response encoding matches what Bedrock actually
// returns (which the unit tests, using a mock client, cannot verify).
//
// It is skipped unless TEST_AWS_BEDROCK_MODEL is set, so it never runs in CI.
// To run it locally against real AWS (credentials resolved from the default
// chain, e.g. an SSO profile):
//
//	TEST_AWS_BEDROCK_MODEL=amazon.titan-embed-text-v2:0 \
//	AWS_REGION=us-east-1 \
//	go test ./internal/impl/aws/ -run TestIntegrationBedrockEmbeddings -v
//
// The model must be one your account has been granted access to in that region.
func TestIntegrationBedrockEmbeddings(t *testing.T) {
	model := os.Getenv("TEST_AWS_BEDROCK_MODEL")
	if model == "" {
		t.Skip("Skipping: set TEST_AWS_BEDROCK_MODEL (and AWS credentials/region) to run the live Bedrock test")
	}

	ctx := context.Background()

	awsConf, err := awsconfig.LoadDefaultConfig(ctx)
	require.NoError(t, err)

	provider := os.Getenv("TEST_AWS_BEDROCK_PROVIDER")
	if provider == "" {
		provider = "auto"
	}

	proc, err := newBedrockEmbeddingsProc(
		bedrockruntime.NewFromConfig(awsConf),
		model,
		provider,
		"search_document",
		service.MockResources(),
	)
	require.NoError(t, err)

	inputs := []string{
		"The quick brown fox jumps over the lazy dog.",
		"Bento is a stream processor.",
	}
	batch := make(service.MessageBatch, 0, len(inputs))
	for _, in := range inputs {
		batch = append(batch, service.NewMessage([]byte(in)))
	}

	outBatches, err := proc.ProcessBatch(ctx, batch)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], len(inputs))

	for i, msg := range outBatches[0] {
		require.NoError(t, msg.GetError(), "message %d returned an error", i)

		structured, err := msg.AsStructured()
		require.NoError(t, err)

		embedding, ok := structured.([]float64)
		require.Truef(t, ok, "message %d embedding was %T, not []float64", i, structured)
		require.NotEmpty(t, embedding, "message %d embedding was empty", i)

		t.Logf("model %q returned a %d-dimensional embedding for input %d (first values: %v)",
			model, len(embedding), i, embedding[:min(4, len(embedding))])
	}
}
