package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	bedFieldModel     = "model"
	bedFieldProvider  = "provider"
	bedFieldInputType = "input_type"
	bedFieldRateLimit = "rate_limit"

	bedProviderAuto   = "auto"
	bedProviderTitan  = "amazon_titan"
	bedProviderCohere = "cohere"
)

func init() {
	conf := service.NewConfigSpec().
		Beta().
		Categories("AI", "Integration").
		Summary("Generates vector embeddings for each message by invoking an Amazon Bedrock embedding model.").
		Description(`Invokes an [Amazon Bedrock](https://docs.aws.amazon.com/bedrock/latest/userguide/what-is-bedrock.html) embedding model for each message, replacing the message contents with the resulting embedding as an array of floats.

Requests are signed with AWS Signature Version 4 by the AWS SDK, so no additional authentication configuration is required beyond standard AWS credentials.

The ` + "`model`" + ` may be a foundation model ID (e.g. ` + "`amazon.titan-embed-text-v2:0`" + `) or an [inference profile](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles.html) ID or ARN. Some models (e.g. Cohere Embed v4) can only be invoked through an inference profile.

The request and response formats differ between model providers, so this processor understands the following families:

- Amazon Titan (` + "`amazon.titan-embed-*`" + `) — one request per message.
- Cohere Embed (` + "`cohere.embed-*`" + `), including v4 — up to 96 messages are sent in a single request, reducing round-trips and Bedrock request-rate throttling.

The family is normally inferred from the ` + "`model`" + ` ID. When ` + "`model`" + ` is an inference profile ARN the family cannot be inferred, so set ` + "`provider`" + ` explicitly.

Batching is driven by the incoming message batch: the messages in each batch are grouped into the largest requests the model supports. If a batched request fails, every message in that group is flagged with the error.

### Preserving the original message

By default the message contents are replaced with the embedding. To keep the original payload and store the embedding alongside it, use a [` + "`branch`" + `](/docs/components/processors/branch) processor.

### Credentials

By default Bento will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level. You can find out more [in this document](/docs/guides/cloud/aws).`).
		Field(service.NewStringField(bedFieldModel).
			Description("The ID of the Bedrock embedding model to invoke.").
			Examples(
				"amazon.titan-embed-text-v2:0",
				"amazon.titan-embed-text-v1",
				"cohere.embed-english-v3",
				"cohere.embed-multilingual-v3",
			)).
		Field(service.NewStringField(bedFieldProvider).
			Description("The model provider family, which determines the request and response encoding. When set to `auto` it is inferred from the `model` ID, but this is not possible when `model` is an inference profile ARN, so it must be set explicitly in that case.").
			Default(bedProviderAuto).
			LintRule(`root = if ![ "auto", "amazon_titan", "cohere" ].contains(this) { [ "provider must be one of: auto, amazon_titan, cohere" ] }`).
			Examples(bedProviderAuto, bedProviderTitan, bedProviderCohere)).
		Field(service.NewStringField(bedFieldInputType).
			Description("The `input_type` sent to Cohere embedding models, which is required by that family. Ignored by other model families.").
			Default("search_document").
			Examples("search_document", "search_query", "classification", "clustering").
			Advanced()).
		Field(service.NewStringField(bedFieldRateLimit).
			Description("An optional [`rate_limit`](/docs/components/rate_limits/about) resource to throttle Bedrock `InvokeModel` requests by. The limit is applied per request, and each Cohere batch (up to 96 texts) counts as one request. Note that Bento rate limits are enforced per running instance, so this does not bound the aggregate rate across horizontally scaled deployments.").
			Default("").
			Advanced())

	for _, f := range config.SessionFields() {
		conf = conf.Field(f)
	}

	conf = conf.Example(
		"Embed documents with Titan",
		"Generate embeddings for a stream of text documents using an Amazon Titan model, keeping the original document and adding the embedding under a `vector` field.",
		`
pipeline:
  processors:
    - branch:
        processors:
          - aws_bedrock_embeddings:
              model: amazon.titan-embed-text-v2:0
              region: us-east-1
        result_map: 'root.vector = this'
`)

	err := service.RegisterBatchProcessor(
		"aws_bedrock_embeddings", conf,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			aconf, err := GetSession(context.TODO(), conf)
			if err != nil {
				return nil, err
			}

			model, err := conf.FieldString(bedFieldModel)
			if err != nil {
				return nil, err
			}

			provider, err := conf.FieldString(bedFieldProvider)
			if err != nil {
				return nil, err
			}

			inputType, err := conf.FieldString(bedFieldInputType)
			if err != nil {
				return nil, err
			}

			rateLimit, err := conf.FieldString(bedFieldRateLimit)
			if err != nil {
				return nil, err
			}

			return newBedrockEmbeddingsProc(bedrockruntime.NewFromConfig(aconf), model, provider, inputType, rateLimit, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type bedrockRuntimeAPI interface {
	InvokeModel(context.Context, *bedrockruntime.InvokeModelInput, ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

// embeddingCodec encodes a group of input texts into a provider-specific
// request body and decodes the resulting embedding vectors, one per input text
// and in the same order, from the provider-specific response body.
type embeddingCodec interface {
	// maxBatchTexts is the largest number of texts the provider accepts in a
	// single InvokeModel call. A value of 1 means the model has no batch API.
	maxBatchTexts() int
	encodeRequest(texts []string) ([]byte, error)
	decodeResponse(body []byte) ([][]float64, error)
}

func codecForModel(model, provider, inputType string) (embeddingCodec, error) {
	switch provider {
	case bedProviderTitan:
		return titanCodec{}, nil
	case bedProviderCohere:
		return cohereCodec{inputType: inputType}, nil
	case bedProviderAuto:
		// Infer the family from the model ID. strings.Contains (rather than a
		// prefix match) also covers cross-region inference profile IDs such as
		// "us.amazon.titan-embed-text-v2:0". Opaque inference profile ARNs carry
		// no model name and cannot be inferred.
		switch {
		case strings.Contains(model, "amazon.titan-embed"):
			return titanCodec{}, nil
		case strings.Contains(model, "cohere.embed"):
			return cohereCodec{inputType: inputType}, nil
		default:
			return nil, fmt.Errorf("could not infer the model family from model '%v' (expected for inference profile ARNs): set the '%v' field to '%v' or '%v'", model, bedFieldProvider, bedProviderTitan, bedProviderCohere)
		}
	default:
		return nil, fmt.Errorf("invalid '%v' value '%v': expected one of %v, %v, %v", bedFieldProvider, provider, bedProviderAuto, bedProviderTitan, bedProviderCohere)
	}
}

//------------------------------------------------------------------------------

// titanCodec handles the Amazon Titan embedding models, which accept a single
// input text per request.
type titanCodec struct{}

func (titanCodec) maxBatchTexts() int { return 1 }

func (titanCodec) encodeRequest(texts []string) ([]byte, error) {
	return json.Marshal(map[string]any{"inputText": texts[0]})
}

func (titanCodec) decodeResponse(body []byte) ([][]float64, error) {
	var resp struct {
		Embedding []float64 `json:"embedding"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to decode Titan embedding response: %w", err)
	}
	if len(resp.Embedding) == 0 {
		return nil, errors.New("titan embedding response contained no embedding")
	}
	return [][]float64{resp.Embedding}, nil
}

//------------------------------------------------------------------------------

// cohereCodec handles the Cohere Embed models, which accept up to 96 input
// texts per request and return an embedding for each.
type cohereCodec struct {
	inputType string
}

// cohereMaxBatchTexts is the maximum number of texts Cohere Embed accepts in a
// single request, per the Bedrock model documentation.
const cohereMaxBatchTexts = 96

func (cohereCodec) maxBatchTexts() int { return cohereMaxBatchTexts }

func (c cohereCodec) encodeRequest(texts []string) ([]byte, error) {
	// embedding_types is required by v4 and accepted by v3; requesting it
	// normalises both onto the {"embeddings": {"float": [...]}} response shape.
	return json.Marshal(map[string]any{
		"texts":           texts,
		"input_type":      c.inputType,
		"embedding_types": []string{"float"},
	})
}

func (cohereCodec) decodeResponse(body []byte) ([][]float64, error) {
	var resp struct {
		Embeddings json.RawMessage `json:"embeddings"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to decode Cohere embedding response: %w", err)
	}

	// v4 (and v3 when embedding_types is set) returns an object keyed by
	// embedding type; older v3 responses return a bare array of vectors.
	var byType struct {
		Float [][]float64 `json:"float"`
	}
	if err := json.Unmarshal(resp.Embeddings, &byType); err == nil && len(byType.Float) > 0 {
		return byType.Float, nil
	}
	var bare [][]float64
	if err := json.Unmarshal(resp.Embeddings, &bare); err == nil && len(bare) > 0 {
		return bare, nil
	}
	return nil, errors.New("cohere embedding response contained no embeddings")
}

//------------------------------------------------------------------------------

type bedrockEmbeddingsProc struct {
	client    bedrockRuntimeAPI
	model     string
	codec     embeddingCodec
	rateLimit string
	log       *service.Logger
	mgr       *service.Resources
}

func newBedrockEmbeddingsProc(client bedrockRuntimeAPI, model, provider, inputType, rateLimit string, mgr *service.Resources) (*bedrockEmbeddingsProc, error) {
	if model == "" {
		return nil, fmt.Errorf("field '%v' must not be empty", bedFieldModel)
	}
	codec, err := codecForModel(model, provider, inputType)
	if err != nil {
		return nil, err
	}
	if rateLimit != "" && !mgr.HasRateLimit(rateLimit) {
		return nil, fmt.Errorf("rate limit resource '%v' was not found", rateLimit)
	}
	return &bedrockEmbeddingsProc{
		client:    client,
		model:     model,
		codec:     codec,
		rateLimit: rateLimit,
		log:       mgr.Logger(),
		mgr:       mgr,
	}, nil
}

// waitForAccess blocks until the configured rate limit grants a request, or
// returns false if ctx is cancelled while waiting. With no rate limit set it
// returns immediately.
func (b *bedrockEmbeddingsProc) waitForAccess(ctx context.Context) bool {
	if b.rateLimit == "" {
		return true
	}
	for {
		var period time.Duration
		var err error
		if rerr := b.mgr.AccessRateLimit(ctx, b.rateLimit, func(rl service.RateLimit) {
			period, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if err != nil {
			b.log.Errorf("Rate limit error: %v", err)
			period = time.Second
		}
		if period <= 0 {
			return true
		}
		select {
		case <-time.After(period):
		case <-ctx.Done():
			return false
		}
	}
}

// invoke sends a single group of texts to Bedrock and returns one embedding per
// text, in the same order.
func (b *bedrockEmbeddingsProc) invoke(ctx context.Context, texts []string) ([][]float64, error) {
	reqBody, err := b.codec.encodeRequest(texts)
	if err != nil {
		return nil, err
	}

	out, err := b.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(b.model),
		Body:        reqBody,
		ContentType: aws.String("application/json"),
		Accept:      aws.String("application/json"),
	})
	if err != nil {
		return nil, err
	}

	embeddings, err := b.codec.decodeResponse(out.Body)
	if err != nil {
		return nil, err
	}
	if len(embeddings) != len(texts) {
		return nil, fmt.Errorf("expected %v embeddings in response but got %v", len(texts), len(embeddings))
	}
	return embeddings, nil
}

func (b *bedrockEmbeddingsProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	batch = batch.Copy()

	// Collect the indices of messages whose contents decode successfully; any
	// that fail are flagged immediately and left out of the Bedrock requests.
	indices := make([]int, 0, len(batch))
	texts := make([]string, 0, len(batch))
	for i, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			msg.SetError(err)
			continue
		}
		indices = append(indices, i)
		texts = append(texts, string(msgBytes))
	}

	chunkSize := b.codec.maxBatchTexts()
	for start := 0; start < len(texts); start += chunkSize {
		end := min(start+chunkSize, len(texts))
		chunkTexts := texts[start:end]
		chunkIndices := indices[start:end]

		if !b.waitForAccess(ctx) {
			for _, idx := range chunkIndices {
				batch[idx].SetError(ctx.Err())
			}
			continue
		}

		embeddings, err := b.invoke(ctx, chunkTexts)
		if err != nil {
			b.log.Errorf("Bedrock embedding request for model '%v' failed: %v", b.model, err)
			for _, idx := range chunkIndices {
				batch[idx].SetError(err)
			}
			continue
		}

		for j, idx := range chunkIndices {
			batch[idx].SetStructuredMut(embeddings[j])
		}
	}

	return []service.MessageBatch{batch}, nil
}

func (b *bedrockEmbeddingsProc) Close(context.Context) error {
	return nil
}
