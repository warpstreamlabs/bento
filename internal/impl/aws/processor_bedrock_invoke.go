package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	bedFieldModel     = "model"
	bedFieldRateLimit = "rate_limit"
)

func bedrockInvokeProcSpec() *service.ConfigSpec {
	conf := service.NewConfigSpec().
		Beta().
		Categories("AI", "Integration").
		Summary("Invokes an Amazon Bedrock model for each message, replacing the message contents with the raw model response.").
		Description(`Sends the contents of each message to the [Amazon Bedrock](https://docs.aws.amazon.com/bedrock/latest/userguide/what-is-bedrock.html) ` + "`InvokeModel`" + ` API and replaces the message with the response body.

Message payloads are forwarded verbatim as the request body and are neither parsed nor validated by this processor, and responses are written back unmodified. The request and response formats differ between model providers, so it is up to you to construct a body the target model accepts and to extract whatever you need from the reply — typically with a [` + "`mapping`" + `](/docs/components/processors/mapping) processor either side. Both the request and response content types are set to ` + "`application/json`" + `.

Requests are signed with AWS Signature Version 4 by the AWS SDK, so no additional authentication configuration is required beyond standard AWS credentials.

The ` + "`model`" + ` may be a foundation model ID (e.g. ` + "`amazon.titan-embed-text-v2:0`" + `) or an [inference profile](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles.html) ID or ARN. Some models can only be invoked through an inference profile.

Each message in a batch is invoked separately and concurrently, and messages larger than one are invoked in parallel. A failed invocation is flagged on the individual message that caused it, leaving the rest of the batch untouched. Note that this means a batch of N messages results in N Bedrock requests; models that accept multiple inputs per request are not batched into a single call.

Streaming APIs (` + "`InvokeModelWithResponseStream`" + `, ` + "`Converse`" + `, ` + "`ConverseStream`" + `) are not supported.

### Preserving the original message

By default the message contents are replaced with the model response. To keep the original payload and store the response alongside it, use a [` + "`branch`" + `](/docs/components/processors/branch) processor.

### Credentials

By default Bento will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level. You can find out more [in this document](/docs/guides/cloud/aws).`).
		Field(service.NewStringField(bedFieldModel).
			Description("The ID of the Bedrock model to invoke, or an inference profile ID or ARN.").
			Examples(
				"amazon.titan-embed-text-v2:0",
				"cohere.embed-english-v3",
				"anthropic.claude-3-5-sonnet-20241022-v2:0",
				"us.anthropic.claude-3-5-sonnet-20241022-v2:0",
			)).
		Field(service.NewStringField(bedFieldRateLimit).
			Description("An optional [`rate_limit`](/docs/components/rate_limits/about) resource to throttle Bedrock `InvokeModel` requests by. The limit is applied per request, and each message in a batch counts as one request. Note that Bento rate limits are enforced per running instance, so this does not bound the aggregate rate across horizontally scaled deployments.").
			Default("").
			Advanced())

	for _, f := range config.SessionFields() {
		conf = conf.Field(f)
	}

	conf = conf.Example(
		"Embed documents with Titan",
		"Generate an embedding for each message using an Amazon Titan model, keeping the original document and adding the embedding under a `vector` field. The `request_map` builds the body Titan expects, and the `result_map` pulls the vector out of the response.",
		`
pipeline:
  processors:
    - branch:
        request_map: 'root.inputText = content().string()'
        processors:
          - aws_bedrock_invoke:
              model: amazon.titan-embed-text-v2:0
              region: us-east-1
        result_map: 'root.vector = this.embedding'
`)

	conf = conf.Example(
		"Prompt Claude",
		"Send each message as a prompt to an Anthropic model and replace the message with the generated text.",
		`
pipeline:
  processors:
    - mapping: |
        root.anthropic_version = "bedrock-2023-05-31"
        root.max_tokens = 1024
        root.messages = [
          {"role": "user", "content": content().string()}
        ]
    - aws_bedrock_invoke:
        model: anthropic.claude-3-5-sonnet-20241022-v2:0
        region: us-east-1
    - mapping: 'root = this.content.0.text'
`)

	return conf
}
func bedrockProcessorFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*bedrockInvokeProc, error) {
	aconf, err := GetSession(context.TODO(), conf)
	if err != nil {
		return nil, err
	}

	model, err := conf.FieldString(bedFieldModel)
	if err != nil {
		return nil, err
	}

	rateLimit, err := conf.FieldString(bedFieldRateLimit)
	if err != nil {
		return nil, err
	}

	return newbedrockInvokeProc(bedrockruntime.NewFromConfig(aconf), model, rateLimit, mgr)
}

func init() {
	err := service.RegisterBatchProcessor(
		"aws_bedrock_invoke", bedrockInvokeProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return bedrockProcessorFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type bedrockRuntimeAPI interface {
	InvokeModel(context.Context, *bedrockruntime.InvokeModelInput, ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

type bedrockInvokeProc struct {
	client    *bedrockClient
	model     string
	rateLimit string
	log       *service.Logger
	mgr       *service.Resources

	shutSig *shutdown.Signaller
}

func newbedrockInvokeProc(bedrock bedrockRuntimeAPI, model, rateLimit string, mgr *service.Resources) (*bedrockInvokeProc, error) {
	if model == "" {
		return nil, fmt.Errorf("field '%v' must not be empty", bedFieldModel)
	}

	if rateLimit != "" && !mgr.HasRateLimit(rateLimit) {
		return nil, fmt.Errorf("rate limit resource '%v' was not found", rateLimit)
	}

	client, err := newBedrockClient(bedrock, model, rateLimit, mgr)
	if err != nil {
		return nil, err
	}

	return &bedrockInvokeProc{
		client:    client,
		model:     model,
		rateLimit: rateLimit,
		log:       mgr.Logger(),
		mgr:       mgr,
		shutSig:   shutdown.NewSignaller(),
	}, nil
}

func (b *bedrockInvokeProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	ctx, cancel := b.shutSig.SoftStopCtx(ctx)
	defer cancel()

	if len(batch) == 1 {
		for _, p := range batch {
			if err := b.client.Invoke(ctx, p); err != nil {
				p.SetError(err)
			}
		}
	} else {
		var wg sync.WaitGroup

		for i := range batch {
			wg.Go(func() {
				err := b.client.Invoke(ctx, batch[i])
				if err != nil {
					batch[i].SetError(err)
				}
			})
		}

		wg.Wait()
	}

	return []service.MessageBatch{batch}, nil
}

func (b *bedrockInvokeProc) Close(ctx context.Context) error {
	b.shutSig.TriggerHardStop()
	return nil
}

//------------------------------------------------------------------------------

type bedrockClient struct {
	bedrock bedrockRuntimeAPI

	log *service.Logger
	mgr *service.Resources

	model     string
	rateLimit string
}

func newBedrockClient(
	bedrock bedrockRuntimeAPI,
	model string,
	rateLimit string,
	mgr *service.Resources,
) (*bedrockClient, error) {
	b := bedrockClient{
		bedrock:   bedrock,
		log:       mgr.Logger(),
		mgr:       mgr,
		model:     model,
		rateLimit: rateLimit,
	}
	if model == "" {
		return nil, errors.New("bedrock model identifier must not be empty")
	}

	if rateLimit != "" {
		if !b.mgr.HasRateLimit(rateLimit) {
			return nil, fmt.Errorf("rate limit resource '%v' was not found", rateLimit)
		}
	}

	return &b, nil
}

//------------------------------------------------------------------------------

// waitForAccess blocks until the configured rate limit grants a request, or
// returns false if ctx is cancelled while waiting. With no rate limit set it
// returns immediately.
func (b *bedrockClient) waitForAccess(ctx context.Context) bool {
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

func (b *bedrockClient) Invoke(ctx context.Context, msg *service.Message) error {
	if !b.waitForAccess(ctx) {
		return ctx.Err()
	}

	reqBody, err := msg.AsBytes()
	if err != nil {
		return err
	}

	out, err := b.bedrock.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(b.model),
		Body:        reqBody,
		ContentType: aws.String("application/json"),
		Accept:      aws.String("application/json"),
	})
	if err != nil {
		return err
	}

	msg.SetBytes(out.Body)

	return nil
}
