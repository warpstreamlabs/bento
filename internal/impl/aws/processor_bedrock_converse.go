package aws

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	bedrocktypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	bedcpFieldModel        = "model"
	bedcpFieldUserPrompt   = "prompt"
	bedcpFieldSystemPrompt = "system_prompt"
	bedcpFieldMaxTokens    = "max_tokens"
	bedcpFieldStop         = "stop"
	bedcpFieldTemp         = "temperature"
	bedcpFieldTopP         = "top_p"
)

func bedrockChatProcSpec() *service.ConfigSpec {
	conf := service.NewConfigSpec().
		Categories("AI", "Integration").
		Summary("Generates responses to messages in a chat conversation, using the AWS Bedrock Converse API.").
		Description(`This processor sends prompts to your chosen large language model (LLM) and generates text from the responses, using the [AWS Bedrock Converse API](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html).

Unlike the ` + "`aws_bedrock_invoke`" + ` processor which uses the model-specific ` + "`InvokeModel`" + ` API, this processor uses the model-agnostic ` + "`Converse`" + ` API, which provides a unified request/response format across all supported models.

For more information, see the [AWS Bedrock documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/what-is-bedrock.html).`).
		Field(service.NewStringField(bedcpFieldModel).
			Description("The model ID to use. For a full list see the [AWS Bedrock documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/model-ids.html).").
			Examples("amazon.titan-text-express-v1", "anthropic.claude-3-5-sonnet-20241022-v2:0", "cohere.command-text-v14", "meta.llama3-1-70b-instruct-v1:0", "mistral.mistral-large-2402-v1:0")).
		Field(service.NewStringField(bedcpFieldUserPrompt).
			Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
			Optional()).
		Field(service.NewStringField(bedcpFieldSystemPrompt).
			Optional().
			Description("The system prompt to submit to the AWS Bedrock LLM.")).
		Field(service.NewIntField(bedcpFieldMaxTokens).
			Optional().
			Description("The maximum number of tokens to allow in the generated response.").
			LintRule(`root = if this < 1 { ["field must be greater than or equal to 1"] }`)).
		Field(service.NewFloatField(bedcpFieldTemp).
			Optional().
			Description("The likelihood of the model selecting higher-probability options while generating a response. A lower value makes the model more likely to choose higher-probability options, while a higher value makes the model more likely to choose lower-probability options.").
			LintRule(`root = if this < 0 || this > 1 { ["field must be between 0.0-1.0"] }`)).
		Field(service.NewStringListField(bedcpFieldStop).
			Optional().
			Advanced().
			Description("A list of stop sequences. A stop sequence is a sequence of characters that causes the model to stop generating the response.")).
		Field(service.NewFloatField(bedcpFieldTopP).
			Optional().
			Advanced().
			Description("The percentage of most-likely candidates that the model considers for the next token. For example, if you choose a value of 0.8, the model selects from the top 80% of the probability distribution of tokens that could be next in the sequence.").
			LintRule(`root = if this < 0 || this > 1 { ["field must be between 0.0-1.0"] }`))

	for _, f := range config.SessionFields() {
		conf = conf.Field(f)
	}

	conf = conf.Example(
		"Chat with Claude",
		"Send each message as a prompt to an Anthropic model and replace the message with the generated text using the Converse API.",
		`
pipeline:
  processors:
    - aws_bedrock_chat:
        model: anthropic.claude-3-5-sonnet-20241022-v2:0
        system_prompt: You are a helpful assistant.
        max_tokens: 1024
        region: us-east-1
`)

	return conf
}

func bedrockChatProcessorFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*bedrockChatProcessor, error) {
	aconf, err := GetSession(context.TODO(), conf)
	if err != nil {
		return nil, err
	}
	client := bedrockruntime.NewFromConfig(aconf)
	model, err := conf.FieldString(bedcpFieldModel)
	if err != nil {
		return nil, err
	}
	p := &bedrockChatProcessor{
		client: client,
		model:  model,
	}
	if conf.Contains(bedcpFieldUserPrompt) {
		pf, err := conf.FieldInterpolatedString(bedcpFieldUserPrompt)
		if err != nil {
			return nil, err
		}
		p.userPrompt = pf
	}
	if conf.Contains(bedcpFieldSystemPrompt) {
		pf, err := conf.FieldInterpolatedString(bedcpFieldSystemPrompt)
		if err != nil {
			return nil, err
		}
		p.systemPrompt = pf
	}
	if conf.Contains(bedcpFieldMaxTokens) {
		v, err := conf.FieldInt(bedcpFieldMaxTokens)
		if err != nil {
			return nil, err
		}
		mt := int32(v)
		p.maxTokens = &mt
	}
	if conf.Contains(bedcpFieldTemp) {
		v, err := conf.FieldFloat(bedcpFieldTemp)
		if err != nil {
			return nil, err
		}
		t := float32(v)
		p.temp = &t
	}
	if conf.Contains(bedcpFieldStop) {
		stop, err := conf.FieldStringList(bedcpFieldStop)
		if err != nil {
			return nil, err
		}
		p.stop = stop
	}
	if conf.Contains(bedcpFieldTopP) {
		v, err := conf.FieldFloat(bedcpFieldTopP)
		if err != nil {
			return nil, err
		}
		tp := float32(v)
		p.topP = &tp
	}
	return p, nil
}

func init() {
	err := service.RegisterProcessor("aws_bedrock_chat", bedrockChatProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return bedrockChatProcessorFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type bedrockChatProcessor struct {
	client *bedrockruntime.Client
	model  string

	userPrompt   *service.InterpolatedString
	systemPrompt *service.InterpolatedString
	maxTokens    *int32
	stop         []string
	temp         *float32
	topP         *float32
}

func (b *bedrockChatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	prompt, err := b.computePrompt(msg)
	if err != nil {
		return nil, err
	}
	input := &bedrockruntime.ConverseInput{
		Messages: []bedrocktypes.Message{
			{
				Role: bedrocktypes.ConversationRoleUser,
				Content: []bedrocktypes.ContentBlock{
					&bedrocktypes.ContentBlockMemberText{
						Value: prompt,
					},
				},
			},
		},
		ModelId: &b.model,
		InferenceConfig: &bedrocktypes.InferenceConfiguration{
			MaxTokens:     b.maxTokens,
			StopSequences: b.stop,
			Temperature:   b.temp,
			TopP:          b.topP,
		},
	}
	if b.systemPrompt != nil {
		prompt, err := b.systemPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("unable to interpolate `%s`: %w", bedcpFieldSystemPrompt, err)
		}
		input.System = []bedrocktypes.SystemContentBlock{
			&bedrocktypes.SystemContentBlockMemberText{Value: prompt},
		}
	}
	resp, err := b.client.Converse(ctx, input)
	if err != nil {
		return nil, err
	}
	respOut, ok := resp.Output.(*bedrocktypes.ConverseOutputMemberMessage)
	if !ok {
		return nil, fmt.Errorf("unexpected output: %T", resp)
	}
	content := respOut.Value.Content
	if len(content) != 1 {
		return nil, fmt.Errorf("unexpected number of response content: %d", len(content))
	}
	out := msg.Copy()
	switch c := content[0].(type) {
	case *bedrocktypes.ContentBlockMemberText:
		out.SetStructured(c.Value)
	default:
		return nil, fmt.Errorf("unsupported response content type: %T", content[0])
	}
	return service.MessageBatch{out}, nil
}

func (b *bedrockChatProcessor) computePrompt(msg *service.Message) (string, error) {
	if b.userPrompt != nil {
		return b.userPrompt.TryString(msg)
	}
	buf, err := msg.AsBytes()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(buf) {
		return "", errors.New("message payload contained invalid UTF8")
	}
	return string(buf), nil
}

func (*bedrockChatProcessor) Close(context.Context) error {
	return nil
}
