package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type customHeaderSQSClient struct {
	sqsAPI
	headers map[string]string
}

// newCustomHeaderSQSClient creates a new client that wraps the provided SQS API
func newCustomHeaderSQSClient(api sqsAPI, headers map[string]string) *customHeaderSQSClient {
	return &customHeaderSQSClient{
		sqsAPI:  api,
		headers: headers,
	}
}

// getAPIOptions returns API options with all custom headers added
func (c *customHeaderSQSClient) getAPIOptions() []func(*sqs.Options) {
	return []func(*sqs.Options){
		func(options *sqs.Options) {
			for key, value := range c.headers {
				options.APIOptions = append(
					options.APIOptions,
					smithyhttp.SetHeaderValue(key, value),
				)
			}
		},
	}
}

// ReceiveMessage wraps the standard ReceiveMessage call and adds custom headers
func (c *customHeaderSQSClient) ReceiveMessage(
	ctx context.Context,
	params *sqs.ReceiveMessageInput,
	optFns ...func(*sqs.Options),
) (*sqs.ReceiveMessageOutput, error) {
	allOptFns := append(c.getAPIOptions(), optFns...)
	return c.sqsAPI.ReceiveMessage(ctx, params, allOptFns...)
}

// DeleteMessageBatch wraps the standard DeleteMessageBatch call and adds custom headers
func (c *customHeaderSQSClient) DeleteMessageBatch(
	ctx context.Context,
	params *sqs.DeleteMessageBatchInput,
	optFns ...func(*sqs.Options),
) (*sqs.DeleteMessageBatchOutput, error) {
	allOptFns := append(c.getAPIOptions(), optFns...)
	return c.sqsAPI.DeleteMessageBatch(ctx, params, allOptFns...)
}

// ChangeMessageVisibilityBatch wraps the standard ChangeMessageVisibilityBatch call and adds custom headers
func (c *customHeaderSQSClient) ChangeMessageVisibilityBatch(
	ctx context.Context,
	params *sqs.ChangeMessageVisibilityBatchInput,
	optFns ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	allOptFns := append(c.getAPIOptions(), optFns...)
	return c.sqsAPI.ChangeMessageVisibilityBatch(ctx, params, allOptFns...)
}

// SendMessageBatch wraps the standard SendMessageBatch call and adds custom headers
func (c *customHeaderSQSClient) SendMessageBatch(
	ctx context.Context,
	params *sqs.SendMessageBatchInput,
	optFns ...func(*sqs.Options),
) (*sqs.SendMessageBatchOutput, error) {
	allOptFns := append(c.getAPIOptions(), optFns...)
	return c.sqsAPI.SendMessageBatch(ctx, params, allOptFns...)
}
