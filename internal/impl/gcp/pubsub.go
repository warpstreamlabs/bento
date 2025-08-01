package gcp

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pubsubClient interface {
	Exists(ctx context.Context, id string) (bool, error)
	Publisher(id string, settings *pubsub.PublishSettings) pubsubPublisher
}

type pubsubPublisher interface {
	Publish(ctx context.Context, msg *pubsub.Message) publishResult
	EnableOrdering()
	Stop()
}

type publishResult interface {
	Get(ctx context.Context) (serverID string, err error)
}

type airGappedPubsubClient struct {
	c *pubsub.Client
}

func (ac *airGappedPubsubClient) Exists(ctx context.Context, id string) (bool, error) {
	_, err := ac.c.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{
		Topic: id,
	})

	if err == nil {
		return true, nil
	}

	st, ok := status.FromError(err)
	if ok && st.Code() == codes.NotFound {
		return false, nil
	}

	return false, err
}

func (ac *airGappedPubsubClient) Publisher(id string, settings *pubsub.PublishSettings) pubsubPublisher {
	t := ac.c.Publisher(id)
	t.PublishSettings = *settings

	return &airGappedTopic{t: t}
}

type airGappedTopic struct {
	t *pubsub.Publisher
}

func (at *airGappedTopic) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	return at.t.Publish(ctx, msg)
}

func (at *airGappedTopic) EnableOrdering() {
	at.t.EnableMessageOrdering = true
}

func (at *airGappedTopic) Stop() {
	at.t.Stop()
}
