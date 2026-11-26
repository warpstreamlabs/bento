package gcp

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

type pubsubClient interface {
	Publisher(ctx context.Context, id string, settings *pubsub.PublishSettings) (pubsubPublisher, error)
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

func (ac *airGappedPubsubClient) Publisher(ctx context.Context, id string, settings *pubsub.PublishSettings) (pubsubPublisher, error) {
	name := qualifiedName(ac.c.Project(), "topics", id)
	if _, err := ac.c.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name}); err != nil {
		return nil, err
	}
	p := ac.c.Publisher(name)
	p.PublishSettings = *settings
	return &airGappedPublisher{p: p}, nil
}

type airGappedPublisher struct {
	p *pubsub.Publisher
}

func (at *airGappedPublisher) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	return at.p.Publish(ctx, msg)
}

func (at *airGappedPublisher) EnableOrdering() {
	at.p.EnableMessageOrdering = true
}

func (at *airGappedPublisher) Stop() {
	at.p.Stop()
}
