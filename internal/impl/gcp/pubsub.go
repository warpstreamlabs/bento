package gcp

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
)

type pubsubClient interface {
	Publisher(id string, settings *pubsub.PublishSettings) (pubsubPublisher, error)
}

type pubsubPublisher interface {
	//Exists(ctx context.Context) (bool, error)
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

func (ac *airGappedPubsubClient) Publisher(id string, settings *pubsub.PublishSettings) (pubsubPublisher, error) {
	t := ac.c.Publisher(id)

	t.PublishSettings = *settings

	return &airGappedPublisher{t: t}, nil
}

type airGappedPublisher struct {
	t *pubsub.Publisher
}

// func (at *airGappedTopic) Exists(ctx context.Context) (bool, error) {
// 	return at.t.Exists(ctx)
// }

func (at *airGappedPublisher) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	return at.t.Publish(ctx, msg)
}

func (at *airGappedPublisher) EnableOrdering() {
	at.t.EnableMessageOrdering = true
}

func (at *airGappedPublisher) Stop() {
	at.t.Stop()
}
