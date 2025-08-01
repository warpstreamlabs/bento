package gcp

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
	"github.com/stretchr/testify/mock"
)

type mockPubSubClient struct {
	mock.Mock
}

var _ pubsubClient = &mockPubSubClient{}

func (c *mockPubSubClient) Publisher(id string, settings *pubsub.PublishSettings) pubsubPublisher {
	args := c.Called(id)

	return args.Get(0).(pubsubPublisher)
}

func (mt *mockPubSubClient) Exists(context.Context, string) (bool, error) {
	args := mt.Called()
	return args.Bool(0), args.Error(1)
}

type mockPublisher struct {
	mock.Mock
}

var _ pubsubPublisher = &mockPublisher{}

func (mt *mockPublisher) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	args := mt.Called(string(msg.Data), msg)

	return args.Get(0).(publishResult)
}

func (mt *mockPublisher) EnableOrdering() {
	mt.Called()
}

func (mt *mockPublisher) Stop() {
	mt.Called()
}

type mockPublishResult struct {
	mock.Mock
}

var _ publishResult = &mockPublishResult{}

func (m *mockPublishResult) Get(ctx context.Context) (string, error) {
	args := m.Called()

	return args.String(0), args.Error(1)
}
