package azure

import (
	"context"
	"errors"
	"testing"

	azq "github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeQueueClient struct {
	deleted   []string
	deleteErr error
}

func (f *fakeQueueClient) DeleteMessage(_ context.Context, messageID, _ string, _ *azq.DeleteMessageOptions) (azq.DeleteMessageResponse, error) {
	f.deleted = append(f.deleted, messageID)
	return azq.DeleteMessageResponse{}, f.deleteErr
}

func testDequeuedMessages() []*azq.DequeuedMessage {
	newMsg := func(id, receipt, text string) *azq.DequeuedMessage {
		return &azq.DequeuedMessage{MessageID: &id, PopReceipt: &receipt, MessageText: &text}
	}
	return []*azq.DequeuedMessage{
		newMsg("id1", "receipt1", "hello"),
		newMsg("id2", "receipt2", "world"),
	}
}

func TestQueueAckFnAckDeletesMessages(t *testing.T) {
	client := &fakeQueueClient{}

	require.NoError(t, queueAckFn(client, testDequeuedMessages(), true)(t.Context(), nil))

	assert.Equal(t, []string{"id1", "id2"}, client.deleted)
}

func TestQueueAckFnAckDeleteError(t *testing.T) {
	client := &fakeQueueClient{deleteErr: errors.New("boom")}

	require.ErrorContains(t, queueAckFn(client, testDequeuedMessages(), true)(t.Context(), nil), "boom")
}

func TestQueueAckFnNackKeepsMessages(t *testing.T) {
	client := &fakeQueueClient{}

	require.NoError(t, queueAckFn(client, testDequeuedMessages(), true)(t.Context(), errors.New("simulated failure")))

	assert.Empty(t, client.deleted)
}

func TestQueueAckFnDeleteMessageDisabled(t *testing.T) {
	client := &fakeQueueClient{}

	require.NoError(t, queueAckFn(client, testDequeuedMessages(), false)(t.Context(), nil))

	assert.Empty(t, client.deleted)
}
