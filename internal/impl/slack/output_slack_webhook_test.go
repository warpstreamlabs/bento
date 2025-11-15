package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slacktest"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

func setupTestServer(t *testing.T) (*slacktest.Server, func()) {
	t.Helper()

	svr := slacktest.NewTestServer()

	var receivedPayload slack.WebhookMessage

	svr.Handle("/webhook", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")

		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&receivedPayload)
		if err != nil {
			t.Errorf("Request contained invalid JSON, %s", err)
		}

		response := []byte(`{}`)
		_, _ = rw.Write(response)
	})

	go svr.Start()
	return svr, svr.Stop
}

func TestSlackWebhookLifecycle(t *testing.T) {
	svr, cancel := setupTestServer(t)
	defer cancel()

	serverAddress, _ := url.JoinPath(svr.GetAPIURL(), "webhook")
	conf := fmt.Sprintf("webhook: %s", serverAddress)

	pconf, err := slackWebhookOutputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)

	slackOutput, err := newWriter(pconf, service.MockResources())
	require.NoError(t, err)

	msg := service.NewMessage([]byte(`{}`))
	require.Error(t, slackOutput.Write(context.TODO(), msg))

	// Closing before connecting should be a no-op
	require.NoError(t, slackOutput.Close(context.TODO()))

	// Ensure Connect is idempotent
	require.NoError(t, slackOutput.Connect(context.TODO()))
	require.NoError(t, slackOutput.Connect(context.TODO()))

	// Writing after close should error
	require.NoError(t, slackOutput.Connect(context.TODO()))
	require.NoError(t, slackOutput.Close(context.TODO()))
	require.Error(t, slackOutput.Write(context.TODO(), msg))

	// Ensure we can reconnect after Close
	require.NoError(t, slackOutput.Connect(context.TODO()))

	// Ensure happy path is fine: Connect -> Write -> Close
	require.NoError(t, slackOutput.Connect(context.TODO()))
	require.NoError(t, slackOutput.Write(context.TODO(), msg))
	require.NoError(t, slackOutput.Close(context.TODO()))

	// Ensure Close is idempotent
	require.NoError(t, slackOutput.Close(context.TODO()))

}

func TestSlackWebhook(t *testing.T) {
	svr := slacktest.NewTestServer()

	var receivedPayload slack.WebhookMessage

	svr.Handle("/webhook", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")

		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&receivedPayload)
		if err != nil {
			t.Errorf("Request contained invalid JSON, %s", err)
		}

		response := []byte(`{}`)
		_, _ = rw.Write(response)
	})

	go svr.Start()
	defer svr.Stop()

	serverAddress, _ := url.JoinPath(svr.GetAPIURL(), "webhook")
	conf := fmt.Sprintf(`
webhook: %s
`[1:], serverAddress)

	pconf, err := slackWebhookOutputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)

	slackOutput, err := newWriter(pconf, service.MockResources())
	require.NoError(t, err)

	err = slackOutput.Connect(context.Background())
	require.NoError(t, err)

	channelID := "channel-" + uuid.NewString()
	text := "We're no strangers to love."
	payload := fmt.Sprintf(`{"channel":"%s","text":"%s"}`, channelID, text)
	msg := service.NewMessage([]byte(payload))

	err = slackOutput.Write(context.Background(), msg)
	require.NoError(t, err)

	require.Equal(t, text, receivedPayload.Text)
	require.Equal(t, channelID, receivedPayload.Channel)
}
