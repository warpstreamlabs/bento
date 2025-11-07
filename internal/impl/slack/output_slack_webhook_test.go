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
		rw.Write(response)
	})

	go svr.Start()
	defer svr.Stop()

	serverAddress, _ := url.JoinPath(svr.GetAPIURL(), "webhook")
	conf := fmt.Sprintf(`
webhook: %s
`[1:], serverAddress)

	pconf, err := ConfigSpec().ParseYAML(conf, nil)
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
