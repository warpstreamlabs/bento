package slack

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/Jeffail/shutdown"
	"github.com/slack-go/slack"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	slackWebhookURLField     = "webhook"
	slackWebhookTimeoutField = "timeout"
	slackWebhookTLSField     = "tls"
)

func init() {
	err := service.RegisterOutput(
		"slack_webhook", slackWebhookOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newWriter(conf, mgr)
			return w, 1, err
		},
	)
	if err != nil {
		panic(err)
	}
}

func slackWebhookOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services", "Social").
		Summary("Post messages to Slack via webhook.").
		Description(`
		This output POSTs messages to a Slack channel via webhook.
		The format of a message should be a JSON object should match the [Golang Slack WebhookMessage struct](https://github.com/slack-go/slack/blob/v0.17.3/webhooks.go#L12) type`).
		Fields(
			service.NewURLField(slackWebhookURLField).
				Description("Slack webhook URL to post messages").
				Example("https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
			service.NewDurationField(slackWebhookTimeoutField).
				Description("The maximum time to wait before abandoning a request (and trying again).").
				Advanced().
				Default("5s"),
			service.NewTLSToggledField(slackWebhookTLSField),
		)
}

type writer struct {
	log *service.Logger

	// The actual webhook URL where requests will be sent.
	webhook string

	httpClient *http.Client

	mu      sync.RWMutex
	shutSig *shutdown.Signaller
}

func newWriter(conf *service.ParsedConfig, mgr *service.Resources) (*writer, error) {
	w := &writer{
		log: mgr.Logger(),
	}

	timeout, err := conf.FieldDuration(slackWebhookTimeoutField)
	if err != nil {
		return nil, err
	}

	w.httpClient = &http.Client{
		Timeout: timeout,
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled(slackWebhookTLSField)
	if err != nil {
		return nil, err
	}

	if tlsEnabled {
		w.httpClient.Transport = &http.Transport{TLSClientConfig: tlsConf}
	}

	w.webhook, err = conf.FieldString(slackWebhookURLField)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (w *writer) Connect(ctx context.Context) error {
	w.log.Debugf("Writing Slack messages with webhook: %s", w.webhook)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.shutSig == nil {
		w.shutSig = shutdown.NewSignaller()
		return nil
	}

	if w.shutSig.IsHardStopSignalled() {
		// allow the shutdown signaller to be re-created if we try connect after a close
		w.shutSig = shutdown.NewSignaller()
		return nil
	}

	return nil
}

func (w *writer) Write(ctx context.Context, msg *service.Message) error {
	w.mu.RLock()
	if w.shutSig == nil || w.shutSig.IsHardStopSignalled() {
		w.mu.RUnlock()
		return service.ErrNotConnected
	}
	ctx, cancel := w.shutSig.SoftStopCtx(ctx)
	defer cancel()
	w.mu.RUnlock()

	rawContent, err := msg.AsBytes()
	if err != nil {
		return err
	}

	var slackMsg slack.WebhookMessage
	if err := json.Unmarshal(rawContent, &slackMsg); err != nil {
		w.log.Errorf("Failed to parse the object for Slack schema '%v': %v", string(rawContent), err)
		return err
	}

	// Post the message to the Slack channel using the webhook URL
	err = slack.PostWebhookCustomHTTPContext(ctx, w.webhook, w.httpClient, &slackMsg)
	if err != nil {
		w.log.Errorf("Failed to post message to Slack: %v", err)
		return err
	}
	w.log.Debugf("Message sent to Slack: %v", string(rawContent))
	return nil
}

func (w *writer) Close(ctx context.Context) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.shutSig == nil {
		return nil
	}

	w.shutSig.TriggerHardStop()
	return nil
}
