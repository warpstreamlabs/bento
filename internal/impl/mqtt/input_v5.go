package mqtt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	miv5FieldTopics             = "topics"
	miv5FieldQoS                = "qos"
	miv5FieldNoLocal            = "no_local"
	miv5FieldRetainAsPublished  = "retain_as_published"
	miv5FieldRetainHandling     = "retain_handling"
	miv5FieldAckInterval        = "ack_interval"
	miv5FieldOnSubscribeRefused = "on_subscribe_refused"
	miv5FieldOnConnectRefused   = "on_connect_refused"
)

// mv5SubackReasons names the reason codes a server can refuse a subscription
// with. The client library reports more than one refused filter as "at least
// one requested subscription failed" and a single one as a sentence ending in a
// reason string most servers never send, so the codes have to be read off the
// SUBACK itself.
var mv5SubackReasons = map[byte]string{
	0x80: "unspecified error",
	0x83: "implementation specific error",
	0x87: "not authorized",
	0x8F: "topic filter invalid",
	0x91: "packet identifier in use",
	0x97: "quota exceeded",
	0x9E: "shared subscriptions not supported",
	0xA1: "subscription identifiers not supported",
	0xA2: "wildcard subscriptions not supported",
}

func mv5SubackReason(code byte) string {
	if name, ok := mv5SubackReasons[code]; ok {
		return name
	}
	return "unrecognised reason code"
}

func inputConfigSpecV5() *service.ConfigSpec {
	// Deliberately not marked Stable: components resting on a v0 dependency are
	// documented as experimental, which is what a plugin spec defaults to.
	return service.NewConfigSpec().
		Categories("Services").
		Summary("Subscribe to topics on MQTT 5 brokers.").
		Description(`
This input speaks MQTT 5 only, and will not fall back to 3.1.1. Use the `+"`mqtt`"+` input for servers that speak the older protocol.

### Durable subscriptions

Receiving messages published while the pipeline was down needs three settings together: `+"`clean_start: false`"+`, a fixed `+"`client_id`"+`, and a `+"`session_expiry_interval`"+` longer than the outage you want to survive. Any one of them left at its default loses the messages.

### Shared subscriptions

A shared subscription is an ordinary topic filter of the form `+"`$share/<group>/<filter>`"+`, so it needs no setting of its own. Scaling out means running more instances of the same configuration with the same group.

### Acknowledgement

Messages are acknowledged to the server only once the pipeline has finished with them, so a message is not lost if a destination is unavailable.

Acknowledgements are released strictly in the order the messages arrived, because that is the only order MQTT 5 allows them to be sent in. A message still being worked on holds back the acknowledgement of everything received after it, even messages the pipeline has already finished with.

**A message that never succeeds therefore stops consumption altogether.** It is never acknowledged, so nothing behind it is acknowledged either; once `+"`receive_maximum`"+` messages are outstanding — or the server's own limit, if that field is unset — the server stops sending and this input receives nothing further. Nothing is lost, and the whole run is redelivered on the next connection, but the pipeline goes on reporting itself healthy while consuming zero messages.

Two warnings exist to stop that being silent. One is logged the first time a message is rejected. The other is logged once when this input has held messages for a minute without finishing any of them, which is the only signal available under the default `+"`auto_replay_nacks`"+`: a message being retried inside Bento for ever never reports an error to this component at all, so there is nothing to react to except the absence of progress.

A pipeline that simply takes longer than a minute to finish each message reaches that second condition too, and for it the warning is expected rather than a fault. It is logged once either way, and again only after the input has caught up and stalled afresh.

**So do not reject messages you cannot ever process.** Send them somewhere instead — a [`+"`fallback`"+`](/docs/components/outputs/fallback) output to a dead-letter destination, so every message is eventually acknowledged and the stream keeps moving:

`+"```yaml"+`
output:
  fallback:
    - kafka_franz: { } # the real destination
    - file:
        path: ./dead-letter.jsonl
`+"```"+`

Pipelines that need messages processed in order should also set `+"`pipeline.threads: 1`"+`.

### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- mqtt_topic
- mqtt_qos
- mqtt_retained
- mqtt_duplicate
- mqtt_message_id
- mqtt_content_type (if set)
- mqtt_response_topic (if set)
- mqtt_correlation_data (if set)
- mqtt_message_expiry_interval (if set)
- mqtt_payload_format_indicator (if set)
- mqtt_subscription_identifier (if set)
`+"```"+`

MQTT 5 user properties are also added, each under its own key with no prefix — this is the part MQTT 3.1.1 cannot carry at all. They are written before the fields above, so a publisher cannot overwrite `+"`mqtt_topic`"+` or any other of them by sending a user property of the same name. Where a key appears more than once in one message, which MQTT 5 permits, the last occurrence wins.

The `+"`mqtt_`"+` fields describe the delivery this input received rather than the message itself, so the [`+"`mqtt_v5`"+` output](/docs/components/outputs/mqtt_v5) holds them back by default: publishing to another MQTT server sends the publisher's own user properties and not this input's bookkeeping. Everything else — the user properties above, and any metadata a pipeline adds — is sent. That default is a setting on the output rather than a rule here, so a pipeline that wants the bookkeeping forwarded can say so.

Outputs for other destinations have no such default, so a `+"`kafka_franz`"+` or `+"`aws_s3`"+` output receives these fields like any other metadata and can carry them as headers or object metadata.

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(clientFieldsV5()...).
		Fields(
			service.NewStringListField(miv5FieldTopics).
				Description("A list of topic filters to consume from. A filter of the form `$share/<group>/<filter>` is a shared subscription, splitting the stream across every instance using the same group."),
			service.NewIntField(miv5FieldQoS).
				Description("The level of delivery guarantee to enforce. Has options 0, 1, 2. At QoS 0 the server neither redelivers nor waits for an acknowledgement, so a message lost in transit stays lost.").
				Advanced().
				Default(1),
			service.NewBoolField(miv5FieldNoLocal).
				Description("Whether to ask the server not to deliver back messages this same client published. Applied to every filter.").
				Advanced().
				Default(false),
			service.NewBoolField(miv5FieldRetainAsPublished).
				Description("Whether messages forwarded by the server keep the retained flag they were published with. Applied to every filter.").
				Advanced().
				Default(false),
			service.NewIntField(miv5FieldRetainHandling).
				Description("Whether the server should send retained messages when this subscription is made: `0` always, `1` only if the subscription is new, `2` never. Applied to every filter.").
				Advanced().
				Default(0),
			service.NewDurationField(miv5FieldAckInterval).
				Description("How often acknowledgements finished by the pipeline are flushed to the server. A longer interval sends fewer, larger batches; a shorter one returns capacity to the server sooner.").
				Advanced().
				Default("50ms"),
			service.NewStringEnumField(miv5FieldOnConnectRefused, mv5RefusedRetry, mv5RefusedFail).
				Description(`What to do when the server refuses the connection outright, which it reports with a reason code such as `+"`0x86`"+` bad user name or password, `+"`0x87`"+` not authorized, or `+"`0x8A`"+` banned.

- `+"`retry`"+` reconnects for ever, logging the reason code each time. A refusal caused by a missing permission then recovers on its own once the permission is granted, with no restart.
- `+"`fail`"+` stops the input instead. Prefer it for batch and one-shot pipelines, where a run that hangs is worse than a run that fails.

The reason code the server sent is logged either way, because the client library's own error text does not carry it.`).
				Advanced().
				Default(mv5RefusedRetry),
			service.NewStringEnumField(miv5FieldOnSubscribeRefused, mv5RefusedRetry, mv5RefusedFail, mv5RefusedContinue).
				Description(`What to do when the server refuses one or more of the topic filters, which it reports with a reason code such as `+"`0x87`"+` not authorized or `+"`0x8F`"+` topic filter invalid.

A refusal does not close the connection, so without one of these the pipeline would sit connected and healthy while receiving nothing at all.

- `+"`retry`"+` subscribes again on a growing delay, for ever, logging each refusal. A filter refused because a permission is missing then starts working on its own once the permission is granted, with no restart.
- `+"`fail`"+` stops the input. Prefer it for batch and one-shot pipelines, where a run that hangs is worse than a run that fails.
- `+"`continue`"+` carries on with whichever filters were granted. Use it when one filter of several is expendable — but note that the pipeline then runs while knowingly missing that data.

Every refused filter is logged with its own reason code whichever is chosen.`).
				Advanced().
				Default(mv5RefusedRetry),
			service.NewAutoRetryNacksToggleField(),
		)
}

func init() {
	err := service.RegisterInput("mqtt_v5", inputConfigSpecV5(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		rdr, err := newMQTTReaderV5FromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksToggled(conf, rdr)
	})
	if err != nil {
		panic(err)
	}
}

type mqttReaderV5 struct {
	clientConf clientConfigV5

	topics             []string
	qos                byte
	noLocal            bool
	retainAsPublished  bool
	retainHandling     byte
	ackInterval        time.Duration
	onSubscribeRefused string
	onConnectRefused   string

	log  *service.Logger
	conn *connectionV5

	msgChan chan paho.PublishReceived

	// subscribeReq carries a request from the connection-up handler, which is
	// not allowed to block, to the goroutine that does the subscribing, which
	// has to wait for a SUBACK and may have to wait out a refusal.
	subscribeReq chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	closeOnce sync.Once

	fatalMut sync.Mutex
	fatalErr error
	fatalCh  chan struct{}

	// rejected counts messages the pipeline refused, so that the warning
	// explaining what that costs is logged once rather than per message.
	rejected atomic.Int64

	// handed and settled count messages given to the pipeline and messages it
	// finished with. Their difference is what the server is still waiting to
	// hear about, and a difference that stops changing is a stalled input.
	handed  atomic.Int64
	settled atomic.Int64

	// How long the watchdog waits before deciding nothing is moving. Fields
	// rather than constants so a test can drive it in milliseconds; there is
	// no reason for an operator to change them, so they are not configuration.
	stallSample time.Duration
	stallAfter  int
}

// watchProgress warns, once, when this input stops making progress while
// holding messages the server has not been told about.
//
// It is keyed on the condition rather than on a rejection because a rejection
// is not where the common case shows up. Under the default auto_replay_nacks
// the pipeline's error never reaches this component at all: Bento re-queues the
// message and retries it internally for ever, and the acknowledgement function
// here is only ever called on eventual success. A message that can never
// succeed therefore holds its acknowledgement — and every acknowledgement
// behind it — with nothing in this component seeing an error to report. Once
// the receive window is full the server stops delivering and the pipeline sits
// healthy and idle. Watching the counters catches that; watching for rejections
// does not.
func (m *mqttReaderV5) watchProgress() {
	sample, stallAfter := m.stallSample, m.stallAfter

	ticker := time.NewTicker(sample)
	defer ticker.Stop()

	// Seeded from the counters as they stand, so the first tick is a real
	// comparison. Starting them at a sentinel made the first tick always read
	// as movement and pushed the warning out by a whole sample, which is a
	// quarter longer than the interval this reports.
	lastHanded, lastSettled := m.handed.Load(), m.settled.Load()
	still, warned := 0, false

	for {
		select {
		case <-ticker.C:
		case <-m.ctx.Done():
			return
		}

		handed, settled := m.handed.Load(), m.settled.Load()
		outstanding := handed - settled

		if outstanding <= 0 {
			// Nothing owed: a quiet topic, or an input that has caught up.
			// Either way a later stall is a new one worth reporting, so this
			// is the only place the warning re-arms.
			lastHanded, lastSettled = handed, settled
			still, warned = 0, false
			continue
		}
		if handed != lastHanded || settled != lastSettled {
			// Something moved, so the wait starts again — but the warning is
			// not re-armed. Re-arming on any movement made this report once
			// for an input that had genuinely stopped and once per message for
			// a pipeline that was merely slow, which is the wrong way round:
			// noisy about the healthy case and quiet about the broken one.
			lastHanded, lastSettled = handed, settled
			still = 0
			continue
		}

		still++
		if still < stallAfter || warned {
			continue
		}
		warned = true
		m.log.Warnf("This input has not finished a message in at least %v, and %v are outstanding. "+
			"Acknowledgements are sent in the order messages arrived, so one message the pipeline "+
			"never finishes with — retried indefinitely, or rejected — holds back every acknowledgement "+
			"behind it, and the server stops delivering once its receive window is full. If a message "+
			"cannot ever succeed, route it to a dead-letter output rather than retrying or rejecting it "+
			"for ever. A pipeline that simply takes longer than this to finish each message reaches the "+
			"same point, and for that this is expected rather than a fault. Reported once either way, "+
			"until the input catches up.", sample*time.Duration(stallAfter), outstanding)
	}
}

// outstandingLimit describes how many unacknowledged messages the server will
// allow before it stops sending, for the warning above. Unset means the server
// chose, and this client has no way to know what it chose.
func (m *mqttReaderV5) outstandingLimit() string {
	if m.clientConf.receiveMaximum != nil {
		return fmt.Sprintf("%v are outstanding", *m.clientConf.receiveMaximum)
	}
	return "enough are outstanding"
}

func newMQTTReaderV5FromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*mqttReaderV5, error) {
	m := &mqttReaderV5{
		log:          mgr.Logger(),
		msgChan:      make(chan paho.PublishReceived),
		subscribeReq: make(chan struct{}, 1),
		fatalCh:      make(chan struct{}),
		stallSample:  20 * time.Second,
		stallAfter:   3, // so a minute of complete stasis
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())

	var err error
	if m.clientConf, err = clientConfigV5FromParsed(conf); err != nil {
		return nil, err
	}
	if m.topics, err = conf.FieldStringList(miv5FieldTopics); err != nil {
		return nil, err
	}
	if len(m.topics) == 0 {
		return nil, errors.New("at least one topic filter is required")
	}

	var qos int
	if qos, err = conf.FieldInt(miv5FieldQoS); err != nil {
		return nil, err
	}
	if qos < 0 || qos > 2 {
		return nil, errors.New("qos must be 0, 1 or 2")
	}
	m.qos = byte(qos)

	if m.noLocal, err = conf.FieldBool(miv5FieldNoLocal); err != nil {
		return nil, err
	}
	if m.retainAsPublished, err = conf.FieldBool(miv5FieldRetainAsPublished); err != nil {
		return nil, err
	}
	var retainHandling int
	if retainHandling, err = conf.FieldInt(miv5FieldRetainHandling); err != nil {
		return nil, err
	}
	if retainHandling < 0 || retainHandling > 2 {
		return nil, errors.New("retain_handling must be 0, 1 or 2")
	}
	m.retainHandling = byte(retainHandling)

	if m.ackInterval, err = conf.FieldDuration(miv5FieldAckInterval); err != nil {
		return nil, err
	}
	if m.ackInterval <= 0 {
		return nil, errors.New("ack_interval must be greater than zero")
	}
	if m.onSubscribeRefused, err = conf.FieldString(miv5FieldOnSubscribeRefused); err != nil {
		return nil, err
	}
	if m.onConnectRefused, err = conf.FieldString(miv5FieldOnConnectRefused); err != nil {
		return nil, err
	}

	m.conn = newConnectionV5(m.log, m.onConnectRefused)
	return m, nil
}

// stop records a condition the input cannot work through, so that both Connect
// and Read report it as the end of the input rather than looping.
func (m *mqttReaderV5) stop(err error) {
	m.fatalMut.Lock()
	defer m.fatalMut.Unlock()
	if m.fatalErr != nil {
		return
	}
	m.fatalErr = err
	close(m.fatalCh)
}

func (m *mqttReaderV5) stopped() error {
	m.fatalMut.Lock()
	defer m.fatalMut.Unlock()
	if m.fatalErr == nil {
		return nil
	}
	// ErrEndOfInput is what Bento reads as "this input is finished"; it stops
	// the reader loop rather than reconnecting for ever.
	return fmt.Errorf("%v: %w", m.fatalErr, service.ErrEndOfInput)
}

func (m *mqttReaderV5) Connect(ctx context.Context) error {
	if err := m.stopped(); err != nil {
		return err
	}

	var cfg autopaho.ClientConfig
	m.clientConf.apply(&cfg)
	m.conn.installHooks(&cfg, m.onConnectionUp)

	cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){m.onPublishReceived}
	// Acknowledgements are held until the pipeline has finished with a message,
	// which is the reason this component exists.
	cfg.EnableManualAcknowledgment = true
	cfg.SendAcksInterval = m.ackInterval

	m.startOnce.Do(func() {
		go m.subscriber()
		go m.watchProgress()
	})

	if err := m.conn.connect(ctx, cfg); err != nil {
		if errors.Is(err, errConnectionRefused) {
			m.stop(err)
			return m.stopped()
		}
		return err
	}
	return nil
}

// onConnectionUp runs on autopaho's goroutine, which documents that it must not
// block. Subscribing means waiting for a SUBACK, and a refusal may mean waiting
// out a delay before trying again, so the work is handed to the subscriber.
func (m *mqttReaderV5) onConnectionUp(*autopaho.ConnectionManager, *paho.Connack) {
	select {
	case m.subscribeReq <- struct{}{}:
	default: // A request is already pending and will use the live connection.
	}
}

func (m *mqttReaderV5) onPublishReceived(pr paho.PublishReceived) (bool, error) {
	select {
	case m.msgChan <- pr:
	case <-m.ctx.Done():
	}
	return true, nil
}

func (m *mqttReaderV5) subscriber() {
	for {
		select {
		case <-m.subscribeReq:
		case <-m.ctx.Done():
			return
		}
		m.subscribeUntilGranted()
	}
}

// subscribeUntilGranted applies on_subscribe_refused. A refusal leaves the
// connection up and healthy while delivering nothing, so doing nothing about it
// is the one option that is never right.
func (m *mqttReaderV5) subscribeUntilGranted() {
	for attempt := 0; ; attempt++ {
		cm := m.conn.manager()
		if cm == nil {
			return
		}

		refused, err := m.subscribe(cm)
		switch {
		case err == nil:
			return
		case !refused:
			// The request never got an answer. A reconnection will ask again.
			m.log.Errorf("Subscribe failed: %v", err)
			return
		}

		switch m.onSubscribeRefused {
		case mv5RefusedFail:
			m.stop(err)
			return
		case mv5RefusedContinue:
			m.log.Warn("Carrying on with the filters that were granted; the refused ones deliver nothing.")
			return
		}

		wait := time.Duration(1<<min(attempt, 6)) * time.Second
		m.log.Infof("Retrying the subscription in %v.", wait)
		select {
		case <-time.After(wait):
		case <-m.subscribeReq:
			attempt = -1 // Reconnected: start the delay from the beginning.
		case <-m.ctx.Done():
			return
		}
	}
}

// subscribe sends one SUBSCRIBE and reports what came back. refused separates a
// server that answered and said no from a request that never got an answer,
// because only the first is worth applying a policy to.
func (m *mqttReaderV5) subscribe(cm *autopaho.ConnectionManager) (refused bool, err error) {
	subs := make([]paho.SubscribeOptions, 0, len(m.topics))
	for _, topic := range m.topics {
		subs = append(subs, paho.SubscribeOptions{
			Topic:             topic,
			QoS:               m.qos,
			NoLocal:           m.noLocal,
			RetainAsPublished: m.retainAsPublished,
			RetainHandling:    m.retainHandling,
		})
	}

	ctx, cancel := context.WithTimeout(m.ctx, m.clientConf.connectTimeout)
	defer cancel()

	suback, subErr := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs})
	if suback == nil {
		return false, subErr
	}

	// Read the packet rather than the error: the library's message reports that
	// a subscription failed and drops the code saying why.
	var refusals []string
	granted := 0
	for i, code := range suback.Reasons {
		topic := "unnamed filter"
		if i < len(m.topics) {
			topic = m.topics[i]
		}
		if code < 0x80 {
			granted++
			m.log.Debugf("Subscribed to %v at QoS %v.", topic, code)
			continue
		}
		m.log.Errorf("Subscription to %v refused: %v (0x%02X).", topic, mv5SubackReason(code), code)
		refusals = append(refusals, fmt.Sprintf("%v: %v (0x%02X)", topic, mv5SubackReason(code), code))
	}

	if len(refusals) == 0 {
		m.log.Infof("Subscribed to %v topic filter(s).", granted)
		return false, nil
	}
	return true, fmt.Errorf("server refused %v of %v topic filters — %v",
		len(refusals), len(suback.Reasons), strings.Join(refusals, "; "))
}

func (m *mqttReaderV5) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if err := m.stopped(); err != nil {
		return nil, nil, err
	}

	select {
	case pr := <-m.msgChan:
		msg, ackFn := m.messageFromPublish(pr)
		m.handed.Add(1)
		return msg, ackFn, nil
	case <-m.fatalCh:
		return nil, nil, m.stopped()
	case <-m.conn.downSignal():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (m *mqttReaderV5) messageFromPublish(pr paho.PublishReceived) (*service.Message, service.AckFunc) {
	pkt := pr.Packet
	msg := service.NewMessage(pkt.Payload)

	// User properties are written first so that the fields below always
	// describe the message as it arrived: a publisher sending a user property
	// named mqtt_topic overwrites nothing.
	if pkt.Properties != nil {
		for _, prop := range pkt.Properties.User {
			msg.MetaSetMut(prop.Key, prop.Value)
		}
	}

	msg.MetaSetMut("mqtt_topic", pkt.Topic)
	msg.MetaSetMut("mqtt_qos", int(pkt.QoS))
	msg.MetaSetMut("mqtt_retained", pkt.Retain)
	msg.MetaSetMut("mqtt_duplicate", pkt.Duplicate())
	msg.MetaSetMut("mqtt_message_id", int(pkt.PacketID))

	if props := pkt.Properties; props != nil {
		if props.ContentType != "" {
			msg.MetaSetMut("mqtt_content_type", props.ContentType)
		}
		if props.ResponseTopic != "" {
			msg.MetaSetMut("mqtt_response_topic", props.ResponseTopic)
		}
		if len(props.CorrelationData) > 0 {
			msg.MetaSetMut("mqtt_correlation_data", props.CorrelationData)
		}
		if props.MessageExpiry != nil {
			msg.MetaSetMut("mqtt_message_expiry_interval", int(*props.MessageExpiry))
		}
		if props.PayloadFormat != nil {
			msg.MetaSetMut("mqtt_payload_format_indicator", int(*props.PayloadFormat))
		}
		if props.SubscriptionIdentifier != nil {
			msg.MetaSetMut("mqtt_subscription_identifier", *props.SubscriptionIdentifier)
		}
	}

	return msg, func(ctx context.Context, res error) error {
		if res != nil {
			// Withholding the acknowledgement is the point: the server keeps
			// the message and delivers it again.
			//
			// It is also the moment consumption starts winding down, and that
			// is worth saying out loud. Acknowledgements can only be sent in
			// the order the messages arrived, so this one holds back every
			// acknowledgement behind it; when enough are outstanding the
			// server stops sending and the input receives nothing further
			// while still reporting itself connected and healthy. Warning on
			// the first rejection is what stops that being silent.
			if m.rejected.Add(1) == 1 {
				m.log.Warnf("A message on %v was rejected and will not be acknowledged: %v. "+
					"Acknowledgements are sent in the order messages arrived, so this holds back every "+
					"acknowledgement behind it, and once %v the server will stop delivering and this input "+
					"will consume nothing further. Route messages that cannot succeed to a dead-letter "+
					"output instead of rejecting them.",
					pkt.Topic, res, m.outstandingLimit())
			}
			return nil
		}
		m.settled.Add(1)
		if err := pr.Client.Ack(pkt); err != nil {
			// Acknowledging after the link has dropped is documented as
			// unpredictable, and there is nothing Bento could do with an error
			// here that redelivery does not already do.
			m.log.Debugf("Could not acknowledge a message on %v: %v", pkt.Topic, err)
		}
		return nil
	}
}

func (m *mqttReaderV5) Close(ctx context.Context) error {
	m.closeOnce.Do(m.cancel)
	return m.conn.close(ctx)
}
