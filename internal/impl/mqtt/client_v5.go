package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	mv5FieldURLs              = "urls"
	mv5FieldClientID          = "client_id"
	mv5FieldDynClientIDSuffix = "dynamic_client_id_suffix"
	mv5FieldConnectTimeout    = "connect_timeout"
	mv5FieldKeepAlive         = "keepalive"
	mv5FieldUser              = "user"
	mv5FieldPassword          = "password"
	mv5FieldTLS               = "tls"

	mv5FieldCleanStart            = "clean_start"
	mv5FieldSessionExpiryInterval = "session_expiry_interval"
	mv5FieldReceiveMaximum        = "receive_maximum"
	mv5FieldMaximumPacketSize     = "maximum_packet_size"

	mv5FieldWill              = "will"
	mv5FieldWillEnabled       = "enabled"
	mv5FieldWillQoS           = "qos"
	mv5FieldWillRetained      = "retained"
	mv5FieldWillTopic         = "topic"
	mv5FieldWillPayload       = "payload"
	mv5FieldWillDelayInterval = "delay_interval"

	mv5FieldReconnectBackoff    = "reconnect_backoff"
	mv5FieldReconnectBackoffMin = "min"
	mv5FieldReconnectBackoffMax = "max"
)

// Actions available when the server refuses something we asked for. Not every
// action is offered for every refusal: a connection cannot be carried on with,
// and a subscription list can.
const (
	mv5RefusedRetry    = "retry"
	mv5RefusedFail     = "fail"
	mv5RefusedContinue = "continue"
)

// clientFieldsV5 returns the configuration fields shared by the mqtt_v5 input
// and output. Field names match the 3.1.1 components wherever the concept
// survives into MQTT 5, so that moving a configuration between them changes as
// little as possible.
func clientFieldsV5() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewURLListField(mv5FieldURLs).
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"tcp://localhost:1883"}),
		service.NewStringField(mv5FieldClientID).
			Description("An identifier for the client connection. Under MQTT 5 this is the key the server stores a session against, so a client that wants to resume a session must present the same identifier every time it connects.").
			Default(""),
		service.NewStringAnnotatedEnumField(mv5FieldDynClientIDSuffix, map[string]string{
			"nanoid": "append a nanoid of length 21 characters",
		}).
			Description("Append a dynamically generated suffix to the specified `client_id` on each run of the pipeline. This can be useful when clustering Bento producers. Note that a generated suffix produces a new client identifier on every run, and therefore a new session: combining it with `clean_start: false` will not resume anything.").
			Optional().
			Advanced().
			LintRule(`root = []`), // Disable linting for now
		service.NewDurationField(mv5FieldConnectTimeout).
			Description("The maximum amount of time to wait in order to establish a connection before the attempt is abandoned.").
			Default("30s").
			Examples("1s", "500ms"),
		service.NewIntField(mv5FieldKeepAlive).
			Description("Max seconds of inactivity before a keepalive message is sent.").
			Default(30).
			Advanced(),
		service.NewStringField(mv5FieldUser).
			Description("A username to connect with.").
			Default("").
			Advanced(),
		service.NewStringField(mv5FieldPassword).
			Description("A password to connect with.").
			Default("").
			Secret().
			Advanced(),
		service.NewTLSToggledField(mv5FieldTLS),

		service.NewBoolField(mv5FieldCleanStart).
			Description("Whether to discard any session the server is holding for this `client_id` when connecting for the first time. Reconnections made by this component are always made with clean start disabled, so that a session established once is resumed rather than thrown away.\n\nA durable subscription is the combination of three settings: `clean_start: false`, a fixed `client_id`, and a `session_expiry_interval` above zero. Any one of them left at its default is enough to lose messages sent while the pipeline was down.").
			Default(true),
		service.NewDurationField(mv5FieldSessionExpiryInterval).
			Description("How long the server keeps this client's session after the connection closes. Zero means the session ends the moment the connection does. Rounded down to whole seconds, which is the resolution MQTT 5 carries it at.").
			Default("0s").
			Examples("1h", "24h"),
		service.NewIntField(mv5FieldReceiveMaximum).
			Description("The number of QoS 1 and QoS 2 messages this client is willing to have in flight at once — in effect, how far ahead of the pipeline the server is allowed to read. Left unset, the server chooses.").
			Optional().
			Advanced(),
		service.NewIntField(mv5FieldMaximumPacketSize).
			Description("The largest packet, in bytes, this client is willing to accept. Left unset, no limit is sent and the server may deliver a packet of any size it supports.").
			Optional().
			Advanced(),
		service.NewObjectField(mv5FieldWill,
			service.NewBoolField(mv5FieldWillEnabled).
				Description("Whether to enable last will messages.").
				Default(false),
			service.NewIntField(mv5FieldWillQoS).
				Description("Set QoS for last will message. Valid values are: 0, 1, 2.").
				Default(0),
			service.NewBoolField(mv5FieldWillRetained).
				Description("Set retained for last will message.").
				Default(false),
			service.NewStringField(mv5FieldWillTopic).
				Description("Set topic for last will message.").
				Default(""),
			service.NewStringField(mv5FieldWillPayload).
				Description("Set payload for last will message.").
				Default(""),
			service.NewDurationField(mv5FieldWillDelayInterval).
				Description("How long the server waits after the connection drops before publishing the will message. A delay longer than the time it takes the pipeline to reconnect means a brief network outage publishes no will at all. Rounded down to whole seconds.").
				Default("0s"),
		).
			Description("Set last will message in case of Bento failure").
			Advanced(),

		service.NewObjectField(mv5FieldReconnectBackoff,
			service.NewDurationField(mv5FieldReconnectBackoffMin).
				Description("How long to wait before the first reconnection attempt.").
				Default("1s"),
			service.NewDurationField(mv5FieldReconnectBackoffMax).
				Description("The longest this component will wait between reconnection attempts.").
				Default("1m"),
		).
			Description("How long to wait between attempts to re-establish a dropped connection. The wait grows from `min` towards `max`, and is reset once a connection succeeds.").
			Advanced(),
	}
}

type willConfigV5 struct {
	enabled       bool
	qos           byte
	retained      bool
	topic         string
	payload       string
	delayInterval uint32
}

type clientConfigV5 struct {
	urls           []*url.URL
	clientID       string
	connectTimeout time.Duration
	keepAlive      uint16
	username       string
	password       string
	tlsEnabled     bool
	tlsConf        *tls.Config

	cleanStart            bool
	sessionExpiryInterval uint32
	receiveMaximum        *uint16
	maximumPacketSize     *uint32

	will willConfigV5

	backoffMin time.Duration
	backoffMax time.Duration
}

// secondsFromDuration converts a configured duration to the whole seconds MQTT
// 5 carries its intervals in, refusing anything that cannot be represented
// rather than silently wrapping to a value the server would accept and honour.
func secondsFromDuration(name string, d time.Duration) (uint32, error) {
	if d < 0 {
		return 0, fmt.Errorf("%v cannot be negative", name)
	}
	if secs := d.Seconds(); secs > math.MaxUint32 {
		return 0, fmt.Errorf("%v is larger than the protocol allows", name)
	}
	return uint32(d / time.Second), nil
}

func clientConfigV5FromParsed(conf *service.ParsedConfig) (c clientConfigV5, err error) {
	if c.urls, err = conf.FieldURLList(mv5FieldURLs); err != nil {
		return
	}
	if c.clientID, err = conf.FieldString(mv5FieldClientID); err != nil {
		return
	}
	if conf.Contains(mv5FieldDynClientIDSuffix) {
		var suffix string
		if suffix, err = conf.FieldString(mv5FieldDynClientIDSuffix); err != nil {
			return
		}
		switch suffix {
		case "nanoid":
			var nid string
			if nid, err = gonanoid.New(); err != nil {
				err = fmt.Errorf("failed to generate nanoid: %w", err)
				return
			}
			c.clientID += nid
		case "":
		default:
			err = fmt.Errorf("unknown dynamic_client_id_suffix: %v", suffix)
			return
		}
	}
	if c.connectTimeout, err = conf.FieldDuration(mv5FieldConnectTimeout); err != nil {
		return
	}

	var keepAlive int
	if keepAlive, err = conf.FieldInt(mv5FieldKeepAlive); err != nil {
		return
	}
	if keepAlive < 0 || keepAlive > math.MaxUint16 {
		err = fmt.Errorf("keepalive must be between 0 and %v seconds", math.MaxUint16)
		return
	}
	c.keepAlive = uint16(keepAlive)

	if c.username, err = conf.FieldString(mv5FieldUser); err != nil {
		return
	}
	if c.password, err = conf.FieldString(mv5FieldPassword); err != nil {
		return
	}
	if c.tlsConf, c.tlsEnabled, err = conf.FieldTLSToggled(mv5FieldTLS); err != nil {
		return
	}

	if c.cleanStart, err = conf.FieldBool(mv5FieldCleanStart); err != nil {
		return
	}
	var sessionExpiry time.Duration
	if sessionExpiry, err = conf.FieldDuration(mv5FieldSessionExpiryInterval); err != nil {
		return
	}
	if c.sessionExpiryInterval, err = secondsFromDuration(mv5FieldSessionExpiryInterval, sessionExpiry); err != nil {
		return
	}

	if conf.Contains(mv5FieldReceiveMaximum) {
		var v int
		if v, err = conf.FieldInt(mv5FieldReceiveMaximum); err != nil {
			return
		}
		if v < 1 || v > math.MaxUint16 {
			err = fmt.Errorf("receive_maximum must be between 1 and %v", math.MaxUint16)
			return
		}
		receiveMaximum := uint16(v)
		c.receiveMaximum = &receiveMaximum
	}
	if conf.Contains(mv5FieldMaximumPacketSize) {
		var v int
		if v, err = conf.FieldInt(mv5FieldMaximumPacketSize); err != nil {
			return
		}
		if v < 1 || v > math.MaxUint32 {
			err = fmt.Errorf("maximum_packet_size must be between 1 and %v", uint32(math.MaxUint32))
			return
		}
		maximumPacketSize := uint32(v)
		c.maximumPacketSize = &maximumPacketSize
	}
	if c.will, err = willConfigV5FromParsed(conf.Namespace(mv5FieldWill)); err != nil {
		return
	}

	backoff := conf.Namespace(mv5FieldReconnectBackoff)
	if c.backoffMin, err = backoff.FieldDuration(mv5FieldReconnectBackoffMin); err != nil {
		return
	}
	if c.backoffMax, err = backoff.FieldDuration(mv5FieldReconnectBackoffMax); err != nil {
		return
	}
	// autopaho.NewExponentialBackoff panics rather than returning an error, so
	// its constraints are checked here where a bad value is still a
	// configuration error the user can read.
	if c.backoffMin <= 0 {
		err = errors.New("reconnect_backoff.min must be greater than zero")
		return
	}
	if c.backoffMax <= c.backoffMin {
		err = errors.New("reconnect_backoff.max must be greater than reconnect_backoff.min")
		return
	}
	return
}

func willConfigV5FromParsed(conf *service.ParsedConfig) (w willConfigV5, err error) {
	if w.enabled, err = conf.FieldBool(mv5FieldWillEnabled); err != nil {
		return
	}
	var qos int
	if qos, err = conf.FieldInt(mv5FieldWillQoS); err != nil {
		return
	}
	if qos < 0 || qos > 2 {
		err = errors.New("will qos must be 0, 1 or 2")
		return
	}
	w.qos = byte(qos)
	if w.retained, err = conf.FieldBool(mv5FieldWillRetained); err != nil {
		return
	}
	if w.topic, err = conf.FieldString(mv5FieldWillTopic); err != nil {
		return
	}
	if w.payload, err = conf.FieldString(mv5FieldWillPayload); err != nil {
		return
	}
	var delay time.Duration
	if delay, err = conf.FieldDuration(mv5FieldWillDelayInterval); err != nil {
		return
	}
	if w.delayInterval, err = secondsFromDuration(mv5FieldWillDelayInterval, delay); err != nil {
		return
	}
	if w.enabled && w.topic == "" {
		err = errors.New("include topic to register a last will")
		return
	}
	return
}

func (w *willConfigV5) apply(cfg *autopaho.ClientConfig) {
	if !w.enabled {
		return
	}
	cfg.WillMessage = &paho.WillMessage{
		Retain:  w.retained,
		QoS:     w.qos,
		Topic:   w.topic,
		Payload: []byte(w.payload),
	}
	delayInterval := w.delayInterval
	cfg.WillProperties = &paho.WillProperties{WillDelayInterval: &delayInterval}
}

// apply fills in everything about the connection that the input and the output
// share. The caller is left to set the handlers that differ between them:
// OnConnectionUp, which the input uses to re-subscribe, and OnPublishReceived.
func (c *clientConfigV5) apply(cfg *autopaho.ClientConfig) {
	cfg.ServerUrls = c.urls
	cfg.ClientID = c.clientID
	cfg.KeepAlive = c.keepAlive
	cfg.ConnectTimeout = c.connectTimeout
	cfg.CleanStartOnInitialConnection = c.cleanStart
	cfg.SessionExpiryInterval = c.sessionExpiryInterval
	cfg.ReconnectBackoff = autopaho.NewExponentialBackoff(c.backoffMin, c.backoffMax, c.backoffMin, 2)

	// Note for the output: leaving Queue nil does not disable queueing —
	// autopaho substitutes an in-memory queue when it is nil. What keeps Bento
	// owning retries is publishing through ConnectionManager.Publish, which
	// sends directly and errors when the link is down, rather than
	// PublishViaQueue, which accepts the message and delivers it later with no
	// status reported back. Two retry engines fighting is how a message goes
	// nowhere twice.

	if c.tlsEnabled {
		cfg.TlsCfg = c.tlsConf
	}
	if c.username != "" {
		cfg.ConnectUsername = c.username
	}
	if c.password != "" {
		cfg.ConnectPassword = []byte(c.password)
	}
	c.will.apply(cfg)

	// Receive Maximum and Maximum Packet Size have no field on
	// autopaho.ClientConfig and reach the wire only through this builder.
	// autopaho allocates Properties only when SessionExpiryInterval is
	// non-zero, so on a default configuration it arrives here nil.
	if c.receiveMaximum != nil || c.maximumPacketSize != nil {
		receiveMaximum, maximumPacketSize := c.receiveMaximum, c.maximumPacketSize
		cfg.ConnectPacketBuilder = func(pkt *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			if pkt.Properties == nil {
				pkt.Properties = &paho.ConnectProperties{}
			}
			if receiveMaximum != nil {
				pkt.Properties.ReceiveMaximum = receiveMaximum
			}
			if maximumPacketSize != nil {
				pkt.Properties.MaximumPacketSize = maximumPacketSize
			}
			return pkt, nil
		}
	}
}

// mv5ConnackReasons names the CONNACK reason codes MQTT 5 defines. The client
// library's error text does not carry the code, so a refusal logged from the
// error alone says only that the connection failed, never why.
var mv5ConnackReasons = map[byte]string{
	0x80: "unspecified error",
	0x81: "malformed packet",
	0x82: "protocol error",
	0x83: "implementation specific error",
	0x84: "unsupported protocol version",
	0x85: "client identifier not valid",
	0x86: "bad user name or password",
	0x87: "not authorized",
	0x88: "server unavailable",
	0x89: "server busy",
	0x8A: "banned",
	0x8C: "bad authentication method",
	0x90: "topic name invalid",
	0x95: "packet too large",
	0x97: "quota exceeded",
	0x99: "payload format invalid",
	0x9A: "retain not supported",
	0x9B: "QoS not supported",
	0x9C: "use another server",
	0x9D: "server moved",
	0x9F: "connection rate exceeded",
}

func mv5ConnackReason(code byte) string {
	if name, ok := mv5ConnackReasons[code]; ok {
		return name
	}
	return "unrecognised reason code"
}

// mv5DisconnectReasons names the reason codes a server can send in a DISCONNECT
// when it hangs up on us. These are a different set from the CONNACK codes, and
// without them a server that says exactly why it dropped the link is
// indistinguishable in the log from a flaky network.
var mv5DisconnectReasons = map[byte]string{
	0x00: "normal disconnection",
	0x80: "unspecified error",
	0x81: "malformed packet",
	0x82: "protocol error",
	0x83: "implementation specific error",
	0x87: "not authorized",
	0x89: "server busy",
	0x8B: "server shutting down",
	0x8D: "keep alive timeout",
	0x8E: "session taken over",
	0x8F: "topic filter invalid",
	0x90: "topic name invalid",
	0x93: "receive maximum exceeded",
	0x94: "topic alias invalid",
	0x95: "packet too large",
	0x96: "message rate too high",
	0x97: "quota exceeded",
	0x98: "administrative action",
	0x99: "payload format invalid",
	0x9A: "retain not supported",
	0x9B: "QoS not supported",
	0x9C: "use another server",
	0x9D: "server moved",
	0x9E: "shared subscriptions not supported",
	0x9F: "connection rate exceeded",
	0xA0: "maximum connect time",
	0xA1: "subscription identifiers not supported",
	0xA2: "wildcard subscriptions not supported",
}

func mv5DisconnectReason(code byte) string {
	if name, ok := mv5DisconnectReasons[code]; ok {
		return name
	}
	return "unrecognised reason code"
}

// errConnectionRefused marks a connection the server answered and turned down,
// as opposed to one that never got an answer at all. Components test for it
// with errors.Is to honour on_connect_refused.
var errConnectionRefused = errors.New("server refused the connection")

// connackRefusal reports the reason code when err is the server refusing the
// connection. The code survives on the wrapped error even though the message
// built from it does not.
func connackRefusal(err error) (byte, bool) {
	var refusal *autopaho.ConnackError
	if errors.As(err, &refusal) {
		return refusal.ReasonCode, true
	}
	return 0, false
}

// mv5PahoLogger carries the client library's own errors into Bento's log,
// which otherwise discards them: autopaho defaults both of its loggers to one
// that throws every line away.
type mv5PahoLogger struct{ log *service.Logger }

func (l mv5PahoLogger) Println(v ...any) { l.log.Error(fmt.Sprint(v...)) }

func (l mv5PahoLogger) Printf(format string, v ...any) { l.log.Errorf(format, v...) }

// connectionV5 owns the connection manager for one component. autopaho
// reconnects and resumes sessions on its own, which is the reason for using it,
// but it does so silently: left to itself it would retry a refused connection
// for ever behind a Connect call that never returns, so Bento would log
// nothing, count nothing, and show a healthy pipeline moving no data.
// connectionV5 exists to put that back in front of Bento.
type connectionV5 struct {
	log       *service.Logger
	onRefused string

	// ctx governs the connection manager itself, which keeps retrying in the
	// background and must outlive any single call to connect — Bento cancels
	// the context it passes to Connect as soon as Connect returns.
	ctx    context.Context
	cancel context.CancelFunc

	mu   sync.Mutex
	cm   *autopaho.ConnectionManager
	down chan struct{}

	connErrs chan error
}

func newConnectionV5(log *service.Logger, onRefused string) *connectionV5 {
	ctx, cancel := context.WithCancel(context.Background())
	c := &connectionV5{
		log:       log,
		onRefused: onRefused,
		ctx:       ctx,
		cancel:    cancel,
		down:      make(chan struct{}),
		connErrs:  make(chan error, 1),
	}
	close(c.down) // Nothing is connected until the first OnConnectionUp.
	return c
}

// installHooks wires the connection lifecycle onto cfg. onUp is called after
// every successful connection, the first one included, and is where the input
// re-subscribes. autopaho calls it on its own goroutine and documents that it
// must not block, so anything done there needs a bounded context.
func (c *connectionV5) installHooks(cfg *autopaho.ClientConfig, onUp func(*autopaho.ConnectionManager, *paho.Connack)) {
	cfg.OnConnectionUp = func(cm *autopaho.ConnectionManager, connack *paho.Connack) {
		c.mu.Lock()
		c.down = make(chan struct{})
		c.mu.Unlock()
		c.log.Info("Connection established.")
		if onUp != nil {
			onUp(cm, connack)
		}
	}
	cfg.OnConnectionDown = func() bool {
		c.markDown()
		c.log.Warn("Connection lost, reconnecting.")
		return true
	}
	cfg.OnConnectError = func(err error) {
		if code, refused := connackRefusal(err); refused {
			c.log.Errorf("Server refused the connection: %v (0x%02X).", mv5ConnackReason(code), code)
		} else {
			c.log.Errorf("Connection attempt failed: %v", err)
		}
		select {
		case c.connErrs <- err:
		default: // An earlier failure is still waiting to be read; one is enough.
		}
	}
	cfg.OnServerDisconnect = func(disconnect *paho.Disconnect) {
		// Reached only when the server sends a DISCONNECT, which is it saying
		// why it is hanging up. autopaho reconnects either way, so there is
		// nothing to decide here — but the reason is the only thing that
		// separates "another client took this client_id" from a bad network.
		reason := mv5DisconnectReason(disconnect.ReasonCode)
		if disconnect.Properties != nil {
			if disconnect.Properties.ReasonString != "" {
				reason = fmt.Sprintf("%v: %v", reason, disconnect.Properties.ReasonString)
			}
			if disconnect.Properties.ServerReference != "" {
				reason = fmt.Sprintf("%v, server reference %v", reason, disconnect.Properties.ServerReference)
			}
		}
		c.log.Errorf("Server closed the connection: %v (0x%02X).", reason, disconnect.ReasonCode)
	}
	cfg.Errors = mv5PahoLogger{log: c.log}
}

func (c *connectionV5) markDown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.down:
	default:
		close(c.down)
	}
}

// downSignal is closed while there is no connection, so that a component
// blocked on a read can notice the link has gone and tell Bento about it.
func (c *connectionV5) downSignal() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.down
}

func (c *connectionV5) manager() *autopaho.ConnectionManager {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cm
}

// connect builds the connection manager on first use and waits for the link to
// come up. A refusal by the server is returned rather than waited through: the
// manager goes on retrying in the background either way, but returning lets
// Bento log it, count it against its failed-connection metric, and apply its
// own backoff before calling again.
func (c *connectionV5) connect(ctx context.Context, cfg autopaho.ClientConfig) error {
	c.mu.Lock()
	if c.cm == nil {
		cm, err := autopaho.NewConnection(c.ctx, cfg)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		c.cm = cm
	}
	cm := c.cm
	c.mu.Unlock()

	// A failure left over from a previous call describes a connection attempt
	// Bento has already been told about.
	select {
	case <-c.connErrs:
	default:
	}

	awaitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	awaited := make(chan error, 1)
	go func() { awaited <- cm.AwaitConnection(awaitCtx) }()

	select {
	case err := <-awaited:
		return err
	case err := <-c.connErrs:
		if code, refused := connackRefusal(err); refused && c.onRefused == mv5RefusedFail {
			return fmt.Errorf("%w: %v (0x%02X)", errConnectionRefused, mv5ConnackReason(code), code)
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connectionV5) close(ctx context.Context) error {
	c.mu.Lock()
	cm := c.cm
	c.cm = nil
	c.mu.Unlock()

	c.markDown()

	var err error
	if cm != nil {
		err = cm.Disconnect(ctx)
	}
	c.cancel()
	return err
}
