package mqtt

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	mov5FieldTopic                  = "topic"
	mov5FieldQoS                    = "qos"
	mov5FieldWriteTimeout           = "write_timeout"
	mov5FieldRetained               = "retained"
	mov5FieldRetainedInterpolated   = "retained_interpolated"
	mov5FieldContentType            = "content_type"
	mov5FieldResponseTopic          = "response_topic"
	mov5FieldCorrelationData        = "correlation_data"
	mov5FieldMessageExpiryInterval  = "message_expiry_interval"
	mov5FieldPayloadFormatIndicator = "payload_format_indicator"
	mov5FieldMetadata               = "metadata"
)

// mv5PubackReasons names the reason codes a server can refuse a publication
// with. Codes below 0x80 are successes — 0x10 "no matching subscribers" in
// particular reads like a failure and is not one.
var mv5PubackReasons = map[byte]string{
	0x80: "unspecified error",
	0x83: "implementation specific error",
	0x87: "not authorized",
	0x90: "topic name invalid",
	0x91: "packet identifier in use",
	0x92: "packet identifier not found",
	0x97: "quota exceeded",
	0x99: "payload format invalid",
}

func mv5PubackReason(code byte) string {
	if name, ok := mv5PubackReasons[code]; ok {
		return name
	}
	return "unrecognised reason code"
}

func outputConfigSpecV5() *service.ConfigSpec {
	// Deliberately not marked Stable: components resting on a v0 dependency are
	// documented as experimental, which is what a plugin spec defaults to.
	return service.NewConfigSpec().
		Categories("Services").
		Summary("Pushes messages to an MQTT 5 broker.").
		Description(`
This output speaks MQTT 5 only, and will not fall back to 3.1.1. Use the `+"`mqtt`"+` output for servers that speak the older protocol.

### Refused publications

At QoS 1 and above the server answers each publication with a reason code, and a refusal — `+"`0x87`"+` not authorized, `+"`0x97`"+` quota exceeded, `+"`0x90`"+` topic name invalid — is returned as an error carrying that code. There is no setting here for what to do about it, because Bento already has a better one: a [`+"`fallback`"+`](/docs/components/outputs/fallback) output puts the error text into a `+"`fallback_error`"+` metadata field, which a [`+"`switch`"+`](/docs/components/outputs/switch) can then route on. That gives a different destination per reason code rather than one blanket behaviour:

`+"```yaml"+`
output:
  fallback:
    - mqtt_v5:
        urls: [ tcp://localhost:1883 ]
        topic: events
    - switch:
        cases:
          - check: 'meta("fallback_error").contains("0x97")'
            output:
              retry:
                output:
                  mqtt_v5:
                    urls: [ tcp://localhost:1883 ]
                    topic: events
          - output:
              file:
                path: ./dead-letter.jsonl
`+"```"+`

Note that `+"`0x10`"+` "no matching subscribers" is a success, not a failure: it means the message was accepted and nobody was listening.

### Metadata as user properties

Every metadata field on a message is sent as an MQTT 5 user property, except those excluded by the `+"`metadata`"+` setting — which by default holds back the `+"`mqtt_`"+` namespace the `+"`mqtt_v5`"+` input writes about a delivery. So a pipeline's own metadata travels and the previous hop's bookkeeping does not, and either can be changed.

This is the part MQTT 3.1.1 cannot do at all, and it is what carries an identifier or a routing key alongside the payload rather than inside it.

### Bridging one MQTT 5 server to another

Reading with the `+"`mqtt_v5`"+` input and writing with this output works without configuring metadata at all: the input describes each delivery in fields named `+"`mqtt_*`"+`, and this output holds that namespace back by default rather than forwarding it to the next server.

What does need naming is the MQTT 5 properties you want carried. They arrive as metadata, so unless the field is named here they would not be sent as properties at all:

`+"```yaml"+`
input:
  mqtt_v5:
    urls: [ tcp://source:1883 ]
    topics: [ events/# ]

output:
  mqtt_v5:
    urls: [ tcp://destination:1883 ]
    topic: ${! meta("mqtt_topic") }
    # These are re-stated deliberately: an inbound content type arrives as
    # metadata, so without naming it here it would not be sent as a content
    # type. The bookkeeping the input wrote — topic, QoS, delivery flags — is
    # held back by default and needs nothing said about it.
    content_type: ${! meta("mqtt_content_type") }
    correlation_data: ${! meta("mqtt_correlation_data") }
`+"```"+`

The same applies to `+"`response_topic`"+`, `+"`message_expiry_interval`"+` and `+"`payload_format_indicator`"+`: name the ones the bridge should carry.

Naming one is safe even when a message does not have it. These fields are all optional, and an interpolation that resolves to nothing — which is what reading an absent metadata field gives you — leaves the property unset rather than failing the message. A value that is present but malformed is still an error, because that is a configuration mistake rather than an absent property.

Reading an absent metadata field yields the text `+"`null`"+`, so that text is what marks a property as unset. A value that is genuinely the four characters `+"`null`"+` — conceivable as correlation data, if nothing else — is therefore not sent. The same is true of the `+"`retained_interpolated`"+` field, whose configured `+"`retained`"+` value stands when the field it reads is absent.`+service.OutputPerformanceDocs(true, false)).
		Fields(clientFieldsV5()...).
		Fields(
			service.NewInterpolatedStringField(mov5FieldTopic).
				Description("The topic to publish messages to."),
			service.NewIntField(mov5FieldQoS).
				Description("The QoS value to set for each message. Has options 0, 1, 2. At QoS 0 the server sends no acknowledgement at all, so a message it would have refused is indistinguishable from one it accepted.").
				Default(1),
			service.NewDurationField(mov5FieldWriteTimeout).
				Description("The maximum amount of time to wait to write data before the attempt is abandoned.").
				Examples("1s", "500ms").
				Default("3s"),
			service.NewBoolField(mov5FieldRetained).
				Description("Set message as retained on the topic.").
				Default(false),
			service.NewInterpolatedStringField(mov5FieldRetainedInterpolated).
				Description("Override the value of `retained` with an interpolable value, this allows it to be dynamically set based on message contents. The value must resolve to either `true` or `false`.").
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(mov5FieldContentType).
				Description("The MIME type describing the payload, sent as the MQTT 5 content type property.").
				Examples("application/json", `${! meta("content_type") }`).
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(mov5FieldResponseTopic).
				Description("The topic a reply to this message should be published to, sent as the MQTT 5 response topic property.").
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(mov5FieldCorrelationData).
				Description("Data a requester uses to match a reply with its request, sent as the MQTT 5 correlation data property.").
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(mov5FieldMessageExpiryInterval).
				Description("How long, in seconds, the server should keep the message for a subscriber that is not connected. Must resolve to a whole number of seconds.").
				Examples("300", `${! meta("expiry_seconds") }`).
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(mov5FieldPayloadFormatIndicator).
				Description("Whether the payload is UTF-8 text: `1` if it is, `0` if it is unspecified bytes. Must resolve to `0` or `1`.").
				Advanced().
				Optional(),
			service.NewObjectField(mov5FieldMetadata,
				service.NewStringListField("exclude_prefixes").
					Description("Metadata keys beginning with any of these are not sent.").
					Default([]any{"mqtt_"}),
			).
				Description(`Which metadata values are sent as MQTT 5 user properties. Everything is sent except the prefixes listed here.

The default excludes `+"`mqtt_`"+`, because that is the namespace the `+"`mqtt_v5`"+` input writes to describe a message it received — its topic, its QoS, its delivery flags. Those describe the hop the message just made rather than the message, and forwarding them means every server-to-server hop adds another handful of properties a consumer did not ask for and a strict server may refuse.

Set it to an empty list to send them, and to other prefixes to hold back metadata of your own:

`+"```yml"+`
metadata:
  exclude_prefixes: []                    # send everything, mqtt_ fields included
  exclude_prefixes: [ mqtt_, secret_ ]    # the default, plus one of your own
`+"```"+`

Note that this setting replaces the default rather than adding to it, so a list naming only your own prefixes will send the `+"`mqtt_`"+` fields again. Include `+"`mqtt_`"+` alongside yours if you want both held back.

Metadata can also be shaped before it reaches here with a `+"[`mapping`](/docs/components/processors/mapping)"+` processor, which is the way to rename a field, drop one, or promote one into the payload.`),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterOutput("mqtt_v5", outputConfigSpecV5(), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
		if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
			return
		}
		out, err = newMQTTWriterV5FromParsed(conf, mgr)
		return
	})
	if err != nil {
		panic(err)
	}
}

type mqttWriterV5 struct {
	clientConf clientConfigV5

	topic          *service.InterpolatedString
	qos            byte
	retained       bool
	retainedInterp *service.InterpolatedString
	writeTimeout   time.Duration

	contentType   *service.InterpolatedString
	responseTopic *service.InterpolatedString
	correlation   *service.InterpolatedString
	messageExpiry *service.InterpolatedString
	payloadFormat *service.InterpolatedString

	metaFilter *service.MetadataExcludeFilter

	log  *service.Logger
	conn *connectionV5
}

func newMQTTWriterV5FromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*mqttWriterV5, error) {
	m := &mqttWriterV5{log: mgr.Logger()}

	var err error
	if m.clientConf, err = clientConfigV5FromParsed(conf); err != nil {
		return nil, err
	}
	if m.topic, err = conf.FieldInterpolatedString(mov5FieldTopic); err != nil {
		return nil, err
	}
	var qos int
	if qos, err = conf.FieldInt(mov5FieldQoS); err != nil {
		return nil, err
	}
	if qos < 0 || qos > 2 {
		return nil, errors.New("qos must be 0, 1 or 2")
	}
	m.qos = byte(qos)

	if m.writeTimeout, err = conf.FieldDuration(mov5FieldWriteTimeout); err != nil {
		return nil, err
	}
	if m.retained, err = conf.FieldBool(mov5FieldRetained); err != nil {
		return nil, err
	}
	if iStrp, _ := conf.FieldString(mov5FieldRetainedInterpolated); iStrp != "" {
		if m.retainedInterp, err = conf.FieldInterpolatedString(mov5FieldRetainedInterpolated); err != nil {
			return nil, err
		}
	}

	for _, optional := range []struct {
		field string
		dest  **service.InterpolatedString
	}{
		{mov5FieldContentType, &m.contentType},
		{mov5FieldResponseTopic, &m.responseTopic},
		{mov5FieldCorrelationData, &m.correlation},
		{mov5FieldMessageExpiryInterval, &m.messageExpiry},
		{mov5FieldPayloadFormatIndicator, &m.payloadFormat},
	} {
		if !conf.Contains(optional.field) {
			continue
		}
		if *optional.dest, err = conf.FieldInterpolatedString(optional.field); err != nil {
			return nil, err
		}
	}

	if m.metaFilter, err = conf.FieldMetadataExcludeFilter(mov5FieldMetadata); err != nil {
		return nil, err
	}

	// An output that stopped itself would be a pipeline that quietly stops
	// delivering, so a refused connection is always retried here — logged with
	// its reason code by the connection's own handlers. on_connect_refused is
	// offered on the input, where stopping is a coherent thing to want.
	m.conn = newConnectionV5(m.log, mv5RefusedRetry)
	return m, nil
}

func (m *mqttWriterV5) Connect(ctx context.Context) error {
	var cfg autopaho.ClientConfig
	m.clientConf.apply(&cfg)
	m.conn.installHooks(&cfg, nil)
	return m.conn.connect(ctx, cfg)
}

func (m *mqttWriterV5) Write(ctx context.Context, msg *service.Message) error {
	cm := m.conn.manager()
	if cm == nil {
		return service.ErrNotConnected
	}

	topic, err := m.topic.TryString(msg)
	if err != nil {
		return fmt.Errorf("topic interpolation error: %w", err)
	}

	retained := m.retained
	if m.retainedInterp != nil {
		retainedStr, parseErr := m.retainedInterp.TryString(msg)
		switch {
		case parseErr != nil:
			m.log.Errorf("Retained interpolation error: %v", parseErr)
		case propertyUnset(retainedStr):
			// The field it reads is absent, so the configured retained value
			// stands. Reporting that as a parse failure described a malformed
			// value when nothing was malformed, and did so once per message.
		default:
			if retained, parseErr = strconv.ParseBool(retainedStr); parseErr != nil {
				m.log.Errorf("Error parsing boolean value from retained flag: %v", parseErr)
				retained = m.retained
			}
		}
	}

	payload, err := msg.AsBytes()
	if err != nil {
		return err
	}

	props, err := m.properties(msg)
	if err != nil {
		return err
	}

	writeCtx, cancel := context.WithTimeout(ctx, m.writeTimeout)
	defer cancel()

	resp, err := cm.Publish(writeCtx, &paho.Publish{
		Topic:      topic,
		QoS:        m.qos,
		Retain:     retained,
		Payload:    payload,
		Properties: props,
	})
	if err != nil {
		if errors.Is(err, autopaho.ConnectionDownError) {
			return service.ErrNotConnected
		}
		return m.publishError(resp, err)
	}

	// The library reports a refusal as an error at QoS 1, and as a success at
	// QoS 2: publishQoS12 returns a PUBREC it has itself logged as "must have
	// errored" with a nil error, and a PUBCOMP carrying a failure code the same
	// way. Reading the reason code here rather than trusting the error return
	// is what stops a publication the server rejected being reported to the
	// pipeline as delivered.
	if resp != nil && resp.ReasonCode >= 0x80 {
		return m.publishError(resp, nil)
	}
	return nil
}

// publishError builds the error a refused publication is reported with. The
// text matters beyond the log: a fallback output copies it into metadata, so a
// pipeline routes on the reason code by matching this string.
func (m *mqttWriterV5) publishError(resp *paho.PublishResponse, cause error) error {
	if resp == nil {
		return fmt.Errorf("publish failed: %w", cause)
	}
	reason := mv5PubackReason(resp.ReasonCode)
	if resp.Properties != nil && resp.Properties.ReasonString != "" {
		reason = fmt.Sprintf("%v: %v", reason, resp.Properties.ReasonString)
	}
	if cause != nil {
		return fmt.Errorf("server refused the publication: %v (0x%02X): %w", reason, resp.ReasonCode, cause)
	}
	return fmt.Errorf("server refused the publication: %v (0x%02X)", reason, resp.ReasonCode)
}

// propertyUnset reports whether an interpolated optional property resolved to
// nothing worth sending.
//
// A metadata key that does not exist interpolates to the string "null", not to
// an empty one, and that is the ordinary case rather than a mistake: bridging
// one server to another means naming each property to carry, and most messages
// carry only some of them. Treating it as a value made a bridge fail every
// message whose source had no message expiry — permanently, because the output
// retries and the input then holds the acknowledgement behind it. A value that
// is present but malformed is still an error, because that is a configuration
// mistake worth stopping for.
func propertyUnset(v string) bool { return v == "" || v == "null" }

func (m *mqttWriterV5) properties(msg *service.Message) (*paho.PublishProperties, error) {
	props := &paho.PublishProperties{}

	if err := m.metaFilter.WalkMut(msg, func(k string, v any) error {
		props.User.Add(k, bloblang.ValueToString(v))
		return nil
	}); err != nil {
		return nil, fmt.Errorf("metadata filter error: %w", err)
	}

	if m.contentType != nil {
		v, err := m.contentType.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("content_type interpolation error: %w", err)
		}
		if !propertyUnset(v) {
			props.ContentType = v
		}
	}
	if m.responseTopic != nil {
		v, err := m.responseTopic.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("response_topic interpolation error: %w", err)
		}
		if !propertyUnset(v) {
			props.ResponseTopic = v
		}
	}
	if m.correlation != nil {
		v, err := m.correlation.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("correlation_data interpolation error: %w", err)
		}
		if !propertyUnset(v) {
			props.CorrelationData = []byte(v)
		}
	}
	if m.messageExpiry != nil {
		v, err := m.messageExpiry.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("message_expiry_interval interpolation error: %w", err)
		}
		if !propertyUnset(v) {
			secs, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("message_expiry_interval must be a whole number of seconds, got %q", v)
			}
			expiry := uint32(secs)
			props.MessageExpiry = &expiry
		}
	}
	if m.payloadFormat != nil {
		v, err := m.payloadFormat.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("payload_format_indicator interpolation error: %w", err)
		}
		switch {
		case propertyUnset(v):
		case v == "0", v == "1":
			format := v[0] - '0'
			props.PayloadFormat = &format
		default:
			return nil, fmt.Errorf("payload_format_indicator must be 0 or 1, got %q", v)
		}
	}

	return props, nil
}

func (m *mqttWriterV5) Close(ctx context.Context) error {
	return m.conn.close(ctx)
}
