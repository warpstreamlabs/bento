package azure

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/Jeffail/shutdown"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	sbqFieldConnectionString   = "connection_string"
	sbqFieldNamespace          = "namespace"
	sbqFieldQueueName          = "queue"
	sbqFieldAutoAck            = "auto_ack"
	sbqFieldNackRejectPatterns = "nack_reject_patterns"
)

type sbqConfig struct {
	connectionString   string
	namespace          string
	queueName          string
	maxInFlight        int
	autoAck            bool
	nackRejectPatterns []*regexp.Regexp
}

func sbqConfigFromParsed(pConf *service.ParsedConfig) (*sbqConfig, error) {
	conf := &sbqConfig{}

	var err error
	if conf.connectionString, err = pConf.FieldString(sbqFieldConnectionString); err != nil {
		return nil, err
	}
	if conf.namespace, err = pConf.FieldString(sbqFieldNamespace); err != nil {
		return nil, err
	}
	if conf.queueName, err = pConf.FieldString(sbqFieldQueueName); err != nil {
		return nil, err
	}
	if conf.maxInFlight, err = pConf.FieldMaxInFlight(); err != nil {
		return nil, err
	}
	if conf.autoAck, err = pConf.FieldBool(sbqFieldAutoAck); err != nil {
		return nil, err
	}
	if pConf.Contains(sbqFieldNackRejectPatterns) {
		nackPatternStrs, err := pConf.FieldStringList(sbqFieldNackRejectPatterns)
		if err != nil {
			return nil, err
		}
		for _, p := range nackPatternStrs {
			r, err := regexp.Compile(p)
			if err != nil {
				return nil, fmt.Errorf("failed to compile nack reject pattern: %w", err)
			}
			conf.nackRejectPatterns = append(conf.nackRejectPatterns, r)
		}
	}

	return conf, nil
}

func sbqSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services", "Azure").
		Version("1.0.0").
		Summary(`Consume messages from an Azure Service Bus Queue.`).
		Description(`
Consume messages from an Azure Service Bus Queue using AMQP 1.0 protocol.

### Authentication

Either `+"`connection_string`"+` or `+"`namespace`"+` must be provided. When using `+"`namespace`"+`, the default Azure credentials will be used.

### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- service_bus_message_id
- service_bus_sequence_number
- service_bus_enqueued_time
- service_bus_delivery_count
- service_bus_locked_until
- service_bus_lock_token
- service_bus_expires_at
- service_bus_content_type
- service_bus_correlation_id
- service_bus_reply_to
- service_bus_reply_to_session_id
- service_bus_session_id
- service_bus_to
- service_bus_user_properties (flattened)
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringField(sbqFieldConnectionString).
				Description("The Service Bus connection string. This can be obtained from the Azure portal. If not provided, namespace and default credentials will be used.").
				Example("Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=...").
				Default(""),
			service.NewStringField(sbqFieldNamespace).
				Description("The Service Bus namespace. Required when connection_string is not provided.").
				Default(""),
			service.NewStringField(sbqFieldQueueName).
				Description("The name of the Service Bus queue to consume from."),
			service.NewInputMaxInFlightField().
				Description("The maximum number of unprocessed messages to fetch at a given time.").
				Default(10),
			service.NewBoolField(sbqFieldAutoAck).
				Description("Whether to automatically acknowledge messages as they are consumed rather than waiting for acknowledgments from downstream. This can improve throughput but at the cost of eliminating delivery guarantees.").
				Default(false).
				Advanced(),
			service.NewStringListField(sbqFieldNackRejectPatterns).
				Description("A list of regular expression patterns whereby if a message that has failed to be delivered by Bento has an error that matches it will be nacked (or sent to dead letter queue if configured). By default failed messages are nacked with requeue enabled.").
				Example([]string{"^reject me please:.+$"}).
				Advanced().
				Default([]any{}),
		)
}

type azureServiceBusQueueReader struct {
	conf *sbqConfig
	log  *service.Logger

	client   *azservicebus.Client
	receiver *azservicebus.Receiver

	messagesChan     chan *azservicebus.ReceivedMessage
	ackMessagesChan  chan *azservicebus.ReceivedMessage
	nackMessagesChan chan *azservicebus.ReceivedMessage
	closeSignal      *shutdown.Signaller

	m sync.RWMutex
}

func newAzureServiceBusQueueReader(conf *sbqConfig, mgr *service.Resources) (*azureServiceBusQueueReader, error) {
	return &azureServiceBusQueueReader{
		conf:             conf,
		log:              mgr.Logger(),
		messagesChan:     make(chan *azservicebus.ReceivedMessage),
		ackMessagesChan:  make(chan *azservicebus.ReceivedMessage),
		nackMessagesChan: make(chan *azservicebus.ReceivedMessage),
		closeSignal:      shutdown.NewSignaller(),
	}, nil
}

func (a *azureServiceBusQueueReader) Connect(ctx context.Context) error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.client != nil {
		return nil
	}

	var err error
	if a.conf.connectionString != "" {
		a.client, err = azservicebus.NewClientFromConnectionString(a.conf.connectionString, nil)
		if err != nil {
			return fmt.Errorf("failed to create Service Bus client from connection string: %w", err)
		}
	} else if a.conf.namespace != "" {
		cred, credErr := azidentity.NewDefaultAzureCredential(nil)
		if credErr != nil {
			return fmt.Errorf("failed to get default Azure credentials: %w", credErr)
		}
		a.client, err = azservicebus.NewClient(a.conf.namespace, cred, nil)
		if err != nil {
			return fmt.Errorf("failed to create Service Bus client with default credentials: %w", err)
		}
	} else {
		return errors.New("either connection_string or namespace must be provided")
	}

	receiverOptions := &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	}

	a.receiver, err = a.client.NewReceiverForQueue(a.conf.queueName, receiverOptions)
	if err != nil {
		return fmt.Errorf("failed to create Service Bus receiver: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go a.readLoop(&wg)
	go a.ackLoop(&wg)
	go func() {
		wg.Wait()
		a.closeSignal.TriggerHasStopped()
	}()

	return nil
}

func (a *azureServiceBusQueueReader) readLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	closeAtLeisureCtx, done := a.closeSignal.SoftStopCtx(context.Background())
	defer done()

	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.InitialInterval = 10 * time.Millisecond
	exponentialBackOff.MaxInterval = time.Minute
	exponentialBackOff.MaxElapsedTime = 0

	for {
		select {
		case <-a.closeSignal.SoftStopChan():
			return
		default:
			a.m.RLock()
			receiver := a.receiver
			a.m.RUnlock()

			if receiver == nil {
				select {
				case <-time.After(exponentialBackOff.NextBackOff()):
				case <-a.closeSignal.SoftStopChan():
					return
				}
				continue
			}

			maxMessages := int32(a.conf.maxInFlight)
			if maxMessages > 10 {
				maxMessages = 10
			}

			messages, err := receiver.ReceiveMessages(closeAtLeisureCtx, int(maxMessages), nil)
			if err != nil {
				a.log.Errorf("Failed to receive messages from Service Bus: %v", err)
				select {
				case <-time.After(exponentialBackOff.NextBackOff()):
				case <-a.closeSignal.SoftStopChan():
					return
				}
				continue
			}

			if len(messages) > 0 {
				exponentialBackOff.Reset()
				for _, msg := range messages {
					select {
					case a.messagesChan <- msg:
					case <-a.closeSignal.SoftStopChan():
						return
					}
				}
			} else {
				select {
				case <-time.After(exponentialBackOff.NextBackOff()):
				case <-a.closeSignal.SoftStopChan():
					return
				}
			}
		}
	}
}

func (a *azureServiceBusQueueReader) ackLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	closeNowCtx, done := a.closeSignal.HardStopCtx(context.Background())
	defer done()

	for {
		select {
		case msg := <-a.ackMessagesChan:
			a.m.RLock()
			receiver := a.receiver
			a.m.RUnlock()

			if receiver != nil {
				if err := receiver.CompleteMessage(closeNowCtx, msg, nil); err != nil {
					a.log.Errorf("Failed to complete message: %v", err)
				}
			}
		case msg := <-a.nackMessagesChan:
			a.m.RLock()
			receiver := a.receiver
			a.m.RUnlock()

			if receiver != nil {
				if err := receiver.AbandonMessage(closeNowCtx, msg, nil); err != nil {
					a.log.Errorf("Failed to abandon message: %v", err)
				}
			}
		case <-a.closeSignal.SoftStopChan():
			return
		}
	}
}

func (a *azureServiceBusQueueReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	a.m.RLock()
	if a.receiver == nil {
		a.m.RUnlock()
		return nil, nil, service.ErrNotConnected
	}
	a.m.RUnlock()

	var msg *azservicebus.ReceivedMessage
	select {
	case msg = <-a.messagesChan:
	case <-a.closeSignal.SoftStopChan():
		return nil, nil, service.ErrEndOfInput
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	part := service.NewMessage(msg.Body)

	// Set metadata
	part.MetaSetMut("service_bus_message_id", msg.MessageID)
	part.MetaSetMut("service_bus_sequence_number", msg.SequenceNumber)
	if !msg.EnqueuedTime.IsZero() {
		part.MetaSetMut("service_bus_enqueued_time", msg.EnqueuedTime.Format(time.RFC3339))
	}
	part.MetaSetMut("service_bus_delivery_count", msg.DeliveryCount)
	if !msg.LockedUntil.IsZero() {
		part.MetaSetMut("service_bus_locked_until", msg.LockedUntil.Format(time.RFC3339))
	}
	part.MetaSetMut("service_bus_lock_token", fmt.Sprintf("%x", msg.LockToken))
	if !msg.ExpiresAt.IsZero() {
		part.MetaSetMut("service_bus_expires_at", msg.ExpiresAt.Format(time.RFC3339))
	}
	if msg.ContentType != nil {
		part.MetaSetMut("service_bus_content_type", *msg.ContentType)
	}
	if msg.CorrelationID != nil {
		part.MetaSetMut("service_bus_correlation_id", *msg.CorrelationID)
	}
	if msg.ReplyTo != nil {
		part.MetaSetMut("service_bus_reply_to", *msg.ReplyTo)
	}
	if msg.ReplyToSessionID != nil {
		part.MetaSetMut("service_bus_reply_to_session_id", *msg.ReplyToSessionID)
	}
	if msg.SessionID != nil {
		part.MetaSetMut("service_bus_session_id", *msg.SessionID)
	}
	if msg.To != nil {
		part.MetaSetMut("service_bus_to", *msg.To)
	}

	// Flatten user properties
	for k, v := range msg.ApplicationProperties {
		part.MetaSetMut("service_bus_"+k, v)
	}

	ackFunc := func(actx context.Context, res error) error {
		if a.conf.autoAck {
			return nil
		}

		if res != nil {
			errStr := res.Error()
			for _, p := range a.conf.nackRejectPatterns {
				if p.MatchString(errStr) {
					a.m.RLock()
					receiver := a.receiver
					a.m.RUnlock()
					if receiver != nil {
						return receiver.DeadLetterMessage(actx, msg, nil)
					}
					select {
					case a.nackMessagesChan <- msg:
					case <-actx.Done():
						return actx.Err()
					case <-a.closeSignal.SoftStopChan():
					}
					return nil
				}
			}
			select {
			case a.nackMessagesChan <- msg:
			case <-actx.Done():
				return actx.Err()
			case <-a.closeSignal.SoftStopChan():
			}
			return nil
		}

		select {
		case a.ackMessagesChan <- msg:
		case <-actx.Done():
			return actx.Err()
		case <-a.closeSignal.SoftStopChan():
		}
		return nil
	}

	return part, ackFunc, nil
}

func (a *azureServiceBusQueueReader) Close(ctx context.Context) error {
	a.closeSignal.TriggerSoftStop()

	var closeNowAt time.Duration
	if dline, ok := ctx.Deadline(); ok {
		if closeNowAt = time.Until(dline) - time.Second; closeNowAt <= 0 {
			a.closeSignal.TriggerHardStop()
		}
	}
	if closeNowAt > 0 {
		select {
		case <-time.After(closeNowAt):
			a.closeSignal.TriggerHardStop()
		case <-ctx.Done():
			return ctx.Err()
		case <-a.closeSignal.HasStoppedChan():
			return a.disconnect()
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closeSignal.HasStoppedChan():
	}
	return a.disconnect()
}

func (a *azureServiceBusQueueReader) disconnect() error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.receiver != nil {
		if err := a.receiver.Close(context.Background()); err != nil {
			a.log.Errorf("Failed to close Service Bus receiver: %v", err)
		}
		a.receiver = nil
	}
	if a.client != nil {
		if err := a.client.Close(context.Background()); err != nil {
			a.log.Errorf("Failed to close Service Bus client: %v", err)
		}
		a.client = nil
	}
	return nil
}

func init() {
	err := service.RegisterInput("azure_service_bus_queue", sbqSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			pConf, err := sbqConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}
			return newAzureServiceBusQueueReader(pConf, mgr)
		})
	if err != nil {
		panic(err)
	}
}
