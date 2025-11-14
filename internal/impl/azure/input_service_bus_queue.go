package azure

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

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
	sbqFieldRenewLock          = "renew_lock"
)

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

type sbqConfig struct {
	connectionString   string
	namespace          string
	queueName          string
	maxInFlight        int
	autoAck            bool
	nackRejectPatterns []*regexp.Regexp
	renewLock          bool
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
	if conf.renewLock, err = pConf.FieldBool(sbqFieldRenewLock); err != nil {
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
		//nolint:gofmt    //Beta(). Experimental component.
		Categories("Services", "Azure").
		Version("1.13.0").
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
				Example("myservicebus.servicebus.windows.net").
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
			service.NewBoolField(sbqFieldRenewLock).
				Description("Automatically renew message locks to prevent lock expiration during processing. Useful for long-running message processing.").
				Default(true).
				Advanced(),
		)
}

type azureServiceBusQueueReader struct {
	conf *sbqConfig
	log  *service.Logger

	m        sync.RWMutex
	client   *azservicebus.Client
	receiver *azservicebus.Receiver

	messagesChan     chan *azservicebus.ReceivedMessage
	ackMessagesChan  chan *azservicebus.ReceivedMessage
	nackMessagesChan chan *azservicebus.ReceivedMessage
	closeSignal      *shutdown.Signaller

	// Lock renewal tracking
	locksMutex  sync.Mutex
	activeLocks map[string]chan struct{} // Map of lock token -> stop channel
}

func newAzureServiceBusQueueReader(conf *sbqConfig, mgr *service.Resources) (*azureServiceBusQueueReader, error) {
	return &azureServiceBusQueueReader{
		conf:             conf,
		log:              mgr.Logger(),
		messagesChan:     make(chan *azservicebus.ReceivedMessage),
		ackMessagesChan:  make(chan *azservicebus.ReceivedMessage),
		nackMessagesChan: make(chan *azservicebus.ReceivedMessage),
		closeSignal:      shutdown.NewSignaller(),
		activeLocks:      make(map[string]chan struct{}),
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

	softStopCtx, _ := a.closeSignal.SoftStopCtx(ctx)

	var wg sync.WaitGroup
	wg.Go(func() { a.readLoop(softStopCtx) })
	wg.Go(func() { a.ackLoop(softStopCtx) })
	go func() {
		wg.Wait()
		a.closeSignal.TriggerHasStopped()
	}()

	return nil
}

func (a *azureServiceBusQueueReader) readLoop(ctx context.Context) {
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.InitialInterval = 10 * time.Millisecond
	exponentialBackOff.MaxInterval = time.Minute
	exponentialBackOff.MaxElapsedTime = 0

	backoffWithContext := backoff.WithContext(exponentialBackOff, ctx)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			a.m.RLock()
			receiver := a.receiver
			a.m.RUnlock()

			if receiver == nil {
				backoffDuration := backoffWithContext.NextBackOff()
				if backoffDuration == backoff.Stop {
					return // Context was canceled
				}
				select {
				case <-time.After(backoffDuration):
				case <-ctx.Done():
					return
				}
				continue
			}

			maxMessages := int32(a.conf.maxInFlight)
			if maxMessages > 10 {
				maxMessages = 10
			}

			messages, err := receiver.ReceiveMessages(ctx, int(maxMessages), nil)
			if err != nil {
				a.log.Errorf("Failed to receive messages from Service Bus: %v", err)
				backoffDuration := backoffWithContext.NextBackOff()
				if backoffDuration == backoff.Stop {
					return // Context was canceled
				}
				select {
				case <-time.After(backoffDuration):
				case <-ctx.Done():
					return
				}
				continue
			}

			if len(messages) > 0 {
				exponentialBackOff.Reset()
				for _, msg := range messages {
					select {
					case a.messagesChan <- msg:
					case <-ctx.Done():
						return
					}
				}
			} else {
				backoffDuration := backoffWithContext.NextBackOff()
				if backoffDuration == backoff.Stop {
					return // Context was canceled
				}
				select {
				case <-time.After(backoffDuration):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (a *azureServiceBusQueueReader) ackLoop(ctx context.Context) {
	hardStopCtx, cancel := a.closeSignal.HardStopCtx(ctx)
	defer cancel()

	for {
		select {
		case msg := <-a.ackMessagesChan:
			a.m.RLock()
			receiver := a.receiver
			a.m.RUnlock()

			if receiver != nil {
				ackCtx := ctx
				select {
				case <-ctx.Done():
					ackCtx = hardStopCtx
				default:
				}
				if err := receiver.CompleteMessage(ackCtx, msg, nil); err != nil {
					// Only a log error if not due to shut down
					select {
					case <-ctx.Done():
						// Shutting down, don't log errors
					default:
						a.log.Errorf("Failed to complete message: %v", err)
					}
				}
			}
		case msg := <-a.nackMessagesChan:
			a.m.RLock()
			receiver := a.receiver
			a.m.RUnlock()

			if receiver != nil {
				// Use the soft stop context first, fall back to hard stop context if cancelled
				ackCtx := ctx
				select {
				case <-ctx.Done():
					ackCtx = hardStopCtx
				default:
				}
				if err := receiver.AbandonMessage(ackCtx, msg, nil); err != nil {
					// Only a log error if not due to shut down
					select {
					case <-ctx.Done():
						// Shutting down, don't log errors
					default:
						a.log.Errorf("Failed to abandon message: %v", err)
					}
				}
			}
		case <-ctx.Done():
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

	// Start lock renewal if enabled
	var lockStopChan chan struct{}
	if a.conf.renewLock && !a.conf.autoAck {
		lockStopChan = a.startLockRenewal(msg)
	}

	ackFunc := func(actx context.Context, res error) error {
		// Stop lock renewal when a message is acknowledged
		if lockStopChan != nil {
			a.stopLockRenewal(fmt.Sprintf("%x", msg.LockToken), lockStopChan)
		}

		if a.conf.autoAck {
			return nil
		}

		// Check if we're shutting down
		select {
		case <-a.closeSignal.SoftStopChan():
			return nil
		default:
		}

		if res == nil {
			select {
			case a.ackMessagesChan <- msg:
			case <-actx.Done():
				return actx.Err()
			case <-a.closeSignal.SoftStopChan():
			}
			return nil
		}

		errStr := res.Error()
		for _, p := range a.conf.nackRejectPatterns {
			if !p.MatchString(errStr) {
				continue
			}

			a.m.RLock()
			receiver := a.receiver
			a.m.RUnlock()
			if receiver == nil {
				return nil
			}

			if err := receiver.DeadLetterMessage(actx, msg, nil); err != nil {
				// If dead lettering fails, abandon the message to requeue it
				if abandonErr := receiver.AbandonMessage(actx, msg, nil); abandonErr != nil {
					a.log.Errorf("Failed to abandon message after dead letter failure: %v", abandonErr)
				}
				return nil
			}
			// Dead lettering succeeded - the message is now in DLQ, no further action needed
			return nil
		}

		// No pattern matched - abandon the message to requeue it
		select {
		case a.nackMessagesChan <- msg:
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
			return a.disconnect(ctx)
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closeSignal.HasStoppedChan():
	}
	return a.disconnect(ctx)
}

func (a *azureServiceBusQueueReader) disconnect(ctx context.Context) error {
	// Stop all active lock renewals first (before acquiring)
	// This prevents deadlock with lock renewal goroutines that acquire locksMutex then a.m.RLock()
	a.stopAllLockRenewals()

	a.m.Lock()
	defer a.m.Unlock()

	// Use hard stop context for closing connections to ensure they're canceled on hard stop
	ctx, cancel := a.closeSignal.HardStopCtx(ctx)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	if a.receiver != nil {
		receiver := a.receiver
		a.receiver = nil
		g.Go(func() error {
			if err := receiver.Close(ctx); err != nil {
				a.log.Errorf("Failed to close Service Bus receiver: %v", err)
				return err
			}
			return nil
		})
	}

	if a.client != nil {
		client := a.client
		a.client = nil
		g.Go(func() error {
			if err := client.Close(ctx); err != nil {
				a.log.Errorf("Failed to close Service Bus client: %v", err)
				return err
			}
			return nil
		})
	}

	// Wait for all close operations to complete or context to be canceled
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (a *azureServiceBusQueueReader) startLockRenewal(msg *azservicebus.ReceivedMessage) chan struct{} {
	lockToken := fmt.Sprintf("%x", msg.LockToken)
	stopChan := make(chan struct{})

	a.locksMutex.Lock()
	a.activeLocks[lockToken] = stopChan
	a.locksMutex.Unlock()

	go func() {
		defer func() {
			a.locksMutex.Lock()
			delete(a.activeLocks, lockToken)
			a.locksMutex.Unlock()
		}()

		a.m.RLock()
		receiver := a.receiver
		a.m.RUnlock()

		if receiver == nil {
			return
		}

		// Renew the lock every 30 seconds (assuming default lock duration is 60s)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-stopChan:
				return
			case <-a.closeSignal.SoftStopChan():
				return
			case <-ticker.C:
				func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := receiver.RenewMessageLock(ctx, msg, nil); err != nil {
						a.log.Debugf("Failed to renew lock for message %s: %v", lockToken, err)
						return
					}
					a.log.Tracef("Successfully renewed lock for message %s", lockToken)
				}()
			}
		}
	}()

	return stopChan
}

func (a *azureServiceBusQueueReader) stopLockRenewal(lockToken string, stopChan chan struct{}) {
	select {
	case <-stopChan:
		// Already stopped
	default:
		close(stopChan)
	}

	a.locksMutex.Lock()
	delete(a.activeLocks, lockToken)
	a.locksMutex.Unlock()
}

func (a *azureServiceBusQueueReader) stopAllLockRenewals() {
	a.locksMutex.Lock()
	defer a.locksMutex.Unlock()

	for _, stopChan := range a.activeLocks {
		select {
		case <-stopChan:
			// Already stopped
		default:
			close(stopChan)
		}
	}
	a.activeLocks = make(map[string]chan struct{})
}
