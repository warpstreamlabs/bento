package azure

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azq "github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// Queue Storage Input Fields
	qsiFieldQueueName                = "queue_name"
	qsiFieldDequeueVisibilityTimeout = "dequeue_visibility_timeout"
	qsiFieldTrackProperties          = "track_properties"
	qsiFieldDeleteMessage            = "delete_message"
)

type qsiConfig struct {
	client                   *azq.ServiceClient
	QueueName                *service.InterpolatedString
	DequeueVisibilityTimeout time.Duration
	MaxInFlight              int
	TrackProperties          bool
	DeleteMessage            bool
}

func qsiConfigFromParsed(pConf *service.ParsedConfig) (conf qsiConfig, err error) {
	if conf.client, err = queueServiceClientFromParsed(pConf); err != nil {
		return
	}
	if conf.QueueName, err = pConf.FieldInterpolatedString(qsiFieldQueueName); err != nil {
		return
	}
	if conf.DequeueVisibilityTimeout, err = pConf.FieldDuration(qsiFieldDequeueVisibilityTimeout); err != nil {
		return
	}
	if conf.MaxInFlight, err = pConf.FieldMaxInFlight(); err != nil {
		return
	}
	if conf.TrackProperties, err = pConf.FieldBool(qsiFieldTrackProperties); err != nil {
		return
	}
	if conf.DeleteMessage, err = pConf.FieldBool(qsiFieldDeleteMessage); err != nil {
		return
	}
	return
}

func qsiSpec() *service.ConfigSpec {
	return azureComponentSpec(false).
		Stable().
		Summary(`Dequeue objects from an Azure Storage Queue.`).
		Description(`
This input adds the following metadata fields to each message:

`+"```"+`
- queue_storage_insertion_time
- queue_storage_queue_name
- queue_storage_message_lag (if 'track_properties' set to true)
- All user defined queue metadata
`+"```"+`

Only one authentication method is required, `+"`storage_connection_string`"+` or `+"`storage_account` and `storage_access_key`"+`. If both are set then the `+"`storage_connection_string`"+` is given priority.

### Delivery Guarantees

When a message is acknowledged it is deleted from the queue, unless `+"`delete_message`"+` is set to `+"`false`"+`. When a message is rejected (nacked) it is never deleted; it is left on the queue and becomes visible again for redelivery once its `+"`dequeue_visibility_timeout`"+` expires.`).
		Fields(
			service.NewInterpolatedStringField(qsiFieldQueueName).
				Description("The name of the source storage queue.").
				Example("foo_queue").
				Example(`${! env("MESSAGE_TYPE").lowercase() }`),
			service.NewDurationField(qsiFieldDequeueVisibilityTimeout).
				Description("The timeout duration until a dequeued message gets visible again, 30s by default").
				Advanced().
				Default("30s"),
			service.NewInputMaxInFlightField().
				Description("The maximum number of unprocessed messages to fetch at a given time.").
				Default(10).
				Advanced(),
			service.NewBoolField(qsiFieldTrackProperties).
				Description("If set to `true` the queue is polled on each read request for information such as the queue message lag. These properties are added to consumed messages as metadata, but will also have a negative performance impact.").
				Default(false).
				Advanced(),
			service.NewBoolField(qsiFieldDeleteMessage).
				Description("Whether to delete messages from the queue once they have been acknowledged. When set to `false` acknowledged messages are left on the queue and become visible again once their `dequeue_visibility_timeout` expires, which means they will be redelivered unless something else removes them. Nacked messages are never deleted, regardless of this setting.").
				Default(true).
				Advanced(),
			service.NewStringField(bscFieldStorageSASToken).Deprecated().Default(""), // This field was never implemented
		)
}

func init() {
	err := service.RegisterBatchInput("azure_queue_storage", qsiSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			pConf, err := qsiConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}
			return newAzureQueueStorage(pConf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type azureQueueStorage struct {
	conf qsiConfig
	log  *service.Logger
}

func newAzureQueueStorage(conf qsiConfig, mgr *service.Resources) (*azureQueueStorage, error) {
	a := &azureQueueStorage{
		conf: conf,
		log:  mgr.Logger(),
	}
	return a, nil
}

func (a *azureQueueStorage) Connect(ctx context.Context) error {
	return nil
}

func (a *azureQueueStorage) ReadBatch(ctx context.Context) (batch service.MessageBatch, ackFn service.AckFunc, err error) {
	var queueName string
	if queueName, err = a.conf.QueueName.TryString(service.NewMessage(nil)); err != nil {
		err = fmt.Errorf("queue name interpolation error: %w", err)
		return
	}
	queueClient := a.conf.client.NewQueueClient(queueName)
	var approxMsgCount int32
	if a.conf.TrackProperties {
		if props, err := queueClient.GetProperties(ctx, nil); err == nil {
			if amc := props.ApproximateMessagesCount; amc != nil {
				approxMsgCount = *amc
			}
		}
	}
	visibilityTimeout := int32(a.conf.DequeueVisibilityTimeout.Seconds())
	numMessages := int32(a.conf.MaxInFlight)
	dequeue, err := queueClient.DequeueMessages(ctx, &azq.DequeueMessagesOptions{
		NumberOfMessages:  &numMessages,
		VisibilityTimeout: &visibilityTimeout,
	})
	if err != nil {
		if cerr, ok := err.(*azcore.ResponseError); ok {
			if cerr.StatusCode == http.StatusNotFound {
				_, err = queueClient.Create(ctx, nil)
				return nil, nil, err
			}
			return nil, nil, fmt.Errorf("storage error message: %v", cerr)
		}
		return nil, nil, fmt.Errorf("error dequeing message: %v", err)
	}
	n := int32(len(dequeue.Messages))
	props, _ := queueClient.GetProperties(ctx, nil)
	dqm := make([]*azq.DequeuedMessage, n)
	for i, queueMsg := range dequeue.Messages {
		part := service.NewMessage([]byte(*queueMsg.MessageText))
		if queueMsg.InsertionTime != nil {
			part.MetaSetMut("queue_storage_insertion_time", queueMsg.InsertionTime.Format(time.RFC3339))
		}
		part.MetaSetMut("queue_storage_queue_name", queueName)
		if a.conf.TrackProperties {
			msgLag := 0
			if approxMsgCount >= n {
				msgLag = int(approxMsgCount - n)
			}
			part.MetaSetMut("queue_storage_message_lag", msgLag)
		}
		for k, v := range props.Metadata {
			if v != nil {
				part.MetaSetMut(k, *v)
			}
		}
		batch = append(batch, part)
		dqm[i] = queueMsg
	}
	return batch, queueAckFn(queueClient, dqm, a.conf.DeleteMessage), nil
}

type queueMessageClient interface {
	DeleteMessage(ctx context.Context, messageID, popReceipt string, o *azq.DeleteMessageOptions) (azq.DeleteMessageResponse, error)
}

// queueAckFn deletes messages on ack when deleteMessage is true. Nacked
// messages, or acked messages when deleteMessage is false, are left in place
// and become visible again once their visibility timeout expires.
func queueAckFn(client queueMessageClient, msgs []*azq.DequeuedMessage, deleteMessage bool) service.AckFunc {
	return func(ctx context.Context, res error) error {
		if res != nil || !deleteMessage {
			return nil
		}
		for _, queueMsg := range msgs {
			if _, err := client.DeleteMessage(ctx, *queueMsg.MessageID, *queueMsg.PopReceipt, nil); err != nil {
				return fmt.Errorf("error deleting message: %v", err)
			}
		}
		return nil
	}
}

func (a *azureQueueStorage) Close(ctx context.Context) error {
	return nil
}
