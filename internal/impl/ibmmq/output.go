//go:build x_bento_extra

package ibmmq

import (
	"context"
	_ "embed"
	"fmt"
	"sync"

	mq "github.com/ibm-messaging/mq-golang/v5/ibmmq"

	"github.com/warpstreamlabs/bento/public/service"
)

//go:embed output_description.md
var outputDescription string

func ibmMQOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("1.24.0").
		Summary("Sends messages to an IBM MQ queue using native MQI.").
		Description(outputDescription).
		Fields(ibmMQConnectionHeadFields()...).
		Field(service.NewStringField(queueField).
			Description("The queue name to publish to.").
			Example("DEV.QUEUE.1")).
		Fields(ibmMQConnectionTailFields()...).
		LintRule(ibmMQConnectionLintRule)
}

func init() {
	err := service.RegisterOutput("ibm_mq", ibmMQOutputSpec(),
		func(config *service.ParsedConfig, serviceManager *service.Resources) (service.Output, int, error) {
			writer, err := ibmMQWriterFromParsed(config, serviceManager)
			if err != nil {
				return nil, 0, err
			}
			return writer, 1, nil
		})
	if err != nil {
		panic(err)
	}
}

type ibmMQWriter struct {
	log *service.Logger

	connectionConfig ibmMQConnectionConfig
	queueName        string

	connectionMutex sync.Mutex
	connection      *ibmMQOutputConnection
}

type ibmMQOutputConnection struct {
	queueManager mq.MQQueueManager
	queue        mq.MQObject
}

func ibmMQWriterFromParsed(config *service.ParsedConfig, serviceManager *service.Resources) (*ibmMQWriter, error) {
	writer := &ibmMQWriter{log: serviceManager.Logger()}

	var err error

	if writer.connectionConfig, err = ibmMQConnectionConfigFromParsed(config); err != nil {
		return nil, err
	}

	if writer.queueName, err = config.FieldString(queueField); err != nil {
		return nil, err
	}

	return writer, nil
}

func (writer *ibmMQWriter) Connect(_ context.Context) error {
	writer.connectionMutex.Lock()
	defer writer.connectionMutex.Unlock()

	if writer.connection != nil {
		return nil
	}

	queueManager, err := ibmMQConnect(writer.connectionConfig, writer.log)

	if err != nil {
		return err
	}

	queueObjectDescriptor := mq.NewMQOD()
	queueObjectDescriptor.ObjectType = mq.MQOT_Q
	queueObjectDescriptor.ObjectName = writer.queueName

	openOptions := mq.MQOO_OUTPUT | mq.MQOO_FAIL_IF_QUIESCING

	queue, err := queueManager.Open(queueObjectDescriptor, openOptions)

	if err != nil {
		_ = queueManager.Disc()
		return err
	}

	writer.connection = &ibmMQOutputConnection{
		queueManager: queueManager,
		queue:        queue,
	}

	return nil
}

func (writer *ibmMQWriter) Write(ctx context.Context, message *service.Message) error {
	payload, err := message.AsBytes()

	if err != nil {
		return err
	}

	messageDescriptor := mq.NewMQMD()
	putMessageOptions := mq.NewMQPMO()
	putMessageOptions.Options = mq.MQPMO_NO_SYNCPOINT | mq.MQPMO_FAIL_IF_QUIESCING

	writer.connectionMutex.Lock()

	connection := writer.connection

	if connection == nil {
		writer.connectionMutex.Unlock()
		return service.ErrNotConnected
	}

	err = connection.queue.Put(messageDescriptor, putMessageOptions, payload)

	writer.connectionMutex.Unlock()

	if err != nil {
		switch ibmMQReasonCode(err) {
		case mq.MQRC_DATA_LENGTH_ERROR:
			return fmt.Errorf("message of %d bytes is too large for channel: %w", len(payload), err)
		case mq.MQRC_MSG_TOO_BIG_FOR_Q:
			return fmt.Errorf("message of %d bytes is too large for queue: %w", len(payload), err)
		case mq.MQRC_MSG_TOO_BIG_FOR_Q_MGR:
			return fmt.Errorf("message of %d bytes is too large for queue manager: %w", len(payload), err)
		case mq.MQRC_Q_FULL:
			return fmt.Errorf("queue is full: %w", err)
		case mq.MQRC_Q_SPACE_NOT_AVAILABLE:
			return fmt.Errorf("queue space not available: %w", err)
		case mq.MQRC_PUT_INHIBITED:
			return fmt.Errorf("put inhibited for queue: %w", err)
		}
		if ctx.Err() == nil {
			_ = writer.Close(ctx)
		}
		return err
	}

	return nil
}

func (writer *ibmMQWriter) Close(context.Context) error {
	writer.connectionMutex.Lock()
	defer writer.connectionMutex.Unlock()

	if writer.connection == nil {
		return nil
	}

	if err := writer.connection.queue.Close(0); err != nil {
		writer.log.Errorf("Failed to close IBM MQ output queue: %v", err)
	}

	if err := writer.connection.queueManager.Disc(); err != nil {
		writer.log.Errorf("Failed to disconnect IBM MQ output queue manager: %v", err)
	}

	writer.connection = nil

	return nil
}
