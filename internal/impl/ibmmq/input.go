//go:build x_bento_extra

package ibmmq

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"sync"

	mq "github.com/ibm-messaging/mq-golang/v5/ibmmq"

	"github.com/warpstreamlabs/bento/public/service"
)

//go:embed input_description.md
var inputDescription string

func ibmMQInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("1.24.0").
		Summary("Reads messages from an IBM MQ queue using native MQI.").
		Description(inputDescription).
		Fields(ibmMQConnectionHeadFields()...).
		Field(service.NewStringField(queueField).
			Description("The queue name to consume from.").
			Example("DEV.QUEUE.1")).
		Fields(ibmMQConnectionTailFields()...).
		LintRule(ibmMQConnectionLintRule)
}

func init() {
	err := service.RegisterInput("ibm_mq", ibmMQInputSpec(),
		func(config *service.ParsedConfig, serviceManager *service.Resources) (service.Input, error) {
			return ibmMQReaderFromParsed(config, serviceManager)
		})
	if err != nil {
		panic(err)
	}
}

type ibmMQReader struct {
	log *service.Logger

	connectionConfig ibmMQConnectionConfig
	queueName        string

	connectionMutex sync.Mutex
	connection      *ibmMQInputConnection

	pendingAckMutex sync.Mutex
	pendingAckDone  chan struct{}
}

type ibmMQInputConnection struct {
	queueManager mq.MQQueueManager
	queue        mq.MQObject
	buffer       []byte
}

func ibmMQReaderFromParsed(config *service.ParsedConfig, serviceManager *service.Resources) (*ibmMQReader, error) {
	reader := &ibmMQReader{log: serviceManager.Logger()}
	var err error

	if reader.connectionConfig, err = ibmMQConnectionConfigFromParsed(config); err != nil {
		return nil, err
	}

	if reader.queueName, err = config.FieldString(queueField); err != nil {
		return nil, err
	}

	return reader, nil
}

func (reader *ibmMQReader) Connect(_ context.Context) error {
	reader.connectionMutex.Lock()
	defer reader.connectionMutex.Unlock()

	if reader.connection != nil {
		return nil
	}

	queueManager, err := ibmMQConnect(reader.connectionConfig, reader.log)

	if err != nil {
		return err
	}

	queueObjectDescriptor := mq.NewMQOD()
	queueObjectDescriptor.ObjectType = mq.MQOT_Q
	queueObjectDescriptor.ObjectName = reader.queueName

	openOptions := mq.MQOO_INPUT_SHARED | mq.MQOO_FAIL_IF_QUIESCING

	queue, err := queueManager.Open(queueObjectDescriptor, openOptions)

	if err != nil {
		_ = queueManager.Disc()
		return err
	}

	reader.connection = &ibmMQInputConnection{
		queueManager: queueManager,
		queue:        queue,
		buffer:       make([]byte, 0, 4096),
	}

	return nil
}

func (reader *ibmMQReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if err := reader.waitForPendingAck(ctx); err != nil {
		return nil, nil, err
	}

	reader.connectionMutex.Lock()
	connection := reader.connection
	reader.connectionMutex.Unlock()

	if connection == nil {
		return nil, nil, service.ErrNotConnected
	}

	var (
		payload []byte
		err     error
	)

	for {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		getMessageOptions := mq.NewMQGMO()
		getMessageOptions.Version = mq.MQGMO_VERSION_3
		getMessageOptions.Options |= mq.MQGMO_SYNCPOINT | mq.MQGMO_WAIT | mq.MQGMO_NO_PROPERTIES | mq.MQGMO_FAIL_IF_QUIESCING
		getMessageOptions.WaitInterval = 1000

		var messageLength int
		payload, messageLength, err = connection.queue.GetSlice(mq.NewMQMD(), getMessageOptions, connection.buffer)

		if ibmMQReasonCode(err) == mq.MQRC_TRUNCATED_MSG_FAILED {
			payload, err = connection.getByToken(getMessageOptions.MsgToken, messageLength)
			if ibmMQReasonCode(err) == mq.MQRC_DATA_LENGTH_ERROR {
				err = fmt.Errorf("message of %d bytes is too large for channel: %w", messageLength, err)
			}
		}

		if err == nil {
			break
		}

		if ibmMQReasonCode(err) == mq.MQRC_NO_MSG_AVAILABLE {
			continue
		}

		if ctx.Err() == nil {
			_ = reader.disconnect()
			return nil, nil, err
		}

		return nil, nil, ctx.Err()
	}

	if ctx.Err() != nil {
		_ = connection.queueManager.Back()
		return nil, nil, ctx.Err()
	}

	return service.NewMessage(bytes.Clone(payload)), reader.newAckFunc(connection.queueManager), nil
}

func (reader *ibmMQReader) newAckFunc(queueManager mq.MQQueueManager) service.AckFunc {
	pendingAckDone := make(chan struct{})

	reader.pendingAckMutex.Lock()
	reader.pendingAckDone = pendingAckDone
	reader.pendingAckMutex.Unlock()

	var once sync.Once

	return func(_ context.Context, err error) error {
		var ackErr error

		once.Do(func() {
			if err != nil {
				ackErr = queueManager.Back()
			} else {
				ackErr = queueManager.Cmit()
			}

			reader.pendingAckMutex.Lock()
			reader.pendingAckDone = nil
			reader.pendingAckMutex.Unlock()

			close(pendingAckDone)
		})

		return ackErr
	}
}

func (reader *ibmMQReader) Close(context.Context) error {
	return reader.disconnect()
}

func (reader *ibmMQReader) disconnect() error {
	reader.connectionMutex.Lock()
	defer reader.connectionMutex.Unlock()

	if reader.connection == nil {
		return nil
	}

	if err := reader.connection.queueManager.Back(); err != nil {
		reader.log.Errorf("Failed to back out IBM MQ input unit of work: %v", err)
	}

	if err := reader.connection.queue.Close(0); err != nil {
		reader.log.Errorf("Failed to close IBM MQ input queue: %v", err)
	}

	if err := reader.connection.queueManager.Disc(); err != nil {
		reader.log.Errorf("Failed to disconnect IBM MQ input queue manager: %v", err)
	}

	reader.connection = nil

	return nil
}

func (reader *ibmMQReader) waitForPendingAck(ctx context.Context) error {
	reader.pendingAckMutex.Lock()
	pendingAckDone := reader.pendingAckDone
	reader.pendingAckMutex.Unlock()

	if pendingAckDone == nil {
		return nil
	}

	select {
	case <-pendingAckDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// getByToken gets the message that did not fit in the buffer, matched by its
// message token, and grows the buffer to fit it.
func (connection *ibmMQInputConnection) getByToken(token []byte, length int) ([]byte, error) {
	getMessageOptions := mq.NewMQGMO()
	getMessageOptions.Version = mq.MQGMO_VERSION_3
	getMessageOptions.Options |= mq.MQGMO_SYNCPOINT | mq.MQGMO_NO_PROPERTIES | mq.MQGMO_FAIL_IF_QUIESCING
	getMessageOptions.MatchOptions = mq.MQMO_MATCH_MSG_TOKEN
	getMessageOptions.MsgToken = token

	buffer := make([]byte, 0, length)

	payload, _, err := connection.queue.GetSlice(mq.NewMQMD(), getMessageOptions, buffer)

	if err == nil {
		connection.buffer = buffer
	}

	return payload, err
}
