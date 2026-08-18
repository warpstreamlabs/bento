package aws

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

type mockSqsInput struct {
	sqsAPI

	mtx          chan struct{}
	queueTimeout int32
	messages     []types.Message
	mesTimeouts  map[string]int32

	getQueueAttributesErr error
}

func (m *mockSqsInput) do(fn func()) {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()
	fn()
}

func (m *mockSqsInput) TimeoutLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			<-m.mtx

			for mesID, timeout := range m.mesTimeouts {
				timeout = timeout - 1
				m.mesTimeouts[mesID] = max(timeout, 0)
			}

			m.mtx <- struct{}{}
		case <-ctx.Done():
			return
		}
	}
}

func (m *mockSqsInput) ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	messages := make([]types.Message, 0, len(m.messages))

	for _, message := range m.messages {
		if timeout, found := m.mesTimeouts[*message.MessageId]; !found || timeout == 0 {
			messages = append(messages, message)
			m.mesTimeouts[*message.MessageId] = m.queueTimeout
		}
	}

	return &sqs.ReceiveMessageOutput{Messages: messages}, nil
}

func (m *mockSqsInput) GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	if m.getQueueAttributesErr != nil {
		return nil, m.getQueueAttributesErr
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{sqsiAttributeNameVisibilityTimeout: strconv.Itoa(int(m.queueTimeout))}}, nil
}

func (m *mockSqsInput) ChangeMessageVisibilityBatch(ctx context.Context, input *sqs.ChangeMessageVisibilityBatchInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	for _, entry := range input.Entries {
		if _, found := m.mesTimeouts[*entry.Id]; found {
			m.mesTimeouts[*entry.Id] = entry.VisibilityTimeout
		} else {
			panic("nope")
		}
	}

	return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
}

func (m *mockSqsInput) DeleteMessageBatch(ctx context.Context, input *sqs.DeleteMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	for _, entry := range input.Entries {
		delete(m.mesTimeouts, *entry.Id)
		for i, message := range m.messages {
			if *entry.Id == *message.MessageId {
				m.messages = append(m.messages[:i], m.messages[i+1:]...)
			}
		}
	}

	return &sqs.DeleteMessageBatchOutput{}, nil
}

func TestSQSInput(t *testing.T) {
	tCtx := context.Background()
	defer tCtx.Done()

	messages := []types.Message{
		{
			Body:          aws.String("message-1"),
			MessageId:     aws.String("message-1"),
			ReceiptHandle: aws.String("message-1"),
		},
		{
			Body:          aws.String("message-2"),
			MessageId:     aws.String("message-2"),
			ReceiptHandle: aws.String("message-2"),
		},
		{
			Body:          aws.String("message-3"),
			MessageId:     aws.String("message-3"),
			ReceiptHandle: aws.String("message-3"),
		},
	}
	expectedMessages := len(messages)

	conf, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)

	r, err := newAWSSQSReader(
		sqsiConfig{
			URL:                 "http://foo.example.com",
			WaitTimeSeconds:     0,
			DeleteMessage:       true,
			ResetVisibility:     true,
			UpdateVisibility:    true,
			MaxNumberOfMessages: 10,
		},
		conf,
		nil,
	)
	require.NoError(t, err)

	mockInput := &mockSqsInput{
		mtx:          make(chan struct{}, 1),
		queueTimeout: 10,
		messages:     messages,
		mesTimeouts:  make(map[string]int32, expectedMessages),
	}
	mockInput.mtx <- struct{}{}
	r.sqs = mockInput
	go mockInput.TimeoutLoop(tCtx)

	defer r.closeSignal.TriggerHardStop()
	err = r.Connect(tCtx)
	require.NoError(t, err)

	receivedMessages := make([]types.Message, 0, expectedMessages)

	// Check that all messages are received from the reader
	require.Eventually(t, func() bool {
	out:
		for {
			select {
			case mes := <-r.messagesChan:
				receivedMessages = append(receivedMessages, mes)
			default:
				break out
			}
		}
		return len(receivedMessages) == expectedMessages
	}, 30*time.Second, time.Second)

	// Wait over the defined queue timeout and check that messages have not been received again
	time.Sleep(time.Duration(mockInput.queueTimeout+5) * time.Second)
	select {
	case <-r.messagesChan:
		require.Fail(t, "messages have been received again due to timeouts")
	default:
	}
	// Check that even if they are not visible, messages haven't been deleted from the queue
	mockInput.do(func() {
		require.Len(t, mockInput.messages, expectedMessages)
		require.Len(t, mockInput.mesTimeouts, expectedMessages)
	})

	// Ack all messages and ensure that they are deleted from SQS
	for _, message := range receivedMessages {
		r.ackMessagesChan <- sqsMessageHandle{id: *message.MessageId, receiptHandle: *message.ReceiptHandle}
	}

	require.Eventually(t, func() bool {
		msgsLen := 0
		mockInput.do(func() {
			msgsLen = len(mockInput.messages)
		})
		return msgsLen == 0
	}, 5*time.Second, time.Second)
}

func TestSQSInputBatchAck(t *testing.T) {
	tCtx := context.Background()
	defer tCtx.Done()

	messages := []types.Message{}
	for i := range 101 {
		messages = append(messages, types.Message{
			Body:          aws.String(fmt.Sprintf("message-%v", i)),
			MessageId:     aws.String(fmt.Sprintf("id-%v", i)),
			ReceiptHandle: aws.String(fmt.Sprintf("h-%v", i)),
		})
	}
	expectedMessages := len(messages)

	conf, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)

	r, err := newAWSSQSReader(
		sqsiConfig{
			URL:                 "http://foo.example.com",
			WaitTimeSeconds:     0,
			DeleteMessage:       true,
			ResetVisibility:     true,
			UpdateVisibility:    true,
			MaxNumberOfMessages: 10,
		},
		conf,
		nil,
	)
	require.NoError(t, err)

	mockInput := &mockSqsInput{
		mtx:          make(chan struct{}, 1),
		queueTimeout: 10,
		messages:     messages,
		mesTimeouts:  make(map[string]int32, expectedMessages),
	}
	mockInput.mtx <- struct{}{}
	r.sqs = mockInput
	go mockInput.TimeoutLoop(tCtx)

	defer r.closeSignal.TriggerHardStop()
	err = r.Connect(tCtx)
	require.NoError(t, err)

	receivedMessageAcks := map[string]service.AckFunc{}

	for _, eMsg := range messages {
		m, aFn, err := r.Read(tCtx)
		require.NoError(t, err)

		mBytes, err := m.AsBytes()
		require.NoError(t, err)

		assert.Equal(t, *eMsg.Body, string(mBytes))
		receivedMessageAcks[string(mBytes)] = aFn
	}

	// Check that messages haven't been deleted from the queue
	mockInput.do(func() {
		require.Len(t, mockInput.messages, expectedMessages)
		require.Len(t, mockInput.mesTimeouts, expectedMessages)
	})

	// Ack all messages as a batch
	for _, aFn := range receivedMessageAcks {
		require.NoError(t, aFn(tCtx, err))
	}

	require.Eventually(t, func() bool {
		msgsLen := 0
		mockInput.do(func() {
			msgsLen = len(mockInput.messages)
		})
		return msgsLen == 0
	}, 5*time.Second, time.Second)
}

func TestSQSInputVisibilityTimeoutFollowsQueue(t *testing.T) {
	tests := []struct {
		name         string
		queueTimeout int32
		attributeErr error
		expected     int
	}{
		{
			name:         "takes the timeout the queue is configured with",
			queueTimeout: 600,
			expected:     600,
		},
		{
			name:         "falls back when the queue cannot be read",
			attributeErr: errors.New("access denied"),
			expected:     defaultVisibilityTimeoutSeconds,
		},
		{
			name:         "falls back when the queue reports zero",
			queueTimeout: 0,
			expected:     defaultVisibilityTimeoutSeconds,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, err := newAWSSQSReader(sqsiConfig{URL: "http://foo.example.com"}, aws.Config{}, nil)
			require.NoError(t, err)
			r.sqs = &mockSqsInput{
				queueTimeout:          test.queueTimeout,
				getQueueAttributesErr: test.attributeErr,
			}

			timeout := r.queueVisibilityTimeout(t.Context())
			assert.Equal(t, test.expected, timeout)

			// The timeout read at connect time is what in-flight handles get
			// refreshed with.
			tracker := &sqsInFlightTracker{
				handles:                  map[string]sqsInFlightHandle{},
				visibilityTimeoutSeconds: timeout,
			}
			tracker.AddNew(types.Message{
				MessageId:     aws.String("message-1"),
				ReceiptHandle: aws.String("message-1"),
			})
			assert.Equal(t, test.expected, tracker.handles["message-1"].timeoutSeconds)
		})
	}
}
