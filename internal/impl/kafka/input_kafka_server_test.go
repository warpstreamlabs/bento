package kafka

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"bytes"
	"io"
	"net"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	xdgscram "github.com/xdg-go/scram"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestKafkaServerInputConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "valid minimal config",
			config: `
address: "127.0.0.1:19092"
`,
			wantErr: false,
		},
		{
			name: "valid config with topics",
			config: `
address: "127.0.0.1:19092"
topics:
  - test-topic
  - events
`,
			wantErr: false,
		},
		{
			name: "valid config with timeout",
			config: `
address: "127.0.0.1:19092"
timeout: "10s"
max_message_bytes: 2097152
`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := kafkaServerInputConfig()
			env := service.NewEnvironment()

			parsed, err := spec.ParseYAML(tt.config, env)
			require.NoError(t, err)

			_, err = newKafkaServerInputFromConfig(parsed, service.MockResources())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestKafkaServerInputBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19092"
timeout: "5s"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	// Connect the input
	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	// Give the server a moment to start listening
	time.Sleep(100 * time.Millisecond)

	// Create a franz-go producer client
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19092"),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce a test message
	testTopic := "test-topic"
	testKey := "test-key"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Key:   []byte(testKey),
		Value: []byte(testValue),
		Headers: []kgo.RecordHeader{
			{Key: "header1", Value: []byte("value1")},
		},
	}

	// Send message asynchronously and wait for acknowledgment
	produceChan := make(chan error, 1)
	t.Logf("Starting producer goroutine")
	go func() {
		t.Logf("Producer: before ProduceSync")
		results := client.ProduceSync(ctx, record)
		t.Logf("Producer: after ProduceSync, results len=%d err=%v", len(results), results[0].Err)
		if len(results) > 0 {
			produceChan <- results[0].Err
		} else {
			produceChan <- fmt.Errorf("no results")
		}
	}()

	// Read message from input
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]

	// Verify message content
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testValue, string(msgBytes))

	// Verify metadata
	topic, exists := msg.MetaGet("kafka_server_topic")
	assert.True(t, exists)
	assert.Equal(t, testTopic, topic)

	key, exists := msg.MetaGet("kafka_server_key")
	assert.True(t, exists)
	assert.Equal(t, testKey, key)

	partitionAny, exists := msg.MetaGetMut("kafka_server_partition")
	assert.True(t, exists)
	t.Logf("Got partition: value=%v, type=%T", partitionAny, partitionAny)
	partition, ok := partitionAny.(int)
	assert.True(t, ok, "partition should be int")
	assert.Equal(t, 0, partition)

	// Verify header
	header1, exists := msg.MetaGet("header1")
	assert.True(t, exists)
	assert.Equal(t, "value1", header1)

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputMultipleMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19093"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create producer
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19093"),
		kgo.ProducerBatchCompression(kgo.NoCompression()), // Disable compression for testing
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce multiple messages
	numMessages := 5
	testTopic := "multi-test"

	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: testTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		go func() {
			client.Produce(ctx, record, func(r *kgo.Record, err error) {
				// Callback - could check errors here
			})
		}()
	}

	// Read messages
	receivedCount := 0
	timeout := time.After(10 * time.Second)

	for receivedCount < numMessages {
		select {
		case <-timeout:
			t.Fatalf("Timeout: only received %d of %d messages", receivedCount, numMessages)
		default:
			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			batch, ackFn, err := input.ReadBatch(readCtx)
			readCancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					continue
				}
				t.Fatalf("Failed to read batch: %v", err)
			}

			receivedCount += len(batch)

			// Acknowledge
			err = ackFn(ctx, nil)
			require.NoError(t, err)
		}
	}

	assert.Equal(t, numMessages, receivedCount)
}

func TestKafkaServerInputTopicFiltering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with topic filtering
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19094"
topics:
  - allowed-topic
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create producer
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19094"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Try to produce to allowed topic
	allowedRecord := &kgo.Record{
		Topic: "allowed-topic",
		Value: []byte("allowed"),
	}

	var allowedErr error
	go func() {
		results := client.ProduceSync(ctx, allowedRecord)
		if len(results) > 0 {
			allowedErr = results[0].Err
		}
	}()

	// Read the allowed message
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	batch, ackFn, err := input.ReadBatch(readCtx)
	readCancel()

	require.NoError(t, err)
	require.Len(t, batch, 1)

	msgBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "allowed", string(msgBytes))

	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify no error from producer
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, allowedErr)

	// Try to produce to disallowed topic - the server should reject it silently
	disallowedRecord := &kgo.Record{
		Topic: "disallowed-topic",
		Value: []byte("disallowed"),
	}

	go func() {
		client.Produce(ctx, disallowedRecord, func(r *kgo.Record, err error) {
			// Callback
		})
	}()

	// Try to read - should timeout since message was filtered
	readCtx2, readCancel2 := context.WithTimeout(ctx, 2*time.Second)
	_, _, err = input.ReadBatch(readCtx2)
	readCancel2()

	// Should timeout since no message should be received
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestKafkaServerInputClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19095"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Close should not error
	err = input.Close(ctx)
	assert.NoError(t, err)

	// Reading after close should return error
	_, _, err = input.ReadBatch(ctx)
	assert.Error(t, err)
}

func TestKafkaServerInputNullKeyAndValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19096"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19096"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce message with null value (tombstone)
	record := &kgo.Record{
		Topic: "test-topic",
		Key:   []byte("key-with-null-value"),
		Value: nil,
	}

	go func() {
		client.ProduceSync(ctx, record)
	}()

	// Read message
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Empty(t, msgBytes) // Null value should be empty

	// Key should still be present
	key, exists := msg.MetaGet("kafka_server_key")
	assert.True(t, exists)
	assert.Equal(t, "key-with-null-value", key)

	err = ackFn(ctx, nil)
	require.NoError(t, err)
}

// Helper that creates a kafkaServerInput with an SCRAM-SHA-256 user for tests.
func makeSCRAMTestServer(t *testing.T, username, password string) *kafkaServerInput {
	k := &kafkaServerInput{
		logger:                  service.MockResources().Logger(),
		saslSCRAM256Credentials: make(map[string]xdgscram.StoredCredentials),
		saslMechanisms:          []string{saslMechanismScramSha256},
		saslEnabled:             true,
		timeout:                 5 * time.Second,
	}
	creds, err := generateSCRAMCredentials(saslMechanismScramSha256, password)
	if err != nil {
		t.Fatalf("failed to create scram credentials: %v", err)
	}
	k.saslSCRAM256Credentials[username] = creds
	return k
}

// Test that SASLAuthenticate requests encoded with an Int32 BYTES prefix are
// properly parsed and core SCRAM server-first responses do not contain NUL.
func TestHandleSaslAuthenticate_Int32Bytes_ParsingAndTrim(t *testing.T) {
	k := makeSCRAMTestServer(t, "scramuser", "password")

	// Build client-first payload and append trailing NULs to simulate padding
	payload := []byte("n,,n=scramuser,r=nonce12345")
	payload = append(payload, 0, 0)

	data := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(payload)))
	copy(data[4:], payload)

	// Prepare a dummy connection pair
	sConn, cConn := net.Pipe()
	defer sConn.Close()
	defer cConn.Close()

	connState := &connectionState{scramMechanism: saslMechanismScramSha256}

	done := make(chan error, 1)
	go func() {
		// Use API version 1 (not 2) - Int32 bytes encoding is only used for version < 2
		_, err := k.handleSaslAuthenticate(sConn, 1, 1, data, 1, connState)
		done <- err
	}()

	// Read response from client side
	var size int32
	if err := binary.Read(cConn, binary.BigEndian, &size); err != nil {
		t.Fatalf("failed to read size: %v", err)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(cConn, buf); err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	// Parse response as SASLAuthenticate (skip correlation id)
	// Locate the server-first message within the response body by searching for "r="
	body := buf[4:]
	start := bytes.Index(body, []byte("r="))
	if start < 0 {
		t.Fatalf("could not find server-first 'r=' in response: %x", body)
	}
	// Slice until first NUL (if present) to avoid trailing padding
	endNull := bytes.IndexByte(body[start:], 0)
	var token []byte
	if endNull >= 0 {
		token = body[start : start+endNull]
	} else {
		token = body[start:]
	}
	if len(token) == 0 {
		t.Fatalf("empty server-first token")
	}
	if bytes.Contains(token, []byte{0}) {
		t.Fatalf("server-first token contains NULs: %x", token)
	}

	// Ensure the handler returned without error
	if err := <-done; err != nil {
		t.Fatalf("handleSaslAuthenticate returned error: %v", err)
	}
}

// Test that SASLAuthenticate requests encoded as CompactBytes are parsed correctly.
func TestHandleSaslAuthenticate_CompactBytes_Parsing(t *testing.T) {
	k := makeSCRAMTestServer(t, "scramuser", "password")

	payload := []byte("n,,n=scramuser,r=nonce_compact")
	// Kafka compact encoding uses uvarint of (len + 1). For small values this is one byte.
	prefix := byte(len(payload) + 1)
	data := append([]byte{prefix}, payload...)

	sConn, cConn := net.Pipe()
	defer sConn.Close()
	defer cConn.Close()

	connState := &connectionState{scramMechanism: saslMechanismScramSha256}

	done := make(chan error, 1)
	go func() {
		_, err := k.handleSaslAuthenticate(sConn, 2, 2, data, 2, connState)
		done <- err
	}()

	// Read response from client side
	var size int32
	if err := binary.Read(cConn, binary.BigEndian, &size); err != nil {
		t.Fatalf("failed to read size: %v", err)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(cConn, buf); err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	// Locate "r=" in body for compact case as well
	body2 := buf[4:]
	start2 := bytes.Index(body2, []byte("r="))
	if start2 < 0 {
		t.Fatalf("could not find server-first 'r=' in compact response: %x", body2)
	}
	endNull2 := bytes.IndexByte(body2[start2:], 0)
	var token2 []byte
	if endNull2 >= 0 {
		token2 = body2[start2 : start2+endNull2]
	} else {
		token2 = body2[start2:]
	}
	if len(token2) == 0 {
		t.Fatalf("empty server-first token in compact case")
	}
	if bytes.Contains(token2, []byte{0}) {
		t.Fatalf("server-first token contains NULs in compact case: %x", token2)
	}
	if err := <-done; err != nil {
		t.Fatalf("handleSaslAuthenticate returned error: %v", err)
	}
}

func TestKafkaServerInputAcks0(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19097"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19097"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce message with acks=0 (NoResponse)
	record := &kgo.Record{
		Topic: "test-topic",
		Value: []byte("test-value-acks-0"),
	}

	// It's not easy to force kgo to use acks=0 per record, but the client can be configured.
	// The franz-go client defaults to acks=all.
	// To test acks=0, construct a raw request or configure the client with kgo.RequiredAcks.
	// kgo.RequiredAcks(kgo.NoAck())

	clientAcks0, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19097"),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer clientAcks0.Close()

	done := make(chan struct{})
	go func() {
		// ProduceAsync with NoAck should return immediately
		clientAcks0.Produce(ctx, record, nil)
		close(done)
	}()

	select {
	case <-done:
		// Success - produce returned
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce with acks=0")
	}

	// Read message
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msgBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "test-value-acks-0", string(msgBytes))

	err = ackFn(ctx, nil)
	require.NoError(t, err)
}

func TestKafkaServerInputAcks1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19098"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Configure client for acks=1 (LeaderAck)
	clientAcks1, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19098"),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer clientAcks1.Close()

	record := &kgo.Record{
		Topic: "test-topic",
		Value: []byte("test-value-acks-1"),
	}

	produceChan := make(chan error, 1)
	go func() {
		results := clientAcks1.ProduceSync(ctx, record)
		if len(results) > 0 {
			produceChan <- results[0].Err
		}
	}()

	// Read message
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msgBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "test-value-acks-1", string(msgBytes))

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputInvalidAcks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19099"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Send a raw Kafka produce request with invalid acks value (e.g., acks=5)
	conn, err := net.Dial("tcp", "127.0.0.1:19099")
	require.NoError(t, err)
	defer conn.Close()

	// Build a produce request with acks=5 (invalid)
	produceReq := kmsg.NewProduceRequest()
	produceReq.SetVersion(9)
	produceReq.Acks = 5 // Invalid value
	produceReq.TimeoutMillis = 5000
	produceReq.Topics = []kmsg.ProduceRequestTopic{
		{
			Topic: "test-topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{
				{Partition: 0},
			},
		},
	}

	// Serialize the request
	var requestBody []byte
	requestBody = kbin.AppendInt16(requestBody, produceReq.Key())        // api key
	requestBody = kbin.AppendInt16(requestBody, produceReq.GetVersion()) // api version
	requestBody = kbin.AppendInt32(requestBody, 1)                       // correlation ID
	requestBody = kbin.AppendInt16(requestBody, -1)                      // null client ID
	requestBody = append(requestBody, 0)                                 // empty tagged fields (flexible)
	requestBody = produceReq.AppendTo(requestBody)

	// Send with size prefix
	sizePrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(sizePrefix, uint32(len(requestBody)))

	_, err = conn.Write(append(sizePrefix, requestBody...))
	require.NoError(t, err)

	// Read response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	respSizeBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf)
	require.NoError(t, err)

	respSize := binary.BigEndian.Uint32(respSizeBuf)
	require.Greater(t, respSize, uint32(0), "Response should not be empty")

	respBuf := make([]byte, respSize)
	_, err = io.ReadFull(conn, respBuf)
	require.NoError(t, err)

	// Parse the response - first 4 bytes are correlation ID
	corrID := binary.BigEndian.Uint32(respBuf[:4])
	assert.Equal(t, uint32(1), corrID)

	// Parse the produce response body (skip correlation ID and tag buffer)
	var produceResp kmsg.ProduceResponse
	produceResp.SetVersion(9)
	err = produceResp.ReadFrom(respBuf[4+1:]) // +1 for empty tag buffer in flexible response
	require.NoError(t, err)

	// Verify error code is InvalidRequiredAcks
	require.Len(t, produceResp.Topics, 1)
	require.Len(t, produceResp.Topics[0].Partitions, 1)
	assert.Equal(t, kerr.InvalidRequiredAcks.Code, produceResp.Topics[0].Partitions[0].ErrorCode,
		"Expected InvalidRequiredAcks error code")
}

func TestKafkaServerInputMessageTooLarge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	// Set a small max_message_bytes to trigger the per-partition check.
	// The franz-go client may compress records, so we use random
	// (incompressible) data to ensure the batch exceeds this limit.
	config := `
address: "127.0.0.1:19100"
max_message_bytes: 100
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create a producer client with no compression so the batch is guaranteed
	// to exceed max_message_bytes.
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19100"),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	require.NoError(t, err)
	defer client.Close()

	// Use random data so it cannot be compressed below max_message_bytes.
	randomValue := make([]byte, 200)
	_, err = rand.Read(randomValue)
	require.NoError(t, err)

	record := &kgo.Record{
		Topic: "test-topic",
		Value: randomValue,
	}

	// The produce should fail with MESSAGE_TOO_LARGE (non-retriable).
	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0].Err, kerr.MessageTooLarge, "Expected MESSAGE_TOO_LARGE error for oversized message")
	t.Logf("Got expected MESSAGE_TOO_LARGE error for oversized message: %v", results[0].Err)
}

// Quick round-trip test: ensure the metadata response constructed can be
// appended-to a buffer and parsed back by kmsg to avoid the 'not enough
// data' errors the client observed in integration tests.
func TestApiVersionsResponseStructure(t *testing.T) {
	resp := kmsg.NewApiVersionsResponse()
	resp.SetVersion(3)
	resp.ErrorCode = 0
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: 18, MinVersion: 0, MaxVersion: 3},
		{ApiKey: 3, MinVersion: 0, MaxVersion: 12},
		{ApiKey: 0, MinVersion: 0, MaxVersion: 9},
	}

	// Build buffer like sendResponse does
	buf := kbin.AppendInt32(nil, 0) // correlationID = 0
	buf = resp.AppendTo(buf)

	t.Logf("ApiVersions response hex (total %d bytes): %x", len(buf), buf)
	t.Logf("Response body (after correlationID, %d bytes): %x", len(buf)-4, buf[4:])
	t.Logf("Is flexible: %v", resp.IsFlexible())
}

func TestMetadataResponseRoundTrip(t *testing.T) {
	resp := kmsg.NewMetadataResponse()
	resp.SetVersion(12)

	// fill sample broker and topic like server does
	resp.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: "127.0.0.1", Port: 19092}}
	clusterID := "kafka-server-cluster"
	resp.ClusterID = &clusterID
	resp.ControllerID = 1

	resp.Topics = []kmsg.MetadataResponseTopic{
		{
			Topic:      kmsg.StringPtr("test-topic"),
			ErrorCode:  0,
			Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}, ErrorCode: 0}},
		},
	}

	// Build buffer as sendResponse would (correlation id + message bytes)
	buf := kbin.AppendInt32(nil, 123)
	buf = resp.AppendTo(buf)

	// Debug: print the hex
	t.Logf("Metadata response hex (total %d bytes): %x", len(buf), buf)
	t.Logf("Response body (after correlationID, %d bytes): %x", len(buf)-4, buf[4:])

	// Now parse back after skipping correlation id
	parsed := kmsg.NewMetadataResponse()
	parsed.SetVersion(12)
	if err := parsed.ReadFrom(buf[4:]); err != nil {
		t.Fatalf("metadata response failed to parse: %v", err)
	}

	// Verify parsed data
	require.Equal(t, 1, len(parsed.Brokers), "should have 1 broker")
	require.Equal(t, "127.0.0.1", parsed.Brokers[0].Host, "broker host")
	require.Equal(t, int32(19092), parsed.Brokers[0].Port, "broker port")
	require.Equal(t, 1, len(parsed.Topics), "should have 1 topic")
	if parsed.Topics[0].Topic != nil {
		require.Equal(t, "test-topic", *parsed.Topics[0].Topic, "topic name")
	}
}

func TestKafkaServerInputCompression(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19099"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create producer with Snappy compression (default when batching)
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19099"),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce multiple compressed messages
	numMessages := 5
	testTopic := "compressed-test"

	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: testTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		go func() {
			client.Produce(ctx, record, func(r *kgo.Record, err error) {
				// Callback
			})
		}()
	}

	// Read messages
	receivedCount := 0
	timeout := time.After(10 * time.Second)

	for receivedCount < numMessages {
		select {
		case <-timeout:
			t.Fatalf("Timeout: only received %d of %d compressed messages", receivedCount, numMessages)
		default:
			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			batch, ackFn, err := input.ReadBatch(readCtx)
			readCancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					continue
				}
				t.Fatalf("Failed to read batch: %v", err)
			}

			receivedCount += len(batch)

			// Verify metadata exists
			for _, msg := range batch {
				topic, exists := msg.MetaGet("kafka_server_topic")
				assert.True(t, exists)
				assert.Equal(t, testTopic, topic)
			}

			// Acknowledge
			err = ackFn(ctx, nil)
			require.NoError(t, err)
		}
	}

	assert.Equal(t, numMessages, receivedCount)
	t.Logf("Successfully received %d compressed messages", receivedCount)
}

func TestKafkaServerInputLegacyMessageSet(t *testing.T) {
	// Test that we can parse legacy MessageSet format (magic 0)
	// This format is used by some older clients like confluent-kafka with MsgVersion 0

	// Build a legacy MessageSet (magic 0) manually
	// Format: Offset(8) + MessageSize(4) + CRC(4) + Magic(1) + Attributes(1) + Key(4+data) + Value(4+data)
	value := []byte(`{"test":"legacy"}`)
	key := []byte("test-key")

	var msgBuf bytes.Buffer
	// Offset
	binary.Write(&msgBuf, binary.BigEndian, int64(0))
	// MessageSize (CRC + Magic + Attributes + Key + Value = 4 + 1 + 1 + 4 + len(key) + 4 + len(value))
	messageSize := int32(4 + 1 + 1 + 4 + len(key) + 4 + len(value))
	binary.Write(&msgBuf, binary.BigEndian, messageSize)
	// CRC (dummy value, we don't validate it)
	binary.Write(&msgBuf, binary.BigEndian, int32(0x12345678))
	// Magic (0 for legacy format)
	msgBuf.WriteByte(0)
	// Attributes (0 = no compression)
	msgBuf.WriteByte(0)
	// Key length + key
	binary.Write(&msgBuf, binary.BigEndian, int32(len(key)))
	msgBuf.Write(key)
	// Value length + value
	binary.Write(&msgBuf, binary.BigEndian, int32(len(value)))
	msgBuf.Write(value)

	recordsData := msgBuf.Bytes()

	// Create a minimal kafkaServerInput for testing
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()
	config := `address: "127.0.0.1:19999"`
	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	// Directly test parseRecordBatch which should detect legacy format
	batch, err := input.parseRecordBatch(1, recordsData, "test-topic", 0, "127.0.0.1:12345")
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msgBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, value, msgBytes)

	msgKey, exists := batch[0].MetaGet("kafka_server_key")
	assert.True(t, exists)
	assert.Equal(t, string(key), msgKey)

	t.Log("Successfully parsed legacy MessageSet format (magic 0)")
}

func TestKafkaServerInputLegacyMessageSetMagic1(t *testing.T) {
	// Test legacy MessageSet format with magic 1 (includes timestamp)
	value := []byte(`{"test":"legacy_v1"}`)
	key := []byte("key-v1")
	timestamp := int64(1700000000000) // some timestamp in ms

	var msgBuf bytes.Buffer
	// Offset
	binary.Write(&msgBuf, binary.BigEndian, int64(42))
	// MessageSize (CRC + Magic + Attributes + Timestamp + Key + Value)
	messageSize := int32(4 + 1 + 1 + 8 + 4 + len(key) + 4 + len(value))
	binary.Write(&msgBuf, binary.BigEndian, messageSize)
	// CRC (dummy value)
	binary.Write(&msgBuf, binary.BigEndian, uint32(0xDEADBEEF))
	// Magic (1 for v1 format with timestamp)
	msgBuf.WriteByte(1)
	// Attributes (0 = no compression)
	msgBuf.WriteByte(0)
	// Timestamp (only for magic >= 1)
	binary.Write(&msgBuf, binary.BigEndian, timestamp)
	// Key length + key
	binary.Write(&msgBuf, binary.BigEndian, int32(len(key)))
	msgBuf.Write(key)
	// Value length + value
	binary.Write(&msgBuf, binary.BigEndian, int32(len(value)))
	msgBuf.Write(value)

	recordsData := msgBuf.Bytes()

	// Create a minimal kafkaServerInput for testing
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()
	config := `address: "127.0.0.1:19999"`
	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	batch, err := input.parseRecordBatch(1, recordsData, "test-topic", 0, "127.0.0.1:12345")
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msgBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, value, msgBytes)

	msgKey, exists := batch[0].MetaGet("kafka_server_key")
	assert.True(t, exists)
	assert.Equal(t, string(key), msgKey)

	// Verify timestamp was parsed
	tsUnix, exists := batch[0].MetaGet("kafka_server_timestamp_unix")
	assert.True(t, exists)
	assert.Equal(t, "1700000000", tsUnix)

	t.Log("Successfully parsed legacy MessageSet format (magic 1)")
}

func TestKafkaServerInputSASLPlain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SASL PLAIN authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19100"
sasl:
  - mechanism: PLAIN
    username: "testuser"
    password: "testpass"
  - mechanism: PLAIN
    username: "anotheruser"
    password: "anotherpass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	t.Logf("Creating client with SASL PLAIN authentication")

	// Create client with SASL PLAIN authentication
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19100"),
		kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			t.Logf("SASL PLAIN auth callback called")
			return plain.Auth{
				User: "testuser",
				Pass: "testpass",
			}, nil
		})),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string {
			return "[CLIENT] "
		})),
	)
	require.NoError(t, err)
	defer client.Close()

	t.Logf("Client created successfully")

	// Produce a test message
	testTopic := "authenticated-topic"
	testValue := "authenticated-message"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	produceChan := make(chan error, 1)
	go func() {
		t.Logf("Producer goroutine: calling ProduceSync")
		results := client.ProduceSync(ctx, record)
		t.Logf("Producer goroutine: ProduceSync returned, results len=%d", len(results))
		if len(results) > 0 {
			produceChan <- results[0].Err
		}
	}()

	t.Logf("Calling ReadBatch")
	// Read message from input
	batch, ackFn, err := input.ReadBatch(ctx)
	t.Logf("ReadBatch returned: batch len=%d, err=%v", len(batch), err)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]

	// Verify message content
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testValue, string(msgBytes))

	// Verify metadata
	topic, exists := msg.MetaGet("kafka_server_topic")
	assert.True(t, exists)
	assert.Equal(t, testTopic, topic)

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputSASLFailedAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create kafka_server input with SASL PLAIN authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19101"
sasl:
  - mechanism: PLAIN
    username: "validuser"
    password: "validpass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client with WRONG credentials
	// Use RetryBackoff to limit retry attempts
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19101"),
		kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{
				User: "validuser",
				Pass: "wrongpass",
			}, nil
		})),
		kgo.RetryBackoffFn(func(int) time.Duration { return 100 * time.Millisecond }),
	)
	require.NoError(t, err)
	defer client.Close()

	// Try to produce a message - should fail quickly due to auth error
	testTopic := "test-topic"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)

	// Should get an authentication error
	assert.Error(t, results[0].Err)
	t.Logf("Expected authentication error received: %v", results[0].Err)
}

func TestKafkaServerInputSASLWithoutAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create kafka_server input with SASL enabled
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19102"
sasl:
  - mechanism: PLAIN
    username: "testuser"
    password: "testpass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client WITHOUT SASL authentication
	// Server requires SASL, so connection should fail
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19102"),
		kgo.RetryBackoffFn(func(int) time.Duration { return 100 * time.Millisecond }),
	)
	require.NoError(t, err)
	defer client.Close()

	// Try to produce a message - should fail
	testTopic := "test-topic"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)

	// Should get an error due to missing authentication
	assert.Error(t, results[0].Err)
	t.Logf("Expected authentication error received: %v", results[0].Err)
}

func TestKafkaServerInputSCRAMSHA256(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SCRAM-SHA-256 authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19103"
sasl:
  - mechanism: SCRAM-SHA-256
    username: "scramuser"
    password: "scrampass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client with SCRAM-SHA-256 authentication
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19103"),
		kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
			t.Logf("SCRAM-SHA-256 auth callback called")
			return scram.Auth{
				User: "scramuser",
				Pass: "scrampass",
			}, nil
		})),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string {
			return "[CLIENT] "
		})),
	)
	require.NoError(t, err)
	defer client.Close()

	t.Logf("Client created successfully")

	// Produce a message
	testTopic := "test-topic"
	testValue := "test-value-scram"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	// Use channel to synchronize producer and consumer
	produceChan := make(chan error, 1)
	go func() {
		results := client.ProduceSync(ctx, record)
		if len(results) > 0 {
			produceChan <- results[0].Err
		}
	}()

	// Read message from input
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]

	// Verify message content
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testValue, string(msgBytes))

	// Verify metadata
	topic, exists := msg.MetaGet("kafka_server_topic")
	assert.True(t, exists)
	assert.Equal(t, testTopic, topic)

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputSCRAMSHA512(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SCRAM-SHA-512 authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19104"
sasl:
  - mechanism: SCRAM-SHA-512
    username: "scramuser512"
    password: "scrampass512"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client with SCRAM-SHA-512 authentication
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19104"),
		kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: "scramuser512",
				Pass: "scrampass512",
			}, nil
		})),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce a message
	testTopic := "test-topic-512"
	testValue := "test-value-scram-512"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	// Use channel to synchronize producer and consumer
	produceChan := make(chan error, 1)
	go func() {
		results := client.ProduceSync(ctx, record)
		if len(results) > 0 {
			produceChan <- results[0].Err
		}
	}()

	// Read message from input
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]

	// Verify message content
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testValue, string(msgBytes))

	// Verify metadata
	topic, exists := msg.MetaGet("kafka_server_topic")
	assert.True(t, exists)
	assert.Equal(t, testTopic, topic)

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputSCRAMFailedAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create kafka_server input with SCRAM-SHA-256 authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19105"
sasl:
  - mechanism: SCRAM-SHA-256
    username: "validuser"
    password: "validpass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client with WRONG credentials
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19105"),
		kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: "validuser",
				Pass: "wrongpass",
			}, nil
		})),
		kgo.RetryBackoffFn(func(int) time.Duration { return 100 * time.Millisecond }),
	)
	require.NoError(t, err)
	defer client.Close()

	// Try to produce a message - should fail
	testTopic := "test-topic"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)

	// Should get an authentication error
	assert.Error(t, results[0].Err)
	t.Logf("Expected SCRAM authentication error received: %v", results[0].Err)
}

// NOTE: TestParseRequestHeader was removed because parseRequestHeader() was replaced
// by using franz-go's kmsg.RequestForKey() + ReadFrom() pattern (following kfake).
// The header parsing functionality is now tested implicitly through integration tests.

// TestProduceResponsePartitionSerialization tests that ProduceResponseTopicPartition
// is correctly initialized using kmsg.NewProduceResponseTopicPartition() to ensure
// proper default values for tagged fields like CurrentLeader.
//
// REGRESSION TEST: Previously, code used struct literals like:
//
//	kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: 0, ...}
//
// which would initialize CurrentLeader to {0, 0} (zero values), causing kmsg
// to serialize it as a tagged field with 11 extra bytes. This broke Java Kafka
// clients which expected either the default {-1, -1} or no tagged field at all.
//
// The fix is to use kmsg.NewProduceResponseTopicPartition() which initializes
// CurrentLeader to {NodeID: -1, Epoch: -1} (the "not present" sentinel values).
func TestProduceResponsePartitionSerialization(t *testing.T) {
	t.Run("NewProduceResponseTopicPartition has correct CurrentLeader defaults", func(t *testing.T) {
		partResp := kmsg.NewProduceResponseTopicPartition()

		// CurrentLeader should be initialized to -1, -1 (not present)
		// which means it won't be serialized as a tagged field
		assert.Equal(t, int32(-1), partResp.CurrentLeader.LeaderID,
			"CurrentLeader.LeaderID should be -1 (not present)")
		assert.Equal(t, int32(-1), partResp.CurrentLeader.LeaderEpoch,
			"CurrentLeader.LeaderEpoch should be -1 (not present)")
	})

	t.Run("struct literal has incorrect CurrentLeader defaults", func(t *testing.T) {
		// This demonstrates the BUG that was fixed - DO NOT use struct literals
		partResp := kmsg.ProduceResponseTopicPartition{}

		// Zero-initialized struct has CurrentLeader at {0, 0}
		// which gets serialized as a tagged field
		assert.Equal(t, int32(0), partResp.CurrentLeader.LeaderID,
			"Struct literal has zero-initialized LeaderID")
		assert.Equal(t, int32(0), partResp.CurrentLeader.LeaderEpoch,
			"Struct literal has zero-initialized LeaderEpoch")
	})

	t.Run("serialization size difference", func(t *testing.T) {
		// Create a proper response using NewProduceResponseTopicPartition
		goodPartResp := kmsg.NewProduceResponseTopicPartition()
		goodPartResp.Partition = 0
		goodPartResp.ErrorCode = 0
		goodPartResp.BaseOffset = 0
		goodPartResp.LogAppendTime = -1
		goodPartResp.LogStartOffset = 0

		// Create a buggy response using struct literal
		badPartResp := kmsg.ProduceResponseTopicPartition{
			Partition:      0,
			ErrorCode:      0,
			BaseOffset:     0,
			LogAppendTime:  -1,
			LogStartOffset: 0,
			// CurrentLeader implicitly {0, 0}
		}

		// Build full responses to serialize
		goodResp := kmsg.NewProduceResponse()
		goodResp.Version = 9 // Flexible version
		goodResp.Topics = []kmsg.ProduceResponseTopic{{
			Topic:      "test",
			Partitions: []kmsg.ProduceResponseTopicPartition{goodPartResp},
		}}

		badResp := kmsg.NewProduceResponse()
		badResp.Version = 9
		badResp.Topics = []kmsg.ProduceResponseTopic{{
			Topic:      "test",
			Partitions: []kmsg.ProduceResponseTopicPartition{badPartResp},
		}}

		goodBytes := goodResp.AppendTo(nil)
		badBytes := badResp.AppendTo(nil)

		// The bad response should be larger due to extra tagged field bytes
		assert.Greater(t, len(badBytes), len(goodBytes),
			"Struct literal response should have more bytes due to CurrentLeader tagged field")

		// Log the actual sizes for debugging
		t.Logf("Good response size: %d bytes", len(goodBytes))
		t.Logf("Bad response size: %d bytes (extra %d bytes from CurrentLeader tag)",
			len(badBytes), len(badBytes)-len(goodBytes))
	})

	t.Run("Java client compatible response", func(t *testing.T) {
		// This test verifies the exact format expected by Java Kafka clients
		// The response should NOT include CurrentLeader as a tagged field
		// when using the default -1,-1 values

		partResp := kmsg.NewProduceResponseTopicPartition()
		partResp.Partition = 0
		partResp.ErrorCode = 0
		partResp.BaseOffset = 0
		partResp.LogAppendTime = -1
		partResp.LogStartOffset = 0

		resp := kmsg.NewProduceResponse()
		resp.Version = 9
		resp.ThrottleMillis = 0
		resp.Topics = []kmsg.ProduceResponseTopic{{
			Topic:      "test-topic",
			Partitions: []kmsg.ProduceResponseTopicPartition{partResp},
		}}

		responseBytes := resp.AppendTo(nil)

		// Parse the response to verify it's valid
		var parsedResp kmsg.ProduceResponse
		parsedResp.SetVersion(9)
		err := parsedResp.ReadFrom(responseBytes)
		require.NoError(t, err, "Response should be parseable")

		// Verify the parsed response matches
		require.Len(t, parsedResp.Topics, 1)
		require.Len(t, parsedResp.Topics[0].Partitions, 1)
		assert.Equal(t, int32(0), parsedResp.Topics[0].Partitions[0].Partition)
		assert.Equal(t, int16(0), parsedResp.Topics[0].Partitions[0].ErrorCode)
	})
}

func TestBuildProduceResponseTopicsPartitionErrors(t *testing.T) {
	t.Run("nil partitionErrors gives all partitions the default error code", func(t *testing.T) {
		topics := []kmsg.ProduceRequestTopic{
			{
				Topic: "topic-a",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0},
					{Partition: 1},
				},
			},
		}
		resp := buildProduceResponseTopics(topics, kerr.RequestTimedOut.Code, nil)

		require.Len(t, resp, 1)
		require.Len(t, resp[0].Partitions, 2)
		assert.Equal(t, kerr.RequestTimedOut.Code, resp[0].Partitions[0].ErrorCode)
		assert.Equal(t, kerr.RequestTimedOut.Code, resp[0].Partitions[1].ErrorCode)
	})

	t.Run("per-partition error overrides default for that partition only", func(t *testing.T) {
		topics := []kmsg.ProduceRequestTopic{
			{
				Topic: "topic-a",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0},
					{Partition: 1},
					{Partition: 2},
				},
			},
		}
		partErrors := map[partitionErrorKey]int16{
			{topic: "topic-a", partition: 1}: kerr.MessageTooLarge.Code,
		}
		resp := buildProduceResponseTopics(topics, 0, partErrors)

		require.Len(t, resp, 1)
		require.Len(t, resp[0].Partitions, 3)
		// partition 0: default (success)
		assert.Equal(t, int16(0), resp[0].Partitions[0].ErrorCode)
		assert.Equal(t, int32(0), resp[0].Partitions[0].Partition)
		// partition 1: per-partition override
		assert.Equal(t, kerr.MessageTooLarge.Code, resp[0].Partitions[1].ErrorCode)
		assert.Equal(t, int32(1), resp[0].Partitions[1].Partition)
		// partition 2: default (success)
		assert.Equal(t, int16(0), resp[0].Partitions[2].ErrorCode)
		assert.Equal(t, int32(2), resp[0].Partitions[2].Partition)
	})

	t.Run("multiple topics with mixed per-partition errors", func(t *testing.T) {
		topics := []kmsg.ProduceRequestTopic{
			{
				Topic: "allowed-topic",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0},
				},
			},
			{
				Topic: "disallowed-topic",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0},
					{Partition: 1},
				},
			},
		}
		partErrors := map[partitionErrorKey]int16{
			{topic: "disallowed-topic", partition: 0}: kerr.UnknownTopicOrPartition.Code,
			{topic: "disallowed-topic", partition: 1}: kerr.UnknownTopicOrPartition.Code,
		}
		resp := buildProduceResponseTopics(topics, 0, partErrors)

		require.Len(t, resp, 2)

		// allowed-topic: partition 0 should get the default (success)
		assert.Equal(t, "allowed-topic", resp[0].Topic)
		require.Len(t, resp[0].Partitions, 1)
		assert.Equal(t, int16(0), resp[0].Partitions[0].ErrorCode)

		// disallowed-topic: both partitions should get the per-partition error
		assert.Equal(t, "disallowed-topic", resp[1].Topic)
		require.Len(t, resp[1].Partitions, 2)
		assert.Equal(t, kerr.UnknownTopicOrPartition.Code, resp[1].Partitions[0].ErrorCode)
		assert.Equal(t, kerr.UnknownTopicOrPartition.Code, resp[1].Partitions[1].ErrorCode)
	})

	t.Run("per-partition error preserved even with non-zero default", func(t *testing.T) {
		topics := []kmsg.ProduceRequestTopic{
			{
				Topic: "topic-a",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0},
					{Partition: 1},
				},
			},
		}
		partErrors := map[partitionErrorKey]int16{
			{topic: "topic-a", partition: 0}: kerr.MessageTooLarge.Code,
		}
		// default is a pipeline error, but partition 0 should still show MessageTooLarge
		resp := buildProduceResponseTopics(topics, kerr.UnknownServerError.Code, partErrors)

		require.Len(t, resp, 1)
		require.Len(t, resp[0].Partitions, 2)
		// partition 0: per-partition override takes precedence over default
		assert.Equal(t, kerr.MessageTooLarge.Code, resp[0].Partitions[0].ErrorCode)
		// partition 1: gets the default pipeline error
		assert.Equal(t, kerr.UnknownServerError.Code, resp[0].Partitions[1].ErrorCode)
	})

	t.Run("response fields are properly initialized", func(t *testing.T) {
		topics := []kmsg.ProduceRequestTopic{
			{
				Topic: "topic-a",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 5},
				},
			},
		}
		resp := buildProduceResponseTopics(topics, 0, nil)

		require.Len(t, resp, 1)
		require.Len(t, resp[0].Partitions, 1)
		p := resp[0].Partitions[0]
		assert.Equal(t, int32(5), p.Partition)
		assert.Equal(t, int64(0), p.BaseOffset)
		assert.Equal(t, int64(-1), p.LogAppendTime)
		assert.Equal(t, int64(0), p.LogStartOffset)
		// CurrentLeader should be {-1, -1} from NewProduceResponseTopicPartition()
		assert.Equal(t, int32(-1), p.CurrentLeader.LeaderID)
		assert.Equal(t, int32(-1), p.CurrentLeader.LeaderEpoch)
	})
}

func TestKafkaServerInputMTLSConfig(t *testing.T) {
	// Create temporary certificate files for testing
	tmpDir := t.TempDir()
	certFile := tmpDir + "/test-cert.pem"
	keyFile := tmpDir + "/test-key.pem"
	caFile := tmpDir + "/test-ca.pem"

	// Generate real certificates for validation tests
	// Create CA key and certificate
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	err = os.WriteFile(caFile, caCertPEM, 0600)
	require.NoError(t, err)

	// Create server key and certificate signed by CA
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, &caTemplate, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	err = os.WriteFile(certFile, serverCertPEM, 0600)
	require.NoError(t, err)

	serverKeyDER := x509.MarshalPKCS1PrivateKey(serverKey)
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: serverKeyDER})
	err = os.WriteFile(keyFile, serverKeyPEM, 0600)
	require.NoError(t, err)

	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "valid mTLS config with require_and_verify",
			config: fmt.Sprintf(`
address: "127.0.0.1:19092"
cert_file: %s
key_file: %s
mtls_auth: require_and_verify
mtls_cas_files:
  - %s
`, certFile, keyFile, caFile),
			wantErr: false, // Valid config
		},
		{
			name: "valid mTLS config with verify_if_given",
			config: fmt.Sprintf(`
address: "127.0.0.1:19092"
cert_file: %s
key_file: %s
mtls_auth: verify_if_given
mtls_cas_files:
  - %s
`, certFile, keyFile, caFile),
			wantErr: false, // Valid config
		},
		{
			name: "valid mTLS config with request",
			config: fmt.Sprintf(`
address: "127.0.0.1:19092"
cert_file: %s
key_file: %s
mtls_auth: request
`, certFile, keyFile),
			wantErr: false, // Valid config
		},
		{
			name: "invalid mtls_auth",
			config: fmt.Sprintf(`
address: "127.0.0.1:19092"
cert_file: %s
key_file: %s
mtls_auth: invalid_option
`, certFile, keyFile),
			wantErr: true,
		},
		{
			name: "cert_file without key_file",
			config: fmt.Sprintf(`
address: "127.0.0.1:19092"
cert_file: %s
`, certFile),
			wantErr: true,
		},
		{
			name: "key_file without cert_file",
			config: fmt.Sprintf(`
address: "127.0.0.1:19092"
key_file: %s
`, keyFile),
			wantErr: true,
		},
		{
			name: "require_and_verify without mtls_cas_files",
			config: fmt.Sprintf(`
address: "127.0.0.1:19092"
cert_file: %s
key_file: %s
mtls_auth: require_and_verify
`, certFile, keyFile),
			wantErr: true,
		},
		{
			name: "verify_if_given without mtls_cas_files",
			config: fmt.Sprintf(`
address: "127.0.0.1:19092"
cert_file: %s
key_file: %s
mtls_auth: verify_if_given
`, certFile, keyFile),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := kafkaServerInputConfig()
			env := service.NewEnvironment()

			parsed, err := spec.ParseYAML(tt.config, env)
			require.NoError(t, err, "Config should parse successfully")

			_, err = newKafkaServerInputFromConfig(parsed, service.MockResources())
			if tt.wantErr {
				require.Error(t, err, "Expected an error but got none")
				t.Logf("Got expected error: %v", err)
			} else {
				require.NoError(t, err, "Expected no error but got: %v", err)
			}
		})
	}
}

func TestKafkaServerInputMTLSIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create CA key and certificate
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Create server key and certificate
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, &caTemplate, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	// Create client key and certificate
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
			CommonName:   "test-client",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, &caTemplate, &clientKey.PublicKey, caKey)
	require.NoError(t, err)

	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

	// Create kafka_server input with mTLS
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	// Write certificates to temp files
	tmpDir := t.TempDir()

	serverCertFile := tmpDir + "/server-cert.pem"
	err = os.WriteFile(serverCertFile, serverCertPEM, 0600)
	require.NoError(t, err)

	serverKeyFile := tmpDir + "/server-key.pem"
	err = os.WriteFile(serverKeyFile, serverKeyPEM, 0600)
	require.NoError(t, err)

	clientCAFile := tmpDir + "/client-ca.pem"
	err = os.WriteFile(clientCAFile, caCertPEM, 0600)
	require.NoError(t, err)

	config := fmt.Sprintf(`
address: "127.0.0.1:19110"
cert_file: %s
key_file: %s
mtls_auth: require_and_verify
mtls_cas_files:
  - %s
`, serverCertFile, serverKeyFile, clientCAFile)

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Test 1: Client with valid certificate should connect
	t.Run("valid_client_cert", func(t *testing.T) {
		clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		require.NoError(t, err)

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCertPEM)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caCertPool,
		}

		client, err := kgo.NewClient(
			kgo.SeedBrokers("127.0.0.1:19110"),
			kgo.DialTLSConfig(tlsConfig),
			kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string {
				return "[CLIENT-VALID] "
			})),
		)
		require.NoError(t, err)
		defer client.Close()

		testTopic := "test-topic-mtls"
		testValue := "test-value-mtls"

		record := &kgo.Record{
			Topic: testTopic,
			Value: []byte(testValue),
		}

		// Use channel for synchronization
		produceChan := make(chan error, 1)
		go func() {
			results := client.ProduceSync(ctx, record)
			if len(results) > 0 {
				produceChan <- results[0].Err
			}
		}()

		// Read message from input
		batch, ackFn, err := input.ReadBatch(ctx)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		msgBytes, err := batch[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, testValue, string(msgBytes))

		err = ackFn(ctx, nil)
		require.NoError(t, err)

		// Verify producer received acknowledgment
		select {
		case err := <-produceChan:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for producer acknowledgment")
		}
	})

	// Test 2: Client without certificate should be rejected
	t.Run("no_client_cert", func(t *testing.T) {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCertPEM)

		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}

		client, err := kgo.NewClient(
			kgo.SeedBrokers("127.0.0.1:19110"),
			kgo.DialTLSConfig(tlsConfig),
			kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string {
				return "[CLIENT-NO-CERT] "
			})),
		)
		require.NoError(t, err)
		defer client.Close()

		testTopic := "test-topic-fail"
		testValue := "should-fail"

		record := &kgo.Record{
			Topic: testTopic,
			Value: []byte(testValue),
		}

		results := client.ProduceSync(ctx, record)
		require.Len(t, results, 1)

		// Should get an error due to missing client certificate
		assert.Error(t, results[0].Err)
		t.Logf("Expected TLS error received: %v", results[0].Err)
	})
}
