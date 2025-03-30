package io_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func parseYAMLOutputConf(t testing.TB, formatStr string, args ...any) (conf output.Config) {
	t.Helper()
	var err error
	conf, err = testutil.OutputFromYAML(fmt.Sprintf(formatStr, args...))
	require.NoError(t, err)
	return
}

func TestHTTPServerOutputBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nTestLoops := 10

	port := getFreePort(t)
	conf := parseYAMLOutputConf(t, `
http_server:
  address: localhost:%v
  path: /testpost
`, port)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}
	if err = h.Consume(msgChan); err == nil {
		t.Error("Expected error from double listen")
	}

	<-time.After(time.Millisecond * 100)

	// Test both single and multipart messages.
	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		go func() {
			testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
			select {
			case msgChan <- message.NewTransaction(testMsg, resChan):
			case <-time.After(time.Second):
				t.Error("Timed out waiting for message")
				return
			}
			select {
			case resMsg := <-resChan:
				if resMsg != nil {
					t.Error(resMsg)
				}
			case <-time.After(time.Second):
				t.Error("Timed out waiting for response")
			}
		}()

		res, err := http.Get(fmt.Sprintf("http://localhost:%v/testpost", port))
		if err != nil {
			t.Error(err)
			return
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Errorf("Wrong error code returned: %v", res.StatusCode)
			return
		}
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPServerOutputBadRequests(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	port := getFreePort(t)
	conf := parseYAMLOutputConf(t, `
http_server:
  address: localhost:%v
  path: /testpost
`, port)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	msgChan := make(chan message.Transaction)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))

	_, err = http.Get(fmt.Sprintf("http://localhost:%v/testpost", port))
	if err == nil {
		t.Error("request success when service should be closed")
	}
}

func TestHTTPServerOutputTimeout(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	port := getFreePort(t)
	conf := parseYAMLOutputConf(t, `
http_server:
  address: localhost:%v
  path: /testpost
  timeout: 1ms
`, port)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	msgChan := make(chan message.Transaction)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	var res *http.Response
	res, err = http.Get(fmt.Sprintf("http://localhost:%v/testpost", port))
	if err != nil {
		t.Error(err)
		return
	}
	if exp, act := http.StatusRequestTimeout, res.StatusCode; exp != act {
		t.Errorf("Unexpected status code: %v != %v", exp, act)
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPServerOutputSSEStream(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	port := getFreePort(t)
	conf := parseYAMLOutputConf(t, `
http_server:
  address: localhost:%v
  stream_path: /teststream
  stream_format: event_source
  cors:
    enabled: true
    allowed_origins:
      - "*"
`, port)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	require.NoError(t, h.Consume(msgChan))

	<-time.After(time.Millisecond * 100)

	// Start a client that will consume the SSE stream
	clientDone := make(chan struct{})
	clientErrors := make(chan error, 1)
	receivedMessages := make(chan string, 10)

	go func() {
		defer close(clientDone)

		req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%v/teststream", port), nil)
		if err != nil {
			clientErrors <- fmt.Errorf("failed to create request: %w", err)
			return
		}

		// Set headers that would be set by a browser for SSE
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Origin", "http://example.com")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			clientErrors <- fmt.Errorf("failed to execute request: %w", err)
			return
		}
		defer resp.Body.Close()

		// Check response headers
		if resp.StatusCode != 200 {
			clientErrors <- fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			return
		}

		if contentType := resp.Header.Get("Content-Type"); contentType != "text/event-stream" {
			clientErrors <- fmt.Errorf("unexpected content type: %s", contentType)
			return
		}

		if cors := resp.Header.Get("Access-Control-Allow-Origin"); cors != "*" {
			clientErrors <- fmt.Errorf("unexpected CORS header: %s", cors)
			return
		}

		// Use a scanner to read the SSE stream line by line
		buffer := make([]byte, 4096)
		currentMsg := ""
		msgCount := 0

		for msgCount < 5 {
			n, err := resp.Body.Read(buffer)
			if err != nil {
				clientErrors <- fmt.Errorf("failed to read from body: %w", err)
				return
			}

			currentMsg += string(buffer[:n])

			// Very simple SSE parser - looking for complete "data: X\n\n" events
			for {
				idx := extractSSEMessage(currentMsg)
				if idx <= 0 {
					break // No complete message found
				}

				// Extract and process the message content
				msgContent := currentMsg[:idx]
				currentMsg = currentMsg[idx:]

				// Parse out the actual data from "data: content\n\n" format
				dataContent := parseSSEData(msgContent)
				if dataContent != "" {
					receivedMessages <- dataContent
					msgCount++
				}

				if msgCount >= 5 {
					break
				}
			}

			if msgCount >= 5 {
				break
			}
		}
	}()

	// Send some test messages through the SSE stream
	testMessages := []string{
		"test message 1",
		"test message 2\nwith a newline",
		"test message 3",
		"test message 4",
		"test message 5",
	}

	for _, msg := range testMessages {
		testMsg := message.QuickBatch([][]byte{[]byte(msg)})

		select {
		case msgChan <- message.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting to send message")
		}

		select {
		case resErr := <-resChan:
			require.NoError(t, resErr)
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for response")
		}

		// Small delay to ensure messages are processed in order
		time.Sleep(time.Millisecond * 10)
	}

	// Wait for client to finish or timeout
	select {
	case <-clientDone:
		// Client finished successfully
	case err := <-clientErrors:
		t.Fatalf("Client error: %v", err)
	case <-time.After(time.Second * 5):
		t.Fatal("Client timed out")
	}

	// Collect and validate received messages
	close(receivedMessages)
	var collectedMessages []string
	for msg := range receivedMessages {
		collectedMessages = append(collectedMessages, msg)
	}

	// Verify that we received the expected messages
	require.GreaterOrEqual(t, len(collectedMessages), 5, "Expected at least 5 messages")

	// Check that all test messages are represented in the received messages
	for _, expectedMsg := range testMessages {
		found := false
		for _, receivedMsg := range collectedMessages {
			if strings.Contains(receivedMsg, expectedMsg) ||
				(strings.Contains(expectedMsg, "\n") &&
					strings.Contains(receivedMsg, strings.ReplaceAll(expectedMsg, "\n", ""))) {
				found = true
				break
			}
		}
		require.True(t, found, "Message not found in response: %s", expectedMsg)
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

// Helper functions for SSE parsing

// extractSSEMessage finds a complete SSE message and returns its ending index
// Returns 0 if no complete message is found
func extractSSEMessage(data string) int {
	// Look for the double newline that terminates an SSE message
	idx := strings.Index(data, "\n\n")
	if idx == -1 {
		return 0
	}
	return idx + 2 // Include the double newline
}

// parseSSEData extracts the data content from an SSE message
func parseSSEData(sseMsg string) string {
	var result strings.Builder
	lines := strings.Split(sseMsg, "\n")

	for _, line := range lines {
		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")
			result.WriteString(data)
		}
	}

	return result.String()
}
