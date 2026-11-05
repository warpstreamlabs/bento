package io_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func parseYAMLProcConf(t testing.TB, formatStr string, args ...any) (conf processor.Config) {
	t.Helper()
	var err error
	conf, err = testutil.ProcessorFromYAML(fmt.Sprintf(formatStr, args...))
	require.NoError(t, err)
	return
}

func TestHTTPClientRetries(t *testing.T) {
	var reqCount atomic.Uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		http.Error(w, "test error", http.StatusForbidden)
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
  retry_period: 1ms
  retries: 3
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("test")}))
	if res != nil {
		t.Fatal(res)
	}
	if len(msgs) != 1 {
		t.Fatal("Wrong count of error messages")
	}
	if msgs[0].Len() != 1 {
		t.Fatal("Wrong count of error message parts")
	}
	if exp, act := "test", string(msgs[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	assert.Error(t, msgs[0].Get(0).ErrorGet())
	if exp, act := "403", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}

	if exp, act := uint32(4), reqCount.Load(); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}
}

func TestHTTPClientBasic(t *testing.T) {
	i := 0
	expPayloads := []string{"foo", "bar", "baz"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.Header().Add("foobar", "baz")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
  retry_period: 1ms
  retries: 3
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).MetaGetStr("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}

	msgs, res = h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).MetaGetStr("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}

	// Check metadata persists.
	msg := message.QuickBatch([][]byte{[]byte("baz")})
	msg.Get(0).MetaSetMut("foo", "bar")
	msgs, res = h.ProcessBatch(context.Background(), msg)
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "bar", msgs[0].Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Metadata not preserved: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).MetaGetStr("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}
}

func TestHTTPClientAlternativePayload(t *testing.T) {
	i := 0
	expPayloads := []string{"bar"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.Header().Add("foobar", "baz")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
  retry_period: 1ms
  retries: 3
  payload: ${!"ba" + "r"}
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).MetaGetStr("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}
}

func TestHTTPClientEmptyResponse(t *testing.T) {
	i := 0
	expPayloads := []string{"foo", "bar", "baz"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "200", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}

	msgs, res = h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "200", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}

	// Check metadata persists.
	msg := message.QuickBatch([][]byte{[]byte("baz")})
	msg.Get(0).MetaSetMut("foo", "bar")
	msgs, res = h.ProcessBatch(context.Background(), msg)
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "200", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}
}

func TestHTTPClientEmpty404Response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foo", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "404", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else {
		assert.Error(t, msgs[0].Get(0).ErrorGet())
	}
}

func TestHTTPClientBasicWithMetadata(t *testing.T) {
	i := 0
	expPayloads := []string{"foo", "bar", "baz"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.Header().Add("foobar", "baz")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
  extract_headers:
    include_patterns: [ ".*" ]
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "baz", msgs[0].Get(0).MetaGetStr("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}
}

func TestHTTPClientSerial(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		if string(bodyBytes) == "bar" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar " + string(bodyBytes)))
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
  retry_period: 1ms
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	inputMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	})
	inputMsg.Get(0).MetaSetMut("foo", "bar")
	msgs, res := h.ProcessBatch(context.Background(), inputMsg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	require.Equal(t, 5, msgs[0].Len())

	assert.Equal(t, "foobar foo", string(msgs[0].Get(0).AsBytes()))
	assert.Equal(t, "bar", string(msgs[0].Get(1).AsBytes()))
	require.Error(t, msgs[0].Get(1).ErrorGet())
	assert.Contains(t, msgs[0].Get(1).ErrorGet().Error(), "request returned unexpected response code")
	assert.Equal(t, "foobar baz", string(msgs[0].Get(2).AsBytes()))
	assert.Equal(t, "foobar qux", string(msgs[0].Get(3).AsBytes()))
	assert.Equal(t, "foobar quz", string(msgs[0].Get(4).AsBytes()))
}

func TestHTTPClientParallel(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		wg.Wait()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
  parallel: true
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	inputMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	})
	inputMsg.Get(0).MetaSetMut("foo", "bar")
	msgs, res := h.ProcessBatch(context.Background(), inputMsg)
	if res != nil {
		t.Error(res)
	} else if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "bar", msgs[0].Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Metadata not preserved: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}
}

func TestHTTPClientParallelError(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		wg.Wait()
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(reqBytes) == "baz" {
			http.Error(w, "test error", http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := parseYAMLProcConf(t, `
http:
  url: %v/testpost
  parallel: true
  retries: 0
`, ts.URL)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	}))
	if res != nil {
		t.Error(res)
	}
	if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Fatalf("Wrong result count: %v != %v", actC, expC)
	}
	if exp, act := "baz", string(msgs[0].Get(2).AsBytes()); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	assert.Error(t, msgs[0].Get(2).ErrorGet())
	if exp, act := "403", msgs[0].Get(2).MetaGetStr("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}
	for _, i := range []int{0, 1, 3, 4} {
		if exp, act := "foobar", string(msgs[0].Get(i).AsBytes()); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
		assert.NoError(t, msgs[0].Get(i).ErrorGet())
		if exp, act := "200", msgs[0].Get(i).MetaGetStr("http_status_code"); exp != act {
			t.Errorf("Wrong response code metadata: %v != %v", act, exp)
		}
	}
}

// A forced stream stop cancels the context messages are processed with, then closes the stream
// resources, rate limits included. A request still waiting on the rate limit after that can no
// longer find it, logs the error and retries once per second, forever: the wait must end with the
// context instead.
func TestHTTPProcessorRateLimitWaitEndsWithContext(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("foobar"))
	}))
	t.Cleanup(ts.Close)

	tests := []struct {
		name     string
		parallel bool
		parts    int
	}{
		{name: "single request", parts: 1},
		{name: "serial batch", parts: 3},
		{name: "parallel batch", parallel: true, parts: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mgr := mock.NewManager()
			// A saturated rate limit: every access is told to wait a little longer.
			mgr.RateLimits["saturated"] = mock.RateLimit(func(context.Context) (time.Duration, error) {
				return 10 * time.Millisecond, nil
			})

			h, err := mgr.NewProcessor(parseYAMLProcConf(t, `
http:
  url: %v/testpost
  rate_limit: saturated
  parallel: %v
`, ts.URL, test.parallel))
			require.NoError(t, err)
			t.Cleanup(func() { _ = h.Close(context.Background()) })

			parts := make([][]byte, test.parts)
			for i := range parts {
				parts[i] = []byte("foo")
			}

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = h.ProcessBatch(ctx, message.QuickBatch(parts))
			}()

			time.Sleep(50 * time.Millisecond)
			cancel()
			require.NoError(t, mgr.RemoveRateLimit(t.Context(), "saturated"))

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("ProcessBatch kept waiting on the rate limit after its context was cancelled")
			}
		})
	}
}
