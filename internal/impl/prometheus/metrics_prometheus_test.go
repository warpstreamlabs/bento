package prometheus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func promFromYAML(t testing.TB, conf string, args ...any) *Metrics {
	t.Helper()

	pConf, err := ConfigSpec().ParseYAML(fmt.Sprintf(conf, args...), nil)
	require.NoError(t, err)

	p, err := FromParsed(pConf, nil)
	require.NoError(t, err)

	return p
}

func TestPrometheusNoPushGateway(t *testing.T) {
	p := promFromYAML(t, ``)
	assert.NotNil(t, p)
	assert.Nil(t, p.pusher)
}

// pushGateway is a stand-in push gateway that counts the pushes it receives
// and wakes the test on each one. The handler never blocks, so pushes that the
// test is not waiting for cannot stall the pusher or the server shutdown.
type pushGateway struct {
	*httptest.Server
	pushes atomic.Int64
	pushed chan struct{}
}

func newPushGateway(t *testing.T) *pushGateway {
	t.Helper()

	g := &pushGateway{pushed: make(chan struct{}, 1)}
	g.Server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		g.pushes.Add(1)
		select {
		case g.pushed <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(g.Close)
	return g
}

// requirePushes waits until at least n pushes have been received.
func (g *pushGateway) requirePushes(t *testing.T, n int64) {
	t.Helper()

	timeout := time.After(time.Second * 5)
	for g.pushes.Load() < n {
		select {
		case <-g.pushed:
		case <-timeout:
			require.FailNowf(t, "PushGateway did not receive expected messages", "want at least %v pushes, got %v", n, g.pushes.Load())
		}
	}
}

func TestPrometheusWithPushGateway(t *testing.T) {
	gateway := newPushGateway(t)

	p := promFromYAML(t, `
push_url: %v
`, gateway.URL)
	assert.NotNil(t, p.pusher)

	// Close pushes synchronously, so the push has been served by the time it
	// returns.
	require.NoError(t, p.Close(context.Background()))
	assert.Equal(t, int64(1), gateway.pushes.Load())
}

func TestPrometheusWithPushGatewayAndPushInterval(t *testing.T) {
	gateway := newPushGateway(t)

	pushInterval := 1 * time.Millisecond
	p := promFromYAML(t, `
push_url: %v
push_interval: %v
`, gateway.URL, pushInterval.String())
	assert.NotNil(t, p.pusher)

	// Wait for the first periodic push
	gateway.requirePushes(t, 1)

	// Close stops the periodic pushes and pushes once more before returning
	before := gateway.pushes.Load()
	require.NoError(t, p.Close(context.Background()))
	assert.Greater(t, gateway.pushes.Load(), before)
}

func getTestProm(t *testing.T) (*Metrics, http.HandlerFunc) {
	t.Helper()

	prom := promFromYAML(t, ``)
	return prom, prom.HandlerFunc()
}

func getPage(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "http://example.com/foo", http.NoBody)
	w := httptest.NewRecorder()
	handler(w, req)

	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)

	return string(body)
}

type floatCtorExpanded interface {
	IncrFloat64(f float64)
}

type floatGagExpanded interface {
	SetFloat64(f float64)
}

func TestPrometheusMetrics(t *testing.T) {
	nm, handler := getTestProm(t)

	ctr := nm.NewCounterCtor("counterone")()
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.NewGaugeCtor("gaugeone")()
	gge.Set(12)

	tmr := nm.NewTimerCtor("timerone")()
	tmr.Timing(13)

	ctrTwo := nm.NewCounterCtor("countertwo", "label1")
	ctrTwo("value1").Incr(10)
	ctrTwo("value2").Incr(11)
	ctrTwo("value3").(floatCtorExpanded).IncrFloat64(10.452)

	ggeTwo := nm.NewGaugeCtor("gaugetwo", "label2")
	ggeTwo("value3").Set(12)

	ggeThree := nm.NewGaugeCtor("gaugethree")()
	ggeThree.(floatGagExpanded).SetFloat64(10.452)

	tmrTwo := nm.NewTimerCtor("timertwo", "label3", "label4")
	tmrTwo("value4", "value5").Timing(13)

	body := getPage(t, handler)

	assert.Contains(t, body, "\ncounterone 21")
	assert.Contains(t, body, "\ngaugeone 12")
	assert.Contains(t, body, "\ntimerone_count 1")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\"} 11")
	assert.Contains(t, body, "\ncountertwo{label1=\"value3\"} 10.452")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\"} 12")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\"} 13")
	assert.Contains(t, body, "\ngaugethree 10.452")
}

func TestPrometheusHistMetrics(t *testing.T) {
	nm := promFromYAML(t, `
use_histogram_timing: true
`)

	applyTestMetrics(nm)

	tmr := nm.NewTimerCtor("timerone")()
	tmr.Timing(13)
	tmrTwo := nm.NewTimerCtor("timertwo", "label3", "label4")
	tmrTwo("value4", "value5").Timing(14)

	handler := nm.HandlerFunc()
	body := getPage(t, handler)

	assertContainsTestMetrics(t, body)
	assert.Contains(t, body, "\ntimerone_sum 1.3e-08")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\"} 1.4e-08")
}

func TestPrometheusWithFileOutputPath(t *testing.T) {
	fPath := t.TempDir() + "/bento_metrics.prom"

	p := promFromYAML(t, `
file_output_path: %v
`, fPath)
	applyTestMetrics(p)

	assert.Nil(t, p.pusher)

	err := p.Close(context.Background())
	assert.NoError(t, err)

	assert.FileExists(t, fPath)
	file, err := os.ReadFile(fPath)
	assert.NoError(t, err)
	assert.NotEmpty(t, file)

	assertContainsTestMetrics(t, string(file))
}

func TestPrometheusDefaultRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = reg
	prometheus.DefaultRegisterer.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "some_other_metric",
		Help: "a metric not created by bento",
	}))

	p := promFromYAML(t, `
use_default_registry: true
`)
	applyTestMetrics(p)

	handler := p.HandlerFunc()
	body := getPage(t, handler)

	assertContainsTestMetrics(t, body)
	assert.Contains(t, body, "\nsome_other_metric 0")
}

func applyTestMetrics(nm *Metrics) {
	ctr := nm.NewCounterCtor("counterone")()
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.NewGaugeCtor("gaugeone")()
	gge.Set(12)

	ctrTwo := nm.NewCounterCtor("countertwo", "label1")
	ctrTwo("value1").Incr(10)
	ctrTwo("value2").Incr(11)

	ggeTwo := nm.NewGaugeCtor("gaugetwo", "label2")
	ggeTwo("value3").Set(12)
}

func assertContainsTestMetrics(t *testing.T, body string) {
	assert.Contains(t, body, "\ncounterone 21")
	assert.Contains(t, body, "\ngaugeone 12")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\"} 11")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\"} 12")
}
