package statsd

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	statsd "github.com/smira/go-statsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func parseStatsdYAML(t testing.TB, conf string, args ...any) *service.ParsedConfig {
	t.Helper()

	pConf, err := statsdSpec().ParseYAML(fmt.Sprintf(conf, args...), nil)
	require.NoError(t, err)
	return pConf
}

func TestStatsdTuningFieldDefaults(t *testing.T) {
	pConf := parseStatsdYAML(t, `address: localhost:8125`)

	sendQueueCapacity, err := pConf.FieldInt(smFieldSendQueueCapacity)
	require.NoError(t, err)
	assert.Equal(t, statsd.DefaultSendQueueCapacity, sendQueueCapacity)

	bufPoolCapacity, err := pConf.FieldInt(smFieldBufPoolCapacity)
	require.NoError(t, err)
	assert.Equal(t, statsd.DefaultBufPoolCapacity, bufPoolCapacity)

	sendLoopCount, err := pConf.FieldInt(smFieldSendLoopCount)
	require.NoError(t, err)
	assert.Equal(t, statsd.DefaultSendLoopCount, sendLoopCount)
}

func TestStatsdTuningFieldOverrides(t *testing.T) {
	pConf := parseStatsdYAML(t, `
address: localhost:8125
send_queue_capacity: 200
buf_pool_capacity: 40
send_loop_count: 4
`)

	sendQueueCapacity, err := pConf.FieldInt(smFieldSendQueueCapacity)
	require.NoError(t, err)
	assert.Equal(t, 200, sendQueueCapacity)

	bufPoolCapacity, err := pConf.FieldInt(smFieldBufPoolCapacity)
	require.NoError(t, err)
	assert.Equal(t, 40, bufPoolCapacity)

	sendLoopCount, err := pConf.FieldInt(smFieldSendLoopCount)
	require.NoError(t, err)
	assert.Equal(t, 4, sendLoopCount)
}

// channelCapacity reads the capacity of an unexported chan field on the vendored
// statsd.Client via reflection. go-statsd doesn't expose SendQueueCapacity/BufPoolCapacity
// once a Client is built, so this is the only way to assert the options passed to
// NewClient actually reached the transport, rather than just asserting config parsing.
func channelCapacity(t testing.TB, c *statsd.Client, fieldName string) int {
	t.Helper()

	trans := reflect.ValueOf(c).Elem().FieldByName("trans").Elem()
	return trans.FieldByName(fieldName).Cap()
}

func TestNewStatsdFromParsedWiresTuningOptions(t *testing.T) {
	pConf := parseStatsdYAML(t, `
address: localhost:8125
send_queue_capacity: 123
buf_pool_capacity: 45
send_loop_count: 2
`)

	m, err := newStatsdFromParsed(pConf, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = m.Close(context.Background())
	})

	assert.Equal(t, 123, channelCapacity(t, m.s, "sendQueue"))
	assert.Equal(t, 45, channelCapacity(t, m.s, "bufPool"))
}
