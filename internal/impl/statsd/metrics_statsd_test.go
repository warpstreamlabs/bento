package statsd

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

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

// setupListener starts a UDP listener on localhost and returns it along with
// a channel that receives every datagram read off the socket.
func setupListener(t testing.TB) (*net.UDPConn, chan []byte) {
	t.Helper()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	out := make(chan []byte, 100)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			packet := make([]byte, n)
			copy(packet, buf[:n])
			out <- packet
		}
	}()

	return conn, out
}

func TestNewStatsdFromParsedWiresTuningOptions(t *testing.T) {
	conn, out := setupListener(t)

	pConf := parseStatsdYAML(t, `
address: %v
flush_period: 1ms
send_queue_capacity: 123
buf_pool_capacity: 45
send_loop_count: 2
`, conn.LocalAddr().String())

	m, err := newStatsdFromParsed(pConf, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = m.Close(context.Background())
	})

	m.NewCounterCtor("test.counter")().Incr(1)

	select {
	case packet := <-out:
		assert.Equal(t, "test.counter:1|c", string(packet))
	case <-time.After(time.Second):
		assert.Fail(t, "timed out waiting for metric packet")
	}
}
