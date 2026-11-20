package mqtt

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// countingBrokerV3 accepts MQTT 3.1.1 connections, answers CONNECT with
// CONNACK and otherwise only counts how many connections were made and how
// many are still open.
type countingBrokerV3 struct {
	ln net.Listener

	mut      sync.Mutex
	accepted int
	open     int
	conns    []net.Conn
}

func newCountingBrokerV3(t *testing.T) *countingBrokerV3 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	b := &countingBrokerV3{ln: ln}
	go b.accept()
	t.Cleanup(b.close)
	return b
}

func (b *countingBrokerV3) accept() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		b.mut.Lock()
		b.accepted++
		b.open++
		b.conns = append(b.conns, conn)
		b.mut.Unlock()
		go b.serve(conn)
	}
}

func (b *countingBrokerV3) serve(conn net.Conn) {
	defer func() {
		b.mut.Lock()
		b.open--
		b.mut.Unlock()
		conn.Close()
	}()
	if _, err := packets.ReadPacket(conn); err != nil { // CONNECT
		return
	}
	if err := packets.NewControlPacket(packets.Connack).Write(conn); err != nil {
		return
	}
	for {
		pkt, err := packets.ReadPacket(conn)
		if err != nil {
			return
		}
		switch pkt.(type) {
		case *packets.PingreqPacket:
			if err := packets.NewControlPacket(packets.Pingresp).Write(conn); err != nil {
				return
			}
		case *packets.DisconnectPacket:
			return
		}
	}
}

func (b *countingBrokerV3) counts() (accepted, open int) {
	b.mut.Lock()
	defer b.mut.Unlock()
	return b.accepted, b.open
}

func (b *countingBrokerV3) close() {
	b.ln.Close()
	b.mut.Lock()
	defer b.mut.Unlock()
	for _, c := range b.conns {
		c.Close()
	}
}

// TestInputFailedSubscribeReconnectsWithoutLeaking covers a subscription that
// paho fails before sending anything (a '#' that is not the last level). Each
// time Read reports the connection gone, the next Connect must make a new
// connection, and the old one must not stay open.
func TestInputFailedSubscribeReconnectsWithoutLeaking(t *testing.T) {
	broker := newCountingBrokerV3(t)
	res, _ := capturedLogger()
	conf, err := inputConfigSpec().ParseYAML(fmt.Sprintf(`
urls: [ "tcp://%s" ]
topics: [ "a/#/b" ]
`, broker.ln.Addr().String()), nil)
	require.NoError(t, err)
	rdr, err := newMQTTReaderFromParsed(conf, res)
	require.NoError(t, err)
	ctx := testCtx(t)

	// What AsyncReader does each time Read returns ErrNotConnected.
	const attempts = 5
	for range attempts {
		require.NoError(t, rdr.Connect(ctx))
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		_, _, err := rdr.Read(readCtx)
		cancel()
		require.ErrorIs(t, err, service.ErrNotConnected)
	}
	require.NoError(t, rdr.Close(ctx))

	// Disconnecting is asynchronous, so give the last client a moment to go.
	var accepted, open int
	deadline := time.Now().Add(2 * time.Second)
	for {
		accepted, open = broker.counts()
		if open == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(t, attempts, accepted, "connections made")
	assert.Equal(t, 0, open, "connections still open at the broker after Close")
}
