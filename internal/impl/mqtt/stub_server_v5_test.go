package mqtt

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/eclipse/paho.golang/packets"
)

// stubServerV5 is a minimal MQTT 5 server that answers with whatever reason
// codes a test asks it for.
//
// It exists because no real broker will refuse on demand. mosquitto grants a
// SUBSCRIBE at the requested QoS even for a topic its own ACL denies, and then
// silently delivers nothing — so an ACL fixture proves the opposite of what it
// looks like it proves. And nothing at all will produce a PUBREC carrying a
// failure code to order, which is the case that matters most here, because the
// client library reports it to the caller as a success.
//
// It speaks only enough of the protocol to get a client connected, subscribed
// and publishing. It is not a broker and should never grow into one.
type stubServerV5 struct {
	listener net.Listener

	// Fixed at construction and never written afterwards, so the goroutine
	// serving connections and the test setting them up cannot race. An earlier
	// version assigned these on the returned struct and did race.
	cfg stubConfig

	mut      sync.Mutex
	received []*packets.Publish

	wg   sync.WaitGroup
	done chan struct{}
}

// stubConfig says how the server should answer. Every zero value is the
// accepting one, so a test states only what it wants to be refused.
type stubConfig struct {
	// connack is the reason code returned to every CONNECT.
	connack byte
	// suback, when set, is returned to every SUBSCRIBE in place of granting the
	// requested QoS. One code per requested filter; a shorter list is padded
	// with its final entry.
	suback []byte
	// puback and pubrec are the reason codes returned to QoS 1 and QoS 2
	// publications.
	puback byte
	pubrec byte
}

func newStubServerV5(t *testing.T, cfg stubConfig) *stubServerV5 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("stub server could not listen: %v", err)
	}
	s := &stubServerV5{listener: listener, cfg: cfg, done: make(chan struct{})}
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(s.close)
	return s
}

func (s *stubServerV5) url() string {
	return "tcp://" + s.listener.Addr().String()
}

func (s *stubServerV5) publishes() []*packets.Publish {
	s.mut.Lock()
	defer s.mut.Unlock()
	return append([]*packets.Publish(nil), s.received...)
}

func (s *stubServerV5) close() {
	select {
	case <-s.done:
		return
	default:
		close(s.done)
	}
	_ = s.listener.Close()
	s.wg.Wait()
}

func (s *stubServerV5) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wg.Go(func() {
			defer func() { _ = conn.Close() }()
			s.serve(conn)
		})
	}
}

func (s *stubServerV5) serve(conn net.Conn) {
	for {
		select {
		case <-s.done:
			return
		default:
		}

		pkt, err := packets.ReadPacket(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				return
			}
			return
		}

		switch content := pkt.Content.(type) {
		case *packets.Connect:
			out := packets.NewControlPacket(packets.CONNACK)
			out.Content.(*packets.Connack).ReasonCode = s.cfg.connack
			if _, err := out.WriteTo(conn); err != nil {
				return
			}
			if s.cfg.connack >= 0x80 {
				return // A refused connection is not carried on with.
			}

		case *packets.Subscribe:
			out := packets.NewControlPacket(packets.SUBACK)
			suback := out.Content.(*packets.Suback)
			suback.PacketID = content.PacketID
			suback.Reasons = s.reasonsFor(content)
			if _, err := out.WriteTo(conn); err != nil {
				return
			}

		case *packets.Publish:
			s.mut.Lock()
			s.received = append(s.received, content)
			s.mut.Unlock()

			switch content.QoS {
			case 1:
				out := packets.NewControlPacket(packets.PUBACK)
				ack := out.Content.(*packets.Puback)
				ack.PacketID = content.PacketID
				ack.ReasonCode = s.cfg.puback
				if _, err := out.WriteTo(conn); err != nil {
					return
				}
			case 2:
				out := packets.NewControlPacket(packets.PUBREC)
				rec := out.Content.(*packets.Pubrec)
				rec.PacketID = content.PacketID
				rec.ReasonCode = s.cfg.pubrec
				if _, err := out.WriteTo(conn); err != nil {
					return
				}
			}

		case *packets.Pubrel:
			out := packets.NewControlPacket(packets.PUBCOMP)
			comp := out.Content.(*packets.Pubcomp)
			comp.PacketID = content.PacketID
			if _, err := out.WriteTo(conn); err != nil {
				return
			}

		case *packets.Pingreq:
			out := packets.NewControlPacket(packets.PINGRESP)
			if _, err := out.WriteTo(conn); err != nil {
				return
			}

		case *packets.Disconnect:
			return
		}
	}
}

// reasonsFor answers one code per requested filter. Granting at the requested
// QoS is the default, so a test only has to say what it wants to be different.
func (s *stubServerV5) reasonsFor(sub *packets.Subscribe) []byte {
	reasons := make([]byte, 0, len(sub.Subscriptions))
	for i, requested := range sub.Subscriptions {
		switch {
		case len(s.cfg.suback) == 0:
			reasons = append(reasons, requested.QoS)
		case i < len(s.cfg.suback):
			reasons = append(reasons, s.cfg.suback[i])
		default:
			reasons = append(reasons, s.cfg.suback[len(s.cfg.suback)-1])
		}
	}
	return reasons
}

// stubInputYAML builds an input configuration pointed at the stub.
func stubInputYAML(s *stubServerV5, topics, extra string) string {
	return fmt.Sprintf(`
urls: [ %v ]
topics: %v
client_id: stub-probe
qos: 1
connect_timeout: 5s
%v
`, s.url(), topics, extra)
}
