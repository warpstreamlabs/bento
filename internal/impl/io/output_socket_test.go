package io

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/public/service"
)

func socketWriterFromConf(t testing.TB, confStr string, bits ...any) *socketWriter {
	t.Helper()

	conf, err := socketOutputSpec().ParseYAML(fmt.Sprintf(confStr, bits...), nil)
	require.NoError(t, err)

	w, err := newSocketWriterFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	return w
}

func TestSocketBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "bento.sock"))
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	wtr := socketWriterFromConf(t, `
network: %v
address: %v
`, ln.Addr().Network(), ln.Addr().String())

	defer func() {
		if err := wtr.Close(ctx); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.Connect(context.Background()); cerr != nil {
			t.Error(cerr)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Go(func() {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(conn)
	})

	if err = wtr.Write(context.Background(), service.NewMessage([]byte("foo"))); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(context.Background(), service.NewMessage([]byte("bar\n"))); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(context.Background(), service.NewMessage([]byte("baz"))); err != nil {
		t.Error(err)
	}

	require.NoError(t, wtr.Close(ctx))
	wg.Wait()

	exp := "foo\nbar\nbaz\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}

type testOutputWrapPacketConn struct {
	r net.PacketConn
}

func (w *testOutputWrapPacketConn) Read(p []byte) (n int, err error) {
	n, _, err = w.r.ReadFrom(p)
	return
}

func TestUDPSocketBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		if conn, err = net.ListenPacket("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer conn.Close()

	wtr := socketWriterFromConf(t, `
network: udp
address: %v
`, conn.LocalAddr().String())

	defer func() {
		if err := wtr.Close(ctx); err != nil {
			t.Error(err)
		}
	}()

	if cerr := wtr.Connect(context.Background()); cerr != nil {
		t.Fatal(cerr)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Go(func() {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(&testOutputWrapPacketConn{r: conn})
	})

	if err = wtr.Write(context.Background(), service.NewMessage([]byte("foo"))); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(context.Background(), service.NewMessage([]byte("bar\n"))); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(context.Background(), service.NewMessage([]byte("baz"))); err != nil {
		t.Error(err)
	}

	require.NoError(t, wtr.Close(ctx))
	wg.Wait()

	exp := "foo\nbar\nbaz\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}

func TestTCPSocketBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if ln, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer ln.Close()

	wtr := socketWriterFromConf(t, `
network: tcp
address: %v
`, ln.Addr().String())

	defer func() {
		if err := wtr.Close(ctx); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.Connect(context.Background()); cerr != nil {
			t.Error(cerr)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Go(func() {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(conn)
	})

	if err = wtr.Write(context.Background(), service.NewMessage([]byte("foo"))); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(context.Background(), service.NewMessage([]byte("bar\n"))); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(context.Background(), service.NewMessage([]byte("baz"))); err != nil {
		t.Error(err)
	}

	require.NoError(t, wtr.Close(ctx))
	wg.Wait()

	exp := "foo\nbar\nbaz\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}

func TestTLSSocketBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	cert, err := createSelfSignedCertificate()
	require.NoError(t, err)

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	require.NoError(t, err)
	defer ln.Close()

	wtr := socketWriterFromConf(t, `
network: tcp
address: %v
tls:
  enabled: true
  skip_cert_verify: true
`, ln.Addr().String())

	defer func() {
		if err := wtr.Close(ctx); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.Connect(context.Background()); cerr != nil {
			t.Error(cerr)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Go(func() {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(conn)
	})

	// Connect runs in the background, wait for it before writing.
	require.Eventually(t, func() bool {
		wtr.writerMut.Lock()
		defer wtr.writerMut.Unlock()
		return wtr.writer != nil
	}, time.Second*5, time.Millisecond*10)

	if err = wtr.Write(context.Background(), service.NewMessage([]byte("foo"))); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(context.Background(), service.NewMessage([]byte("bar\n"))); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(context.Background(), service.NewMessage([]byte("baz"))); err != nil {
		t.Error(err)
	}

	require.NoError(t, wtr.Close(ctx))
	wg.Wait()

	exp := "foo\nbar\nbaz\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}

func TestTLSSocketUntrustedServer(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	cert, err := createSelfSignedCertificate()
	require.NoError(t, err)

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_ = conn.(*tls.Conn).Handshake()
		conn.Close()
	}()

	// Without skip_cert_verify the self signed certificate must be rejected.
	wtr := socketWriterFromConf(t, `
network: tcp
address: %v
tls:
  enabled: true
`, ln.Addr().String())

	require.Error(t, wtr.Connect(ctx))
	require.ErrorIs(t, wtr.Write(ctx, service.NewMessage([]byte("foo"))), component.ErrNotConnected)
}

func TestSocketTLSRequiresTCP(t *testing.T) {
	for _, network := range []string{"udp", "unix"} {
		conf, err := socketOutputSpec().ParseYAML(fmt.Sprintf(`
network: %v
address: 127.0.0.1:6000
tls:
  enabled: true
`, network), nil)
		require.NoError(t, err)

		_, err = newSocketWriterFromParsed(conf, service.MockResources())
		require.Error(t, err, network)
	}
}

func TestSocketTLSLintRuleErr(t *testing.T) {
	builder := service.NewStreamBuilder()

	err := builder.SetYAML(`
input:
  stdin: {}
output:
  socket:
    network: udp
    address: 127.0.0.1:6000
    tls:
      enabled: true
`)
	require.ErrorContains(t, err, "tls can only be enabled when network is tcp")
}

func TestSocketTLSLintRuleHappy(t *testing.T) {
	builder := service.NewStreamBuilder()

	err := builder.SetYAML(`
input:
  stdin: {}
output:
  socket:
    network: tcp
    address: 127.0.0.1:6000
    tls:
      enabled: true
`)
	require.NoError(t, err)
}
