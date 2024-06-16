package io

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func inputFromConf(t testing.TB, confStr string, bits ...any) input.Streamed {
	t.Helper()

	conf, err := testutil.InputFromYAML(fmt.Sprintf(confStr, bits...))
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)
	return s
}

func TestSocketInputBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "bento.sock"))
	if err != nil {
		t.Fatalf("failed to listen on a address: %v", err)
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: %v
  address: %v
`, ln.Addr().Network(), ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("bar")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestSocketInputReconnect(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "bento.sock"))
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: %v
  address: %v
`, ln.Addr().Network(), ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		if cerr != nil {
			t.Error(cerr)
		}
		conn.Close()
		conn, cerr = ln.Accept()
		require.NoError(t, cerr)

		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("bar")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestSocketInputMultipart(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "bento.sock"))
	if err != nil {
		t.Fatalf("failed to listen on a port: %v", err)
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: %v
  address: %v
  codec: lines/multipart
`, ln.Addr().Network(), ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n\n")); cerr != nil {
			t.Error(cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestSocketMultipartCustomDelim(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "b.sock"))
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: %v
  address: %v
  codec: delim:@/multipart
`, ln.Addr().Network(), ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n@@")); cerr != nil {
			t.Error(cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz\n")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestSocketMultipartShutdown(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "bento.sock"))
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: %v
  address: %v
  codec: lines/multipart
`, ln.Addr().Network(), ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
		conn.Close()
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out on read")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
}

func TestTCPSocketInputBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if ln, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: tcp
  address: %v
`, ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("bar")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestTCPSocketReconnect(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if ln, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: tcp
  address: %v
`, ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		if cerr != nil {
			t.Error(cerr)
		}
		conn.Close()
		conn, cerr = ln.Accept()
		if cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("bar")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestTCPSocketInputMultipart(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if ln, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: tcp
  address: %v
  codec: lines/multipart
`, ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n\n")); cerr != nil {
			t.Error(cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestTCPSocketMultipartCustomDelim(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if ln, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: tcp
  address: %v
  codec: delim:@/multipart
`, ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n@@")); cerr != nil {
			t.Error(cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz\n")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestTCPSocketMultipartShutdown(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if ln, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer ln.Close()

	rdr := inputFromConf(t, `
socket:
  network: tcp
  address: %v
  codec: lines/multipart
`, ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		if err := rdr.WaitForClose(ctx); err != nil {
			t.Error(err)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
		conn.Close()
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var msg message.Batch
		select {
		case tran := <-rdr.TransactionChan():
			msg = tran.Payload.DeepCopy()
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out on read")
		}
		return msg, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
}

func BenchmarkTCPSocketWithCutOff(b *testing.B) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ln, err = net.Listen("tcp6", "[::1]:0")
		require.NoError(b, err)
	}
	b.Cleanup(func() {
		ln.Close()
	})

	rdr := inputFromConf(b, `
socket:
  network: tcp
  address: %v
`, ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(b, rdr.WaitForClose(ctx))
	}()

	conn, err := ln.Accept()
	require.NoError(b, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 60))
		for i := 0; i < b.N; i++ {
			_, cerr := fmt.Fprintf(conn, "hello world this is message %v\n", i)
			assert.NoError(b, cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (string, error) {
		var payload string
		select {
		case tran := <-rdr.TransactionChan():
			payload = string(tran.Payload.Get(0).AsBytes())
			go func() {
				require.NoError(b, tran.Ack(ctx, nil))
			}()
		case <-time.After(time.Second):
			return "", errors.New("timed out")
		}
		return payload, nil
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		exp := fmt.Sprintf("hello world this is message %v", i)
		act, err := readNextMsg()
		assert.NoError(b, err)
		assert.Equal(b, exp, act)
	}

	wg.Wait()
	conn.Close()
}

func BenchmarkTCPSocketNoCutOff(b *testing.B) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ln, err = net.Listen("tcp6", "[::1]:0")
		require.NoError(b, err)
	}
	b.Cleanup(func() {
		ln.Close()
	})

	rdr := inputFromConf(b, `
socket:
  network: tcp
  address: %v
`, ln.Addr().String())

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(b, rdr.WaitForClose(ctx))
	}()

	conn, err := ln.Accept()
	require.NoError(b, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 60))
		for i := 0; i < b.N; i++ {
			_, cerr := fmt.Fprintf(conn, "hello world this is message %v\n", i)
			assert.NoError(b, cerr)
		}
		wg.Done()
	}()

	readNextMsg := func() (string, error) {
		var payload string
		select {
		case tran := <-rdr.TransactionChan():
			payload = string(tran.Payload.Get(0).AsBytes())
			go func() {
				require.NoError(b, tran.Ack(ctx, nil))
			}()
		case <-time.After(time.Second):
			return "", errors.New("timed out")
		}
		return payload, nil
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		exp := fmt.Sprintf("hello world this is message %v", i)
		act, err := readNextMsg()
		assert.NoError(b, err)
		assert.Equal(b, exp, act)
	}

	wg.Wait()
	conn.Close()
}
