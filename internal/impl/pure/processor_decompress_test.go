package pure_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestDecompressBadAlgo(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
decompress:
  algorithm: does not exist
`)
	require.NoError(t, err)

	_, err = mock.NewManager().NewProcessor(conf)
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestDecompressGZIP(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
decompress:
  algorithm: gzip
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		var buf bytes.Buffer

		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(input[i])
		zw.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressPGZIP(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
decompress:
  algorithm: pgzip
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		var buf bytes.Buffer

		zw := pgzip.NewWriter(&buf)
		_, _ = zw.Write(input[i])
		zw.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressSnappy(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
decompress:
  algorithm: snappy
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])
		input[i] = snappy.Encode(nil, input[i])
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressZLIB(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
decompress:
  algorithm: zlib
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		var buf bytes.Buffer

		zw := zlib.NewWriter(&buf)
		_, _ = zw.Write(input[i])
		zw.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressFlate(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
decompress:
  algorithm: flate
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		var buf bytes.Buffer

		zw, err := flate.NewWriter(&buf, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = zw.Write(input[i])
		zw.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestDecompressLZ4(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
decompress:
  algorithm: lz4
`)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}

	for i := range input {
		exp = append(exp, input[i])

		buf := bytes.Buffer{}
		w := lz4.NewWriter(&buf)
		if _, err := w.Write(input[i]); err != nil {
			w.Close()
			t.Fatalf("Failed to compress input: %s", err)
		}
		w.Close()

		input[i] = buf.Bytes()
	}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Error("Decompress failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}
