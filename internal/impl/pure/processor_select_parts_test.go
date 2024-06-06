package pure_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestSelectParts(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
select_parts:
  parts: [ 1, 3 ]
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Error(err)
		return
	}

	type test struct {
		in  [][]byte
		out [][]byte
	}

	tests := []test{
		{
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
			},
			out: [][]byte{
				[]byte("1"),
				[]byte("3"),
			},
		},
		{
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("1"),
			},
		},
		{
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
			},
			out: [][]byte{
				[]byte("1"),
				[]byte("3"),
			},
		},
	}

	for _, test := range tests {
		msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(test.in))
		if len(msgs) != 1 {
			t.Errorf("Select Parts failed on: %s", test.in)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := test.out, message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
			t.Errorf("Unexpected output: %s != %s", act, exp)
		}
	}
}

func TestSelectPartsIndexBounds(t *testing.T) {
	input := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
	}

	tests := map[int]string{
		-5: "0",
		-4: "1",
		-3: "2",
		-2: "3",
		-1: "4",
		0:  "0",
		1:  "1",
		2:  "2",
		3:  "3",
		4:  "4",
	}

	for i, exp := range tests {
		conf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
select_parts:
  parts: [ %v ]
`, i))
		require.NoError(t, err)

		proc, err := mock.NewManager().NewProcessor(conf)
		if err != nil {
			t.Fatal(err)
		}

		msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(input))
		if len(msgs) != 1 {
			t.Errorf("Select Parts failed on index: %v", i)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if act := string(message.GetAllBytes(msgs[0])[0]); exp != act {
			t.Errorf("Unexpected output for index %v: %v != %v", i, act, exp)
		}
	}
}

func TestSelectPartsEmpty(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
select_parts:
  parts: [ 3 ]
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Error(err)
		return
	}

	msgs, _ := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo")}))
	if len(msgs) != 0 {
		t.Error("Expected failure with zero parts selected")
	}
}
