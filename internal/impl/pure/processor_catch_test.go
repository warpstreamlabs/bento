package pure_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/v1/internal/component/testutil"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
	"github.com/warpstreamlabs/bento/v1/internal/message"

	_ "github.com/warpstreamlabs/bento/v1/internal/impl/pure"
)

func TestCatchEmpty(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
catch: []
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte("foo bar baz"),
	}
	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(exp))
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	_ = msgs[0].Iter(func(i int, p *message.Part) error {
		if p.ErrorGet() != nil {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchBasic(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
catch:
  - bloblang: 'root = if batch_index() == 0 { content().encode("base64") }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("Zm9vIGJhciBiYXo="),
		[]byte("MSAyIDMgNA=="),
		[]byte("aGVsbG8gZm9vIHdvcmxk"),
	}

	msg := message.QuickBatch(parts)
	_ = msg.Iter(func(i int, p *message.Part) error {
		p.ErrorSet(errors.New("foo"))
		return nil
	})
	msgs, res := proc.ProcessBatch(context.Background(), msg)
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	_ = msgs[0].Iter(func(i int, p *message.Part) error {
		if p.ErrorGet() != nil {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchFilterSome(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
catch:
  - bloblang: 'root = if !content().contains("foo") { deleted() }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("foo bar baz"),
		[]byte("hello foo world"),
	}
	msg := message.QuickBatch(parts)
	_ = msg.Iter(func(i int, p *message.Part) error {
		p.ErrorSet(errors.New("foo"))
		return nil
	})
	msgs, res := proc.ProcessBatch(context.Background(), msg)
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	_ = msgs[0].Iter(func(i int, p *message.Part) error {
		if p.ErrorGet() != nil {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchMultiProcs(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
catch:
  - bloblang: 'root = if !content().contains("foo") { deleted() }'
  - bloblang: 'root = if batch_index() == 0 { content().encode("base64") }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("Zm9vIGJhciBiYXo="),
		[]byte("aGVsbG8gZm9vIHdvcmxk"),
	}
	msg := message.QuickBatch(parts)
	_ = msg.Iter(func(i int, p *message.Part) error {
		p.ErrorSet(errors.New("foo"))
		return nil
	})
	msgs, res := proc.ProcessBatch(context.Background(), msg)
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	_ = msgs[0].Iter(func(i int, p *message.Part) error {
		if p.ErrorGet() != nil {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchNotFails(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
catch:
  - bloblang: 'root = if batch_index() == 0 { content().encode("base64") }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte(`FAILED ENCODE ME PLEASE`),
		[]byte("NOT FAILED, DO NOT ENCODE"),
		[]byte(`FAILED ENCODE ME PLEASE 2`),
	}
	exp := [][]byte{
		[]byte("RkFJTEVEIEVOQ09ERSBNRSBQTEVBU0U="),
		[]byte("NOT FAILED, DO NOT ENCODE"),
		[]byte("RkFJTEVEIEVOQ09ERSBNRSBQTEVBU0UgMg=="),
	}
	msg := message.QuickBatch(parts)
	msg.Get(0).ErrorSet(errors.New("foo"))
	msg.Get(2).ErrorSet(errors.New("foo"))
	msgs, res := proc.ProcessBatch(context.Background(), msg)
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	_ = msgs[0].Iter(func(i int, p *message.Part) error {
		if p.ErrorGet() != nil {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchFilterAll(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
catch:
  - bloblang: 'root = if !content().contains("foo") { deleted() }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello world"),
	}
	msg := message.QuickBatch(parts)
	_ = msg.Iter(func(i int, p *message.Part) error {
		p.ErrorSet(errors.New("foo"))
		return nil
	})
	msgs, res := proc.ProcessBatch(context.Background(), msg)
	assert.NoError(t, res)
	if len(msgs) != 0 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
}
