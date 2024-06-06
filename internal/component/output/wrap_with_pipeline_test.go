package output_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/pipeline"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

type mockOutput struct {
	ts <-chan message.Transaction
}

func (m *mockOutput) Consume(ts <-chan message.Transaction) error {
	m.ts = ts
	return nil
}

func (m *mockOutput) Connected() bool {
	return true
}

func (m *mockOutput) TriggerCloseNow() {
	// NOT EXPECTING TO HIT THIS
}

func (m *mockOutput) WaitForClose(ctx context.Context) error {
	select {
	case _, open := <-m.ts:
		if open {
			return errors.New("messages chan still open")
		}
	case <-ctx.Done():
		return errors.New("timed out")
	}
	return nil
}

//------------------------------------------------------------------------------

type mockPipe struct {
	tsIn      <-chan message.Transaction
	ts        chan message.Transaction
	closeOnce sync.Once
}

func (m *mockPipe) Consume(ts <-chan message.Transaction) error {
	m.tsIn = ts
	return nil
}

func (m *mockPipe) TransactionChan() <-chan message.Transaction {
	return m.ts
}

func (m *mockPipe) TriggerCloseNow() {
	m.closeOnce.Do(func() {
		close(m.ts)
	})
}

func (m *mockPipe) WaitForClose(ctx context.Context) error {
	return errors.New("not expecting to see this")
}

//------------------------------------------------------------------------------

func TestBasicWrapPipeline(t *testing.T) {
	mockOut := &mockOutput{}
	mockPi := &mockPipe{
		ts: make(chan message.Transaction),
	}

	_, err := output.WrapWithPipeline(mockOut, func() (processor.Pipeline, error) {
		return nil, errors.New("nope")
	})
	if err == nil {
		t.Error("expected error from back constructor")
	}

	newOutput, err := output.WrapWithPipeline(mockOut, func() (processor.Pipeline, error) {
		return mockPi, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	dudMsgChan := make(chan message.Transaction)
	if err = newOutput.Consume(dudMsgChan); err != nil {
		t.Error(err)
	}

	if mockPi.tsIn != dudMsgChan {
		t.Error("Wrong message chan in mock pipe")
	}

	if mockOut.ts != mockPi.ts {
		t.Error("Wrong messages chan in mock pipe")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	close(dudMsgChan)
	mockPi.TriggerCloseNow()
	newOutput.TriggerCloseNow()
	require.NoError(t, newOutput.WaitForClose(ctx))
}

func TestBasicWrapPipelinesOrdering(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOut := &mockOutput{}

	firstProc, err := testutil.ProcessorFromYAML(`
insert_part:
  content: foo
  index: 0
`)
	require.NoError(t, err)

	secondProc, err := testutil.ProcessorFromYAML(`
select_parts:
  parts: [ 0 ]
`)
	require.NoError(t, err)

	conf := output.NewConfig()
	conf.Processors = append(conf.Processors, firstProc)

	newOutput, err := output.WrapWithPipelines(
		mockOut,
		func() (processor.Pipeline, error) {
			proc, err := mock.NewManager().NewProcessor(firstProc)
			if err != nil {
				return nil, err
			}
			return pipeline.NewProcessor(proc), nil
		},
		func() (processor.Pipeline, error) {
			proc, err := mock.NewManager().NewProcessor(secondProc)
			if err != nil {
				return nil, err
			}
			return pipeline.NewProcessor(proc), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan message.Transaction)
	resChan := make(chan error)
	if err = newOutput.Consume(tChan); err != nil {
		t.Error(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case tChan <- message.NewTransaction(
		message.QuickBatch([][]byte{[]byte("bar")}), resChan,
	):
	}

	var tran message.Transaction
	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case tran = <-mockOut.ts:
	}

	exp := [][]byte{
		[]byte("foo"),
	}
	if act := message.GetAllBytes(tran.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong contents: %s != %s", act, exp)
	}

	go func() {
		require.NoError(t, tran.Ack(ctx, nil))
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out")
	case res := <-resChan:
		if res != nil {
			t.Error(res)
		}
	}

	close(tChan)
	newOutput.TriggerCloseNow()
	require.NoError(t, newOutput.WaitForClose(ctx))
}
