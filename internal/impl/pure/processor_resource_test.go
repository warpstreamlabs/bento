package pure_test

import (
	"context"
	"testing"

	"github.com/warpstreamlabs/bento/v1/internal/component/processor"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
	"github.com/warpstreamlabs/bento/v1/internal/message"

	_ "github.com/warpstreamlabs/bento/v1/internal/impl/pure"
)

func TestResourceProc(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "bloblang"
	conf.Plugin = `root = "foo: " + content()`

	mgr := mock.NewManager()

	resProc, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	mgr.Processors["foo"] = func(b message.Batch) ([]message.Batch, error) {
		msgs, res := resProc.ProcessBatch(context.Background(), b)
		if res != nil {
			return nil, res
		}
		return msgs, nil
	}

	nConf := processor.NewConfig()
	nConf.Type = "resource"
	nConf.Plugin = "foo"

	p, err := mgr.NewProcessor(nConf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := p.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Fatal(res)
	}
	if len(msgs) != 1 {
		t.Error("Expected only 1 message")
	}
	if exp, act := "foo: bar", string(msgs[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestResourceBadName(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "resource"
	conf.Plugin = "foo"

	_, err := mock.NewManager().NewProcessor(conf)
	if err == nil {
		t.Error("expected error from bad resource")
	}
}
