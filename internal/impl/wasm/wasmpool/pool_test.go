package wasmpool

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

type testModule struct {
	api.Module
	closed atomic.Bool
	closes atomic.Int32
}

func (m *testModule) Close(context.Context) error {
	m.closes.Add(1)
	m.closed.Store(true)
	return nil
}

func TestCleanupMustNotCloseBorrowedModule(t *testing.T) {
	ctx := context.Background()
	p, err := NewWasmModulePool(ctx, func(context.Context) (*testModule, error) { return &testModule{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && !acquired.closed.Load(); i++ {
		runtime.GC()
		runtime.Gosched()
	}
	runtime.KeepAlive(p)
	runtime.KeepAlive(acquired)
	if acquired.closed.Load() {
		t.Fatal("borrowed module was closed by cleanup while still in use")
	}
}

func TestCleanupMustNotCloseBorrowedWazeroModule(t *testing.T) {
	ctx := context.Background()
	r := wazero.NewRuntime(ctx)
	defer r.Close(ctx)
	p, err := NewWasmModulePool(ctx, func(ctx context.Context) (api.Module, error) {
		return r.Instantiate(ctx, []byte{0, 97, 115, 109, 1, 0, 0, 0})
	})
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && !acquired.IsClosed(); i++ {
		runtime.GC()
		runtime.Gosched()
	}
	runtime.KeepAlive(p)
	runtime.KeepAlive(acquired)
	if acquired.IsClosed() {
		t.Fatal("real wazero module was closed while borrowed")
	}
}

func TestCleanupClosesDiscardedIdleModule(t *testing.T) {
	ctx := context.Background()
	m := &testModule{}
	p, err := NewWasmModulePool(ctx, func(context.Context) (*testModule, error) { return m, nil })
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !m.closed.Load() && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	runtime.KeepAlive(p)
	if !m.closed.Load() {
		t.Fatal("discarded idle module was not closed")
	}
}

func TestCloseCancelsCleanup(t *testing.T) {
	ctx := context.Background()
	m := &testModule{}
	p, err := NewWasmModulePool(ctx, func(context.Context) (*testModule, error) { return m, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		runtime.GC()
		runtime.Gosched()
	}
	if got := m.closes.Load(); got != 1 {
		t.Fatalf("module closed %d times, want 1", got)
	}
}
