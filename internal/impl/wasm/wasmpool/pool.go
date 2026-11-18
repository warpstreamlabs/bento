package wasmpool

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/tetratelabs/wazero/api"
)

// pooledInstance owns cleanup only while the module is idle in the pool.
type pooledInstance[T api.Module] struct {
	module  T
	cleanup runtime.Cleanup
}

func (i *pooledInstance[T]) take() T {
	i.cleanup.Stop()
	return i.module
}

type constructor[T api.Module] func(context.Context) (T, error)

type WasmModulePool[T api.Module] struct {
	ctor         constructor[T]
	instancePool sync.Pool
}

func NewWasmModulePool[T api.Module](ctx context.Context, ctor constructor[T]) (*WasmModulePool[T], error) {
	pool := &WasmModulePool[T]{
		instancePool: sync.Pool{},
		ctor:         ctor,
	}

	mod, err := ctor(ctx)
	if err != nil {
		return nil, err
	}

	// warm up with one instance
	pool.Put(mod)

	return pool, nil
}

func (p *WasmModulePool[T]) newInstance(ctx context.Context) (T, error) {
	module, err := p.ctor(ctx)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("failed to instantiate module: %w", err)
	}
	return module, nil
}

func (p *WasmModulePool[T]) Get(ctx context.Context) (T, error) {
	pooled := p.instancePool.Get()
	if pooled == nil {
		return p.newInstance(ctx)
	}
	return pooled.(*pooledInstance[T]).take(), nil
}

func (p *WasmModulePool[T]) Put(instance T) {
	entry := &pooledInstance[T]{module: instance}
	entry.cleanup = runtime.AddCleanup(entry, func(inst T) {
		// Close may be slow; don't block other cleanups (see the Go GC guide).
		go func() { _ = inst.Close(context.Background()) }()
	}, instance)

	p.instancePool.Put(entry)
}

func (p *WasmModulePool[T]) Close(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			pooled := p.instancePool.Get()
			if pooled == nil {
				return nil
			}
			_ = pooled.(*pooledInstance[T]).take().Close(ctx)
		}
	}
}
