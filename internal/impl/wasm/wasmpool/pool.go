package wasmpool

import (
	"context"
	"fmt"
	"sync"

	"runtime"

	"github.com/tetratelabs/wazero/api"
)

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
	return pooled.(T), nil
}

func (p *WasmModulePool[T]) Put(instance T) {
	_ = runtime.AddCleanup(&instance, func(inst T) {
		inst.Close(context.Background())
	}, instance)

	p.instancePool.Put(instance)
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
			if closer, ok := pooled.(interface{ Close(context.Context) error }); ok {
				_ = closer.Close(ctx)
			}
		}
	}
}
