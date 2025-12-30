package plugin

import (
	"context"
	"errors"
	"runtime"
	"sync"
)

type closer interface {
	Close(context.Context) error
}

type ConstructFunc[T any] func(context.Context) (T, error)

type InstancePool[T closer] struct {
	instancePool sync.Pool
	ctor         ConstructFunc[T]
}

func NewInstancePool[T closer](
	ctor ConstructFunc[T],
) *InstancePool[T] {
	return &InstancePool[T]{
		instancePool: sync.Pool{},
		ctor:         ctor,
	}
}

func (p *InstancePool[T]) newInstance(ctx context.Context) (T, error) {
	var zero T
	if p.ctor == nil {
		return zero, errors.New("cannot create instance: no constructor provided")
	}

	instance, err := p.ctor(ctx)
	if err != nil {
		return zero, err
	}

	_ = runtime.AddCleanup(&instance, func(val T) { _ = val.Close(context.TODO()) }, instance)

	return instance, nil
}

func (p *InstancePool[T]) Get(ctx context.Context) (T, error) {
	pooled := p.instancePool.Get()
	if pooled == nil {
		return p.newInstance(ctx)
	}
	return pooled.(T), nil
}

func (p *InstancePool[T]) Put(instance T) {
	p.instancePool.Put(instance)
}

func (p *InstancePool[T]) Close(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			pooled := p.instancePool.Get()
			if pooled == nil {
				return nil
			}
			if closer, ok := pooled.(T); ok {
				_ = closer.Close(ctx)
			}
		}
	}
}
