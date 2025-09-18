package genkit

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"
	"github.com/warpstreamlabs/bento/public/service"
)

var errModelNotDefined = errors.New("model not found")

func safeExec(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	return fn()
}

type genkitInstanceKey struct{}

type genkitWrapper struct {
	once     sync.Once
	instance *genkit.Genkit
	err      error
}

func (g *genkitWrapper) get() (*genkit.Genkit, error) {
	g.once.Do(func() {
		g.instance = genkit.Init(context.Background())
	})
	return g.instance, g.err
}

func LoadInstance(res *service.Resources) (*genkit.Genkit, error) {
	actual, _ := res.GetOrSetGeneric(genkitInstanceKey{}, &genkitWrapper{})
	wrapper := actual.(*genkitWrapper)
	instance, err := wrapper.get()
	if err != nil {
		return nil, err
	}
	return instance, nil
}

type Plugin interface {
	Init(ctx context.Context) []api.Action
	Name() string
}

func InitPlugin[P Plugin](res *service.Resources, plugin P) (P, error) {
	g, err := LoadInstance(res)
	if err != nil {
		var zero P
		return zero, err
	}

	if p := genkit.LookupPlugin(g, plugin.Name()); p != nil {
		return p.(P), nil
	}

	if err := safeExec(func() error {
		_ = plugin.Init(context.Background())
		return nil
	}); err != nil {
		var zero P
		return zero, err
	}

	return plugin, nil
}
