package service_test

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/warpstreamlabs/bento/public/service"

	// Import only pure Bento components, switch with `components/all` for all
	// standard components.
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

type RandomRateLimit struct {
	max time.Duration
}

func (r *RandomRateLimit) Access(context.Context) (time.Duration, error) {
	return time.Duration(rand.Int() % int(r.max)), nil
}

func (r *RandomRateLimit) Close(ctx context.Context) error {
	return nil
}

// This example demonstrates how to create a rate limit plugin, which is
// configured by providing a struct containing the fields to be parsed from
// within the Bento configuration.
func Example_rateLimitPlugin() {
	configSpec := service.NewConfigSpec().
		Summary("A rate limit that's pretty much just random.").
		Description("I guess this isn't really that useful, sorry.").
		Field(service.NewStringField("maximum_duration").Default("1s"))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
		maxDurStr, err := conf.FieldString("maximum_duration")
		if err != nil {
			return nil, err
		}
		maxDuration, err := time.ParseDuration(maxDurStr)
		if err != nil {
			return nil, fmt.Errorf("invalid max duration: %w", err)
		}
		return &RandomRateLimit{maxDuration}, nil
	}

	err := service.RegisterRateLimit("random", configSpec, constructor)
	if err != nil {
		panic(err)
	}

	// And then execute Bento with:
	// service.RunCLI(context.Background())
}
