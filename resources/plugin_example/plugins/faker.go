package plugins

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pioz/faker"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fakerInputFieldUsername  = "username"
	fakerInputFieldAge       = "age"
	fakerInputFieldBatchSize = "batch_size"
)

func fakerInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().Fields(
		service.NewBoolField(fakerInputFieldUsername),
		service.NewBoolField(fakerInputFieldAge),
		service.NewIntField(fakerInputFieldBatchSize).Default(1),
	)
}

func newFakerInput(conf *service.ParsedConfig, opts []int) (service.BatchInput, error) {
	name, err := conf.FieldBool(fakerInputFieldUsername)
	if err != nil {
		return nil, err
	}

	age, err := conf.FieldBool(fakerInputFieldAge)
	if err != nil {
		return nil, err
	}

	batchSize, err := conf.FieldInt(fakerInputFieldBatchSize)
	if err != nil {
		return nil, err
	}

	if opts != nil {
		seed := opts[0]
		// AutoRetryNacksBatched will reattempt failed messages because this input won't be able to handle nacks.
		return service.AutoRetryNacksBatched(&fakerInput{name: name, age: age, batchSize: batchSize, seed: seed}), err
	}
	return service.AutoRetryNacksBatched(&fakerInput{name: name, age: age, batchSize: batchSize}), err
}

func init() {
	err := service.RegisterBatchInput("faker", fakerInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
			return newFakerInput(pConf, nil)
		})
	if err != nil {
		panic(err)
	}
}

// ------------------------------------------------------------------------------------

type fakerInput struct {
	name      bool
	age       bool
	batchSize int

	seed int
}

func (f *fakerInput) Connect(ctx context.Context) error {
	return nil
}

func (f *fakerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	batch := make(service.MessageBatch, 0, f.batchSize)

	if f.seed != 0 {
		faker.SetSeed(int64(f.seed))
	}

	for range f.batchSize {

		fd := make(map[string]any)

		if f.name {
			fd[fakerInputFieldUsername] = faker.Username()
		}

		if f.age {
			fd[fakerInputFieldAge] = faker.IntInRange(0, 95)
		}

		jsonData, err := json.Marshal(fd)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshall JSON: %w", err)
		}

		msg := service.NewMessage(jsonData)
		batch = append(batch, msg)
	}

	return batch, func(context.Context, error) error { return nil }, nil
}

func (f *fakerInput) Close(ctx context.Context) error {
	return nil
}
