//go:build wasm

// Command batch is a batch processor plugin, where the operation is chosen by
// config.
package main

import (
	"bytes"
	"context"
	"errors"
	"strconv"

	"github.com/warpstreamlabs/bento/internal/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/batch_processor"
)

type config struct {
	Op string `json:"op"`
}

func init() {
	if err := plugin.RegisterBatchProcessor(func(conf *config, _ *service.Resources) (service.BatchProcessor, error) {
		return &proc{op: conf.Op}, nil
	}); err != nil {
		panic(err)
	}
}

type proc struct {
	op string
}

// calls counts the calls made to this instance.
var calls int

func (p *proc) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	switch p.op {
	case "meta":
		for _, msg := range batch {
			value, _ := msg.MetaGet("keep")
			_, missingFound := msg.MetaGet("missing")
			_, emptyFound := msg.MetaGet("empty")
			msg.MetaSet("kept", value)
			msg.MetaSet("missing_found", strconv.FormatBool(missingFound))
			msg.MetaSet("empty_found", strconv.FormatBool(emptyFound))
		}

	case "structured":
		for _, msg := range batch {
			v, err := msg.AsStructured()
			if err != nil {
				return nil, err
			}
			obj := v.(map[string]any)
			obj["seen"] = true
			msg.SetStructured(obj)
		}

	case "split":
		out := make([]service.MessageBatch, 0, len(batch))
		for _, msg := range batch {
			b, _ := msg.AsBytes()
			source, _ := msg.MetaGet("source")

			var split service.MessageBatch
			for _, v := range bytes.Split(b, []byte(",")) {
				m := service.NewMessage(v)
				m.MetaSet("source", source)
				split = append(split, m)
			}
			out = append(out, split)
		}
		return out, nil

	case "error":
		// Marks the first message and clears the error of the rest.
		for i, msg := range batch {
			if i == 0 {
				msg.SetError(errors.New("marked"))
			} else {
				msg.SetError(nil)
			}
		}

	case "drop":
		return nil, nil

	case "fail":
		for _, msg := range batch {
			msg.SetBytes([]byte("changed"))
			msg.MetaSet("changed", "true")
		}
		return nil, errors.New("failed")

	case "retained":
		// Reports the buffers retained by the plugin, which must only be the
		// input of this call.
		calls++
		for _, msg := range batch {
			msg.MetaSet("retained", strconv.Itoa(wasm.Retained()))
			msg.MetaSet("calls", strconv.Itoa(calls))
		}

	case "trap":
		if b, _ := batch[0].AsBytes(); string(b) == "trap" {
			panic("trapped")
		}
	}
	return []service.MessageBatch{batch}, nil
}

func (p *proc) Close(context.Context) error {
	return nil
}

func main() {}
