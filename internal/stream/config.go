package stream

import (
	"github.com/warpstreamlabs/bento/v1/internal/component/buffer"
	"github.com/warpstreamlabs/bento/v1/internal/component/input"
	"github.com/warpstreamlabs/bento/v1/internal/component/output"
	"github.com/warpstreamlabs/bento/v1/internal/docs"
	"github.com/warpstreamlabs/bento/v1/internal/pipeline"
)

const (
	fieldInput    = "input"
	fieldBuffer   = "buffer"
	fieldPipeline = "pipeline"
	fieldOutput   = "output"
)

// Config is a configuration struct representing all four layers of a Bento
// stream.
type Config struct {
	Input    input.Config    `yaml:"input"`
	Buffer   buffer.Config   `yaml:"buffer"`
	Pipeline pipeline.Config `yaml:"pipeline"`
	Output   output.Config   `yaml:"output"`

	rawSource any
}

func (c *Config) GetRawSource() any {
	return c.rawSource
}

func FromParsed(prov docs.Provider, pConf *docs.ParsedConfig, rawSource any) (conf Config, err error) {
	conf.rawSource = rawSource
	var v any
	if v, err = pConf.FieldAny(fieldInput); err != nil {
		return
	}
	if conf.Input, err = input.FromAny(prov, v); err != nil {
		return
	}

	if v, err = pConf.FieldAny(fieldBuffer); err != nil {
		return
	}
	if conf.Buffer, err = buffer.FromAny(prov, v); err != nil {
		return
	}

	if v, err = pConf.FieldAny(fieldPipeline); err != nil {
		return
	}
	if conf.Pipeline, err = pipeline.FromAny(prov, v); err != nil {
		return
	}

	if v, err = pConf.FieldAny(fieldOutput); err != nil {
		return
	}
	if conf.Output, err = output.FromAny(prov, v); err != nil {
		return
	}
	return
}
