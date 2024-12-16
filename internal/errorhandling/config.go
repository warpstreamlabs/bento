package errorhandling

import (
	"github.com/warpstreamlabs/bento/internal/docs"
)

const (
	fieldStrategy        = "strategy"
	fieldErrorSampleRate = "error_sample_rate"
	fieldMaxRetries      = "max_retries"
)

// Config holds configuration options for the global error handling.
type Config struct {
	Strategy        string  `yaml:"strategy"`
	ErrorSampleRate float64 `yaml:"error_sample_rate"`
	MaxRetries      int     `yaml:"max_retries"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		Strategy:        "none",
		ErrorSampleRate: 0,
		MaxRetries:      0,
	}
}

func FromParsed(pConf *docs.ParsedConfig) (conf Config, err error) {
	if conf.Strategy, err = pConf.FieldString(fieldStrategy); err != nil {
		return
	}

	if conf.ErrorSampleRate, err = pConf.FieldFloat(fieldErrorSampleRate); err != nil {
		return
	}

	if conf.MaxRetries, err = pConf.FieldInt(fieldMaxRetries); err != nil {
		return
	}
	return
}
