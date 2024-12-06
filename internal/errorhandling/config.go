package errorhandling

import (
	"github.com/warpstreamlabs/bento/internal/docs"
)

const (
	fieldStrategy = "strategy"
)

// Config holds configuration options for the global error handling.
type Config struct {
	Strategy string `yaml:"strategy"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		Strategy: "none",
	}
}

func FromParsed(pConf *docs.ParsedConfig) (conf Config, err error) {
	if conf.Strategy, err = pConf.FieldString(fieldStrategy); err != nil {
		return
	}

	return
}
