package errorhandling

import (
	"github.com/warpstreamlabs/bento/internal/docs"
)

const (
	fieldStrategy = "strategy"

	fieldLog              = "log"
	fieldLogEnabled       = "enabled"
	fieldLogAddPayload    = "add_payload"
	fieldLogSamplingRatio = "sampling_ratio"
)

// Config holds configuration options for the global error handling.
type Config struct {
	Strategy string    `yaml:"strategy"`
	Log      LogConfig `yaml:"log"`
}

// LogConfig holds configuration options for global error logging.
type LogConfig struct {
	Enabled       bool    `yaml:"enabled"`
	AddPayload    bool    `yaml:"add_payload"`
	SamplingRatio float64 `yaml:"sampling_ratio"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		Strategy: "none",
		Log: LogConfig{
			Enabled:       false,
			AddPayload:    false,
			SamplingRatio: 1,
		},
	}
}

func FromParsed(pConf *docs.ParsedConfig) (conf Config, err error) {
	if conf.Strategy, err = pConf.FieldString(fieldStrategy); err != nil {
		return
	}

	if pConf.Contains(fieldLog) {
		logConf := pConf.Namespace(fieldLog)
		if conf.Log.Enabled, err = logConf.FieldBool(fieldLogEnabled); err != nil {
			return
		}
		if conf.Log.AddPayload, err = logConf.FieldBool(fieldLogAddPayload); err != nil {
			return
		}
		if conf.Log.SamplingRatio, err = logConf.FieldFloat(fieldLogSamplingRatio); err != nil {
			return
		}
	}

	return
}
