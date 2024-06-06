package lambda

import (
	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/config"
	"github.com/warpstreamlabs/bento/v1/internal/docs"
)

func DefaultConfigAndSpec() (conf config.Type, spec docs.FieldSpecs, err error) {
	spec = config.Spec()

	spec.SetDefault(map[string]any{
		"none": map[string]any{},
	}, "metrics")

	spec.SetDefault("json", "logger", "format")

	spec.SetDefault(map[string]any{
		"switch": map[string]any{
			"retry_until_success": false,
			"cases": []any{
				map[string]any{
					"check": "errored()",
					"output": map[string]any{
						"reject": "processing failed due to: ${! error() }",
					},
				},
				map[string]any{
					"output": map[string]any{
						"sync_response": map[string]any{},
					},
				},
			},
		},
	}, "output")

	var pConf *docs.ParsedConfig
	if pConf, err = spec.ParsedConfigFromAny(map[string]any{}); err != nil {
		return
	}

	if conf, err = config.FromParsed(bundle.GlobalEnvironment, pConf, nil); err != nil {
		return
	}
	return
}
