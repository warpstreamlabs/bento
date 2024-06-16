package metrics

import (
	"fmt"
	"sort"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/mapping"
	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/value"
)

// Mapping is a compiled Bloblang mapping used to rewrite metrics.
type Mapping struct {
	m          *mapping.Executor
	logger     log.Modular
	staticVars map[string]any
}

// NewMapping parses a Bloblang mapping and returns a metrics mapping.
func NewMapping(mapping string, logger log.Modular) (*Mapping, error) {
	if mapping == "" {
		return &Mapping{m: nil, logger: logger}, nil
	}
	m, err := bloblang.GlobalEnvironment().NewMapping(mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("%v", perr.ErrorAtPosition([]rune(mapping)))
		}
		return nil, err
	}
	return &Mapping{m: m, logger: logger, staticVars: map[string]any{}}, nil
}

// WithStaticVars adds a map of key/value pairs to the static variables of the
// metrics mapping. These are variables that will be made available to each
// invocation of the metrics mapping.
func (m *Mapping) WithStaticVars(kvs map[string]any) *Mapping {
	newM := *m

	newM.staticVars = map[string]any{}
	for k, v := range m.staticVars {
		newM.staticVars[k] = v
	}
	for k, v := range kvs {
		newM.staticVars[k] = v
	}

	return &newM
}

func (m *Mapping) mapPath(path string, labelNames, labelValues []string) (outPath string, outLabelNames, outLabelValues []string) {
	if m == nil || m.m == nil {
		return path, labelNames, labelValues
	}

	part := message.NewPart(nil)
	part.SetStructuredMut(path)
	for i, v := range labelNames {
		part.MetaSetMut(v, labelValues[i])
	}
	msg := message.Batch{part}

	outPart := part.DeepCopy()

	var input any = path
	vars := map[string]any{}
	for k, v := range m.staticVars {
		vars[k] = v
	}

	var v any = value.Nothing(nil)
	if err := m.m.ExecOnto(query.FunctionContext{
		Maps:     m.m.Maps(),
		Vars:     vars,
		MsgBatch: msg,
		NewMeta:  outPart,
		NewValue: &v,
	}.WithValue(input), mapping.AssignmentContext{
		Vars:  vars,
		Meta:  outPart,
		Value: &v,
	}); err != nil {
		m.logger.Error("Failed to apply path mapping on '%v': %v\n", path, err)
		return path, nil, nil
	}

	_ = outPart.MetaIterStr(func(k, v string) error {
		outLabelNames = append(outLabelNames, k)
		return nil
	})
	if len(outLabelNames) > 0 {
		sort.Strings(outLabelNames)
		for _, k := range outLabelNames {
			v := outPart.MetaGetStr(k)
			m.logger.Trace("Metrics label '%v' created with static value '%v'.\n", k, v)
			outLabelValues = append(outLabelValues, v)
		}
	}

	switch t := v.(type) {
	case value.Delete:
		m.logger.Trace("Deleting metrics path: %v\n", path)
		return "", nil, nil
	case value.Nothing:
		m.logger.Trace("Metrics path '%v' registered unchanged.\n", path)
		outPath = path
		return
	case string:
		m.logger.Trace("Updated metrics path '%v' to: %v\n", path, t)
		outPath = t
		return
	}
	m.logger.Error("Path mapping returned invalid result, expected string, found %T\n", v)
	return path, labelNames, labelValues
}
