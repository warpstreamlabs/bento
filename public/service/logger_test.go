package service

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/v1/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/v1/internal/log"
)

func TestReverseAirGapLogger(t *testing.T) {
	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"

	var buf bytes.Buffer
	logger, err := log.New(&buf, ifs.OS(), lConf)
	require.NoError(t, err)

	agLogger := newReverseAirGapLogger(logger)
	agLogger2 := agLogger.With("field1", "value1", "field2", "value2")

	agLogger.Debugf("foo: %v", "bar1")
	agLogger.Infof("foo: %v", "bar2")

	agLogger2.Debugf("foo2: %v", "bar1")
	agLogger2.Infof("foo2: %v", "bar2")

	agLogger.Warnf("foo: %v", "bar3")
	agLogger.Errorf("foo: %v", "bar4")

	agLogger2.Warnf("foo2: %v", "bar3")
	agLogger2.Errorf("foo2: %v", "bar4")

	assert.Equal(t, `{"@service":"bento","level":"info","msg":"foo: bar2"}
{"@service":"bento","field1":"value1","field2":"value2","level":"info","msg":"foo2: bar2"}
{"@service":"bento","level":"warning","msg":"foo: bar3"}
{"@service":"bento","level":"error","msg":"foo: bar4"}
{"@service":"bento","field1":"value1","field2":"value2","level":"warning","msg":"foo2: bar3"}
{"@service":"bento","field1":"value1","field2":"value2","level":"error","msg":"foo2: bar4"}
`, buf.String())
}

func TestReverseAirGapLoggerDodgyFields(t *testing.T) {
	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"

	var buf bytes.Buffer
	logger, err := log.New(&buf, ifs.OS(), lConf)
	require.NoError(t, err)

	agLogger := newReverseAirGapLogger(logger)

	agLogger.With("field1", "value1", "field2").Infof("foo1")
	agLogger.With(10, 20).Infof("foo2")
	agLogger.With("field3", 30).Infof("foo3")
	agLogger.With("field4", "value4").With("field5", "value5").Infof("foo4")

	assert.Equal(t, `{"@service":"bento","field1":"value1","level":"info","msg":"foo1"}
{"10":"20","@service":"bento","level":"info","msg":"foo2"}
{"@service":"bento","field3":"30","level":"info","msg":"foo3"}
{"@service":"bento","field4":"value4","field5":"value5","level":"info","msg":"foo4"}
`, buf.String())
}
