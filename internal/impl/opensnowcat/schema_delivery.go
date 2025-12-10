package opensnowcat

import (
	"bytes"
	"net/http"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type schemaDeliveryConfig struct {
	enabled       bool
	flushInterval time.Duration
	endpoint      string
	template      string
}

type schemaDelivery struct {
	config       schemaDeliveryConfig
	ticker       *time.Ticker
	stopChan     chan struct{}
	getSchemas   func() []string
	clearSchemas func()
	log          *service.Logger
}

func newSchemaDelivery(enabled bool, flushInterval time.Duration, endpoint string, template string, getSchemas func() []string, clearSchemas func(), log *service.Logger) *schemaDelivery {
	sd := &schemaDelivery{
		config: schemaDeliveryConfig{
			enabled:       enabled,
			flushInterval: flushInterval,
			endpoint:      endpoint,
			template:      template,
		},
		stopChan:     make(chan struct{}),
		getSchemas:   getSchemas,
		clearSchemas: clearSchemas,
		log:          log,
	}

	if enabled {
		sd.ticker = time.NewTicker(flushInterval)
		go sd.runTimer()
	}

	return sd
}

func (sd *schemaDelivery) runTimer() {
	for {
		select {
		case <-sd.ticker.C:
			sd.flush()
		case <-sd.stopChan:
			return
		}
	}
}

func (sd *schemaDelivery) flush() {
	schemas := sd.getSchemas()
	if len(schemas) == 0 {
		return
	}

	if err := sd.deliver(schemas); err != nil {
		sd.log.Warnf("Failed to deliver schema discovery: %v", err)
	} else {
		sd.clearSchemas()
	}
}

func (sd *schemaDelivery) deliver(schemas []string) error {
	if !sd.config.enabled {
		return nil
	}

	schemasJSON, err := marshalSchemas(schemas)
	if err != nil {
		return err
	}

	body := bytes.NewBufferString(sd.config.template)
	bodyStr := body.String()
	bodyStr = replaceSchemasVar(bodyStr, schemasJSON)
	resp, err := http.Post(sd.config.endpoint, "application/json", bytes.NewBufferString(bodyStr))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return err
	}
	return nil
}

func (sd *schemaDelivery) stop() {
	if sd.ticker != nil {
		sd.ticker.Stop()
		close(sd.stopChan)
	}
}

func marshalSchemas(schemas []string) (string, error) {
	var buf bytes.Buffer
	buf.WriteString("[")
	for i, schema := range schemas {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(`"`)
		buf.WriteString(schema)
		buf.WriteString(`"`)
	}
	buf.WriteString("]")
	return buf.String(), nil
}

func replaceSchemasVar(template, schemas string) string {
	return string(bytes.ReplaceAll([]byte(template), []byte("{{SCHEMAS}}"), []byte(schemas)))
}
