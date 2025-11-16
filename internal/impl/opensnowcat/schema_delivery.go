package opensnowcat

import (
	"bytes"
	"net/http"
	"time"
)

type schemaDeliveryConfig struct {
	enabled       bool
	flushInterval time.Duration
	endpoint      string
	template      string
}

type schemaDelivery struct {
	config   schemaDeliveryConfig
	lastSent time.Time
}

func newSchemaDelivery(enabled bool, flushInterval time.Duration, endpoint string, template string) *schemaDelivery {
	return &schemaDelivery{
		config: schemaDeliveryConfig{
			enabled:       enabled,
			flushInterval: flushInterval,
			endpoint:      endpoint,
			template:      template,
		},
		lastSent: time.Now(),
	}
}

func (sd *schemaDelivery) shouldFlush() bool {
	return time.Since(sd.lastSent) >= sd.config.flushInterval
}

func (sd *schemaDelivery) deliver(schemas string) error {
	if !sd.config.enabled {
		return nil
	}
	body := bytes.NewBufferString(sd.config.template)
	bodyStr := body.String()
	bodyStr = replaceSchemasVar(bodyStr, schemas)
	resp, err := http.Post(sd.config.endpoint, "application/json", bytes.NewBufferString(bodyStr))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return err
	}
	sd.lastSent = time.Now()
	return nil
}

func replaceSchemasVar(template, schemas string) string {
	return string(bytes.ReplaceAll([]byte(template), []byte("{{SCHEMAS}}"), []byte(schemas)))
}
