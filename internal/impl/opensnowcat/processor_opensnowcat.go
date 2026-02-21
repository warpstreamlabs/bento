package opensnowcat

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"
	"time"

	analytics "github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	oscFieldFilters         = "filters"
	oscFieldFiltersDrop     = "drop"
	oscFieldOutputFormat    = "output_format"
	oscFieldSchemaDiscovery = "schema_discovery"
)

// OpenSnowcat/Snowplow Enriched TSV column names (lowercase, 131 columns)
// Source: https://docs.snowplow.io/docs/fundamentals/canonical-event/understanding-the-enriched-tsv-format/
var opensnowcatColumns = []string{
	"app_id", "platform", "etl_tstamp", "collector_tstamp", "dvce_created_tstamp",
	"event", "event_id", "txn_id", "name_tracker", "v_tracker",
	"v_collector", "v_etl", "user_id", "user_ipaddress", "user_fingerprint",
	"domain_userid", "domain_sessionidx", "network_userid", "geo_country", "geo_region",
	"geo_city", "geo_zipcode", "geo_latitude", "geo_longitude", "geo_region_name",
	"ip_isp", "ip_organization", "ip_domain", "ip_netspeed", "page_url",
	"page_title", "page_referrer", "page_urlscheme", "page_urlhost", "page_urlport",
	"page_urlpath", "page_urlquery", "page_urlfragment", "refr_urlscheme", "refr_urlhost",
	"refr_urlport", "refr_urlpath", "refr_urlquery", "refr_urlfragment", "refr_medium",
	"refr_source", "refr_term", "mkt_medium", "mkt_source", "mkt_term",
	"mkt_content", "mkt_campaign", "contexts", "se_category", "se_action",
	"se_label", "se_property", "se_value", "unstruct_event", "tr_orderid",
	"tr_affiliation", "tr_total", "tr_tax", "tr_shipping", "tr_city",
	"tr_state", "tr_country", "ti_orderid", "ti_sku", "ti_name",
	"ti_category", "ti_price", "ti_quantity", "pp_xoffset_min", "pp_xoffset_max",
	"pp_yoffset_min", "pp_yoffset_max", "useragent", "br_name", "br_family",
	"br_version", "br_type", "br_renderengine", "br_lang", "br_features_pdf",
	"br_features_flash", "br_features_java", "br_features_director", "br_features_quicktime",
	"br_features_realplayer", "br_features_windowsmedia", "br_features_gears", "br_features_silverlight",
	"br_cookies", "br_colordepth", "br_viewwidth", "br_viewheight", "os_name",
	"os_family", "os_manufacturer", "os_timezone", "dvce_type", "dvce_ismobile",
	"dvce_screenwidth", "dvce_screenheight", "doc_charset", "doc_width", "doc_height",
	"tr_currency", "tr_total_base", "tr_tax_base", "tr_shipping_base", "ti_currency",
	"ti_price_base", "base_currency", "geo_timezone", "mkt_clickid", "mkt_network",
	"etl_tags", "dvce_sent_tstamp", "refr_domain_userid", "refr_dvce_tstamp", "derived_contexts",
	"domain_sessionid", "derived_tstamp", "event_vendor", "event_name", "event_format",
	"event_version", "event_fingerprint", "true_tstamp",
}

func opensnowcatProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Parsing").
		Summary("Processes [OpenSnowcat](https://opensnowcat.io/)/[Snowplow enriched TSV](https://docs.snowplow.io/docs/fundamentals/canonical-event/understanding-the-enriched-tsv-format/) events. Convert enriched TSV to flattened JSON, filter events, and transform sensitive fields for privacy compliance.").
		Description(`
This processor provides comprehensive event processing capabilities:

## Features

### Format Conversion
- Convert enriched TSV to flattened JSON with automatic context extraction
- Maintain TSV format for OpenSnowcat/Snowplow downstream compatibility

### Event Filtering
- Drop events based on field values (IP addresses, user agents, etc.)
- Filter by schema property paths in contexts, derived_contexts, and unstruct_event
- OR logic: event is dropped if ANY filter matches

### Field Transformations
Transform sensitive fields for PII compliance and privacy:

- **hash**: Hash field values using configurable algorithms (MD5, SHA-1, SHA-256, SHA-384, SHA-512) with salt
- **redact**: Replace field values with a fixed string (e.g., "[REDACTED]")
- **anonymize_ip**: Mask IP addresses while preserving network information (supports both IPv4 and IPv6)
  - IPv4: Mask last N octets using `+"`anon_octets`"+` parameter
  - IPv6: Mask last N segments using `+"`anon_segments`"+` parameter

All transformations support both direct TSV columns and schema property paths.`).
		Version("1.12.0").
		Field(service.NewStringAnnotatedEnumField(oscFieldOutputFormat, map[string]string{
			"json":          "Convert enriched TSV to flattened JSON with contexts, derived_contexts, and unstruct_event automatically flattened into top-level objects.",
			"tsv":           "Maintain enriched TSV format without conversion.",
			"enriched_json": "Convert to database-optimized nested JSON with key-based schema structure. Each schema becomes a key (vendor_name) containing version and data array. Compatible with BigQuery, Snowflake, Databricks, Redshift, PostgreSQL, ClickHouse, and Iceberg tables. Enables simple queries without UNNEST and schema evolution without table mutations.",
		}).Description("Output format for processed events.").Default("tsv")).
		Field(service.NewStringMapField("set_metadata").
			Description("Map metadata keys to OpenSnowcat canonical event model field names. Supports direct TSV column names (e.g., 'event_fingerprint', 'app_id') and schema property paths (e.g., 'com.vendor.schema.field'). Metadata is set before any filters or transformations are applied.").
			Optional().
			Example(map[string]interface{}{
				"fingerprint":      "event_fingerprint",
				"eid":              "event_id",
				"app_id":           "app_id",
				"collector_tstamp": "collector_tstamp",
			})).
		Field(service.NewObjectField(oscFieldFilters,
			service.NewAnyMapField(oscFieldFiltersDrop).
				Description("Map of field names to filter criteria. Events matching ANY criteria will be dropped (OR logic). Supports both regular TSV columns (e.g., `user_ipaddress`, `useragent`) and schema property paths (e.g., `com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily`). Each filter uses 'contains' for substring matching.").
				Optional().
				Advanced(),
			service.NewObjectField("transform",
				service.NewStringField("salt").
					Description("Global default salt for hashing operations. Can be overridden per field.").
					Optional().
					Advanced(),
				service.NewStringEnumField("hash_algo", "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512").
					Description("Global default hash algorithm. Can be overridden per field.").
					Default("SHA-256").
					Advanced(),
				service.NewAnyMapField("fields").
					Description(`Map of field names to transformation configurations. Each field must specify:
- **strategy** (required): Transformation type - "hash", "redact", or "anonymize_ip"
- **hash_algo** (optional): Algorithm for hash strategy - "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512" (overrides global default)
- **salt** (optional): Salt for hash strategy (overrides global default)
- **redact_value** (optional): Replacement value for redact strategy (default: "[REDACTED]")
- **anon_octets** (optional): Number of IPv4 octets to mask for anonymize_ip strategy (default: 0)
- **anon_segments** (optional): Number of IPv6 segments to mask for anonymize_ip strategy (default: 0)

Supports both TSV columns (e.g., user_id, user_ipaddress) and schema property paths (e.g., com.vendor.schema.field).`).
					Optional(),
			).
				Description("Field transformation configuration for anonymization, hashing, and redaction").
				Optional().
				Advanced(),
		).Description("Filter and transformation configurations").Optional().Advanced()).
		Field(service.NewObjectField(oscFieldSchemaDiscovery,
			service.NewBoolField("enabled").
				Description("Enable schema discovery feature").
				Default(false),
			service.NewStringField("flush_interval").
				Description("Interval between schema discovery flushes").
				Default("5m"),
			service.NewStringField("endpoint").
				Description("HTTP endpoint to send schema discovery data").
				Default("https://api.snowcatcloud.com/internal/schema-discovery"),
			service.NewStringField("template").
				Description("Template for schema discovery payload. Use `{{SCHEMAS}}` variable for schema list").
				Default(`{"schemas": {{SCHEMAS}}}`),
		).Description("Schema discovery configuration").Optional().Advanced()).
		Example(
			"TSV > JSON",
			"Converts OpenSnowcat/Snowplow enriched TSV events to flattened JSON format, extracting all contexts, derived contexts, and unstruct events into top-level fields.",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: json
`,
		).
		Example(
			"TSV > Enriched JSON",
			"Converts OpenSnowcat/Snowplow enriched TSV to database-optimized nested JSON with key-based schema structure. Each schema becomes a key (vendor_schema_name) with version and data fields. Enables simple direct-access queries across all databases without UNNEST operations. Perfect for BigQuery, Snowflake, Databricks, Redshift, and other data warehouses.",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: enriched_json
# Out: { 'contexts': { 'com_snowplowanalytics_snowplow_web_page': {version: '1-0-0', data: [{id: '...'}] } } }
`,
		).
		Example(
			"Filter IP",
			"Filters out events from IP addresses while maintaining TSV format.",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: tsv
        filters:
          drop:
            user_ipaddress:
              contains: ["127.0.0.1", "192.168.", "10.0."]
`,
		).
		Example(
			"Schema Filter",
			"Filters events based on schema property values (without version). The processor automatically searches contexts, derived_contexts, and unstruct_event fields for matching vendor, schemas and property name (case sensitive).",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: tsv
        filters:
          drop:
            com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily:
              contains: ["Chrome", "Firefox"]
            user_ipaddress:
              contains: ["10.0."]
`,
		).
		Example(
			"Transform",
			"Transforms sensitive fields using various strategies: hash user identifiers, anonymize IP addresses, and redact network identifiers. Perfect for GDPR and privacy compliance.",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: json
        filters:
          transform:
            salt: "your-secret-salt-here"
            hash_algo: SHA-256
            fields:
              user_id:
                strategy: hash
              user_ipaddress:
                strategy: anonymize_ip
                anon_octets: 2
                anon_segments: 3
              network_userid:
                strategy: redact
                redact_value: "[REDACTED]"
`,
		).
		Example(
			"Advanced Transforms",
			"Combines multiple transformation strategies with field-specific configurations. Uses different hash algorithms for different fields and supports both IPv4 and IPv6 anonymization.",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: tsv
        filters:
          transform:
            salt: "global-default-salt"
            hash_algo: SHA-256
            fields:
              user_id:
                strategy: hash
                hash_algo: SHA-512
                salt: "user-specific-salt"
              user_ipaddress:
                strategy: anonymize_ip
                anon_octets: 2
                anon_segments: 4
              domain_userid:
                strategy: hash
              network_userid:
                strategy: redact
                redact_value: "REDACTED"
              user_fingerprint:
                strategy: hash
                hash_algo: MD5
`,
		).
		Example(
			"Deduplication with Cache",
			"Sets metadata from event fields to enable deduplication using Bento's cache processor. Parses TSV once, extracts fingerprint as metadata, then uses cache for deduplication. Duplicate events within 5 minutes are dropped.",
			`
pipeline:
  processors:
    - opensnowcat:
        set_metadata:
          fingerprint: event_fingerprint
        output_format: json
    
    - cache:
        resource: dedupe_cache
        operator: add
        key: ${! metadata("fingerprint") }
        value: "1"
    
    - mapping: |
        root = if !errored() { this } else { deleted() }

cache_resources:
  - label: dedupe_cache
    memory:
      default_ttl: 5m
`,
		)
}

func init() {
	err := service.RegisterProcessor(
		"opensnowcat",
		opensnowcatProcessorConfig(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			return newOpenSnowcatProcessorFromConfig(conf, res)
		})
	if err != nil {
		panic(err)
	}
}

type filterCriteria struct {
	contains []string
	schemas  []string
}

type fieldTransform struct {
	strategy     string
	hashAlgo     string
	salt         string
	redactValue  string
	anonOctets   int
	anonSegments int
}

type transformConfig struct {
	salt     string
	hashAlgo string
	fields   map[string]*fieldTransform
}

type opensnowcatProcessor struct {
	dropFilters      map[string]*filterCriteria
	transformConfig  *transformConfig
	outputFormat     string
	columnIndexMap   map[string]int
	metadataMapping  map[string]string
	log              *service.Logger
	mDropped         *service.MetricCounter
	schemaDiscovery  *schemaDelivery
	schemasCollected map[string]bool
	schemasMu        sync.Mutex
}

func newOpenSnowcatProcessorFromConfig(conf *service.ParsedConfig, res *service.Resources) (*opensnowcatProcessor, error) {
	outputFormat := "tsv"

	if conf.Contains(oscFieldOutputFormat) {
		format, err := conf.FieldString(oscFieldOutputFormat)
		if err != nil {
			return nil, err
		}
		outputFormat = format
	}

	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	dropFilters := make(map[string]*filterCriteria)
	var transformCfg *transformConfig

	if conf.Contains(oscFieldFilters) {
		filtersConf, err := conf.FieldAnyMap(oscFieldFilters)
		if err != nil {
			return nil, err
		}

		if dropParsed, exists := filtersConf[oscFieldFiltersDrop]; exists {
			dropAny, _ := dropParsed.FieldAny()
			if dropConfig, ok := dropAny.(map[string]interface{}); ok {
				for fieldName, criteria := range dropConfig {
					if criteriaMap, ok := criteria.(map[string]interface{}); ok {
						fc := &filterCriteria{}

						if containsList, ok := criteriaMap["contains"]; ok {
							if containsSlice, ok := containsList.([]interface{}); ok {
								for _, item := range containsSlice {
									if str, ok := item.(string); ok {
										fc.contains = append(fc.contains, str)
									}
								}
							}
						}

						if schemasList, ok := criteriaMap["schemas"]; ok {
							if schemasSlice, ok := schemasList.([]interface{}); ok {
								for _, item := range schemasSlice {
									if str, ok := item.(string); ok {
										fc.schemas = append(fc.schemas, str)
									}
								}
							}
						}

						if len(fc.contains) > 0 || len(fc.schemas) > 0 {

							normalizedFieldName := fieldName
							if !strings.Contains(fieldName, ".") || strings.HasPrefix(fieldName, "geo.") || strings.HasPrefix(fieldName, "metrics.") || strings.HasPrefix(fieldName, "site.") {
								normalizedFieldName = strings.ToLower(fieldName)
							}
							dropFilters[normalizedFieldName] = fc
						}
					}
				}
			}
		}

		if transformParsed, exists := filtersConf["transform"]; exists {
			transformAny, _ := transformParsed.FieldAny()
			if transformMap, ok := transformAny.(map[string]interface{}); ok {
				transformCfg = &transformConfig{
					fields: make(map[string]*fieldTransform),
				}

				if salt, ok := transformMap["salt"].(string); ok {
					transformCfg.salt = salt
				}

				if hashAlgo, ok := transformMap["hash_algo"].(string); ok {
					transformCfg.hashAlgo = hashAlgo
				} else {
					transformCfg.hashAlgo = "SHA-256"
				}

				if fieldsMap, ok := transformMap["fields"].(map[string]interface{}); ok {
					for fieldName, fieldConfig := range fieldsMap {
						if fieldCfgMap, ok := fieldConfig.(map[string]interface{}); ok {
							ft := &fieldTransform{}

							if strategy, ok := fieldCfgMap["strategy"].(string); ok {
								ft.strategy = strategy
							}

							if hashAlgo, ok := fieldCfgMap["hash_algo"].(string); ok {
								ft.hashAlgo = hashAlgo
							}

							if salt, ok := fieldCfgMap["salt"].(string); ok {
								ft.salt = salt
							}

							if redactVal, ok := fieldCfgMap["redact_value"].(string); ok {
								ft.redactValue = redactVal
							} else {
								ft.redactValue = "[REDACTED]"
							}

							if octets, ok := fieldCfgMap["anon_octets"].(float64); ok {
								ft.anonOctets = int(octets)
							} else {
								ft.anonOctets = 2
							}

							if segments, ok := fieldCfgMap["anon_segments"].(float64); ok {
								ft.anonSegments = int(segments)
							} else {
								ft.anonSegments = 4
							}

							normalizedFieldName := fieldName
							if !strings.Contains(fieldName, ".") || strings.HasPrefix(fieldName, "geo.") || strings.HasPrefix(fieldName, "metrics.") || strings.HasPrefix(fieldName, "site.") {
								normalizedFieldName = strings.ToLower(fieldName)
							}
							transformCfg.fields[normalizedFieldName] = ft
						}
					}
				}
			}
		}
	}

	schemasCollected := make(map[string]bool)

	metadataMapping := map[string]string{}
	if conf.Contains("set_metadata") {
		var err error
		metadataMapping, err = conf.FieldStringMap("set_metadata")
		if err != nil {
			return nil, err
		}
	}

	proc := &opensnowcatProcessor{
		dropFilters:      dropFilters,
		transformConfig:  transformCfg,
		outputFormat:     outputFormat,
		columnIndexMap:   columnIndexMap,
		metadataMapping:  metadataMapping,
		log:              res.Logger(),
		mDropped:         res.Metrics().NewCounter("dropped"),
		schemasCollected: schemasCollected,
	}

	if conf.Contains(oscFieldSchemaDiscovery) {
		sdConf, err := conf.FieldObjectMap(oscFieldSchemaDiscovery)
		if err != nil {
			return nil, err
		}

		enabled := false
		if enabledVal, exists := sdConf["enabled"]; exists {
			if enabledBool, err := enabledVal.FieldBool(); err == nil {
				enabled = enabledBool
			}
		}

		if enabled {
			flushIntervalStr := "5m"
			if intervalVal, exists := sdConf["flush_interval"]; exists {
				if intervalStr, err := intervalVal.FieldString(); err == nil {
					flushIntervalStr = intervalStr
				}
			}

			flushInterval, err := time.ParseDuration(flushIntervalStr)
			if err != nil {
				return nil, fmt.Errorf("invalid flush_interval: %w", err)
			}

			endpoint := "https://api.snowcatcloud.com/internal/schema-discovery"
			if endpointVal, exists := sdConf["endpoint"]; exists {
				if endpointStr, err := endpointVal.FieldString(); err == nil {
					endpoint = endpointStr
				}
			}

			template := `{"schemas": {{SCHEMAS}}}`
			if templateVal, exists := sdConf["template"]; exists {
				if templateStr, err := templateVal.FieldString(); err == nil {
					if templateStr != "" && templateStr != "{}" {
						template = templateStr
					}
				}
			}

			getSchemas := func() []string {
				proc.schemasMu.Lock()
				defer proc.schemasMu.Unlock()
				schemas := make([]string, 0, len(proc.schemasCollected))
				for schema := range proc.schemasCollected {
					schemas = append(schemas, schema)
				}
				return schemas
			}

			clearSchemas := func() {
				proc.schemasMu.Lock()
				defer proc.schemasMu.Unlock()
				proc.schemasCollected = make(map[string]bool)
			}

			proc.schemaDiscovery = newSchemaDelivery(enabled, flushInterval, endpoint, template, getSchemas, clearSchemas, res.Logger())
		}
	}

	return proc, nil
}

func (o *opensnowcatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	tsvBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	tsvString := string(tsvBytes)

	columns := strings.Split(tsvString, "\t")

	// Set metadata first (before any filters or transformations)
	if len(o.metadataMapping) > 0 {
		o.setMetadataFromFields(msg, columns)
	}

	if o.schemaDiscovery != nil && o.schemaDiscovery.config.enabled {
		schemas := o.extractSchemasFromEvent(columns)
		o.schemasMu.Lock()
		for _, schema := range schemas {
			o.schemasCollected[schema] = true
		}
		o.schemasMu.Unlock()
	}

	if len(o.dropFilters) > 0 {
		if o.shouldDropEventFromTSV(columns) {
			o.mDropped.Incr(1)
			return nil, nil
		}
	}

	if o.transformConfig != nil && len(o.transformConfig.fields) > 0 {
		o.applyTransformations(columns)
	}

	if o.outputFormat == "tsv" {
		transformedTSV := strings.Join(columns, "\t")
		msg.SetBytes([]byte(transformedTSV))
		return service.MessageBatch{msg}, nil
	}

	transformedTSV := strings.Join(columns, "\t")
	parsedEvent, err := analytics.ParseEvent(transformedTSV)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OpenSnowcat event: %w", err)
	}

	eventMap, err := parsedEvent.ToMap()
	if err != nil {
		return nil, fmt.Errorf("failed to convert event to map: %w", err)
	}

	if o.outputFormat == "enriched_json" {
		eventMap = o.restructureForEnrichedJSON(eventMap, columns)
	}

	msg.SetStructuredMut(eventMap)

	return service.MessageBatch{msg}, nil
}

func (o *opensnowcatProcessor) shouldDropEventFromTSV(columns []string) bool {
	for fieldName, criteria := range o.dropFilters {
		if strings.Contains(fieldName, ".") && !strings.HasPrefix(fieldName, "geo.") && !strings.HasPrefix(fieldName, "metrics.") && !strings.HasPrefix(fieldName, "site.") {
			if o.matchesSchemaProperty(columns, fieldName, criteria) {
				return true
			}
			continue
		}

		colIndex, exists := o.columnIndexMap[fieldName]
		if !exists {
			o.log.Warnf("Filter field %s not found in column map", fieldName)
			continue
		}

		if colIndex >= len(columns) {
			continue
		}

		fieldValue := columns[colIndex]

		for _, containsStr := range criteria.contains {
			if strings.Contains(strings.ToLower(fieldValue), strings.ToLower(containsStr)) {
				return true
			}
		}
	}
	return false
}

func (o *opensnowcatProcessor) setMetadataFromFields(msg *service.Message, columns []string) {
	for metaKey, fieldName := range o.metadataMapping {
		// Check if it's a schema property path (contains dots, but not geo./metrics./site.)
		if strings.Contains(fieldName, ".") &&
			!strings.HasPrefix(fieldName, "geo.") &&
			!strings.HasPrefix(fieldName, "metrics.") &&
			!strings.HasPrefix(fieldName, "site.") {
			// Extract from JSON context fields (preserve case for schema paths)
			value := o.extractSchemaPropertyForMetadata(columns, fieldName)
			if value != "" {
				msg.MetaSet(metaKey, value)
			}
			continue
		}

		// Direct TSV column (normalize to lowercase)
		fieldNameLower := strings.ToLower(fieldName)
		if colIndex, exists := o.columnIndexMap[fieldNameLower]; exists && colIndex < len(columns) {
			value := columns[colIndex]
			if value != "" {
				msg.MetaSet(metaKey, value)
			}
		}
	}
}

func (o *opensnowcatProcessor) extractSchemaPropertyForMetadata(columns []string, schemaPath string) string {
	// Check contexts field
	if contextsIdx, ok := o.columnIndexMap["contexts"]; ok && contextsIdx < len(columns) {
		if value := o.extractSchemaPropertyValue(columns[contextsIdx], schemaPath); value != "" {
			return value
		}
	}
	// Check derived_contexts field
	if derivedIdx, ok := o.columnIndexMap["derived_contexts"]; ok && derivedIdx < len(columns) {
		if value := o.extractSchemaPropertyValue(columns[derivedIdx], schemaPath); value != "" {
			return value
		}
	}
	// Check unstruct_event field
	if unstructIdx, ok := o.columnIndexMap["unstruct_event"]; ok && unstructIdx < len(columns) {
		if value := o.extractSchemaPropertyValue(columns[unstructIdx], schemaPath); value != "" {
			return value
		}
	}
	return ""
}

func (o *opensnowcatProcessor) matchesSchemaProperty(columns []string, schemaPath string, criteria *filterCriteria) bool {

	jsonFields := []string{"contexts", "derived_contexts", "unstruct_event"}

	for _, jsonFieldName := range jsonFields {
		colIndex, exists := o.columnIndexMap[jsonFieldName]
		if !exists || colIndex >= len(columns) {
			continue
		}

		jsonValue := columns[colIndex]
		if jsonValue == "" {
			continue
		}

		propertyValue := o.extractSchemaPropertyValue(jsonValue, schemaPath)
		if propertyValue != "" {
			for _, containsStr := range criteria.contains {
				if strings.Contains(strings.ToLower(propertyValue), strings.ToLower(containsStr)) {
					return true
				}
			}
		}
	}

	return false
}

func (o *opensnowcatProcessor) extractSchemaPropertyValue(jsonValue string, schemaPath string) string {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonValue), &data); err != nil {
		return ""
	}

	return o.searchSchemaProperty(data, schemaPath)
}

func (o *opensnowcatProcessor) searchSchemaProperty(data interface{}, schemaPath string) string {
	switch v := data.(type) {
	case map[string]interface{}:
		if schemaVal, ok := v["schema"].(string); ok && strings.HasPrefix(schemaVal, "iglu:") {
			schemaURI := strings.TrimPrefix(schemaVal, "iglu:")
			parts := strings.SplitN(schemaURI, "/", 2)
			if len(parts) >= 2 {
				vendor := parts[0]
				schemaParts := strings.Split(parts[1], "/")
				if len(schemaParts) > 0 {
					schemaName := schemaParts[0]
					// Build schema key with dots first, then convert to underscores for filter matching
					dottedSchema := vendor + "." + schemaName
					fullSchema := strings.ReplaceAll(dottedSchema, ".", "_")

					if strings.HasPrefix(schemaPath, fullSchema+".") {
						propertyPath := strings.TrimPrefix(schemaPath, fullSchema+".")

						if dataObj, ok := v["data"].(map[string]interface{}); ok {
							return o.getNestedProperty(dataObj, propertyPath)
						}
					}
				}
			}
		}

		for _, value := range v {
			result := o.searchSchemaProperty(value, schemaPath)
			if result != "" {
				return result
			}
		}

	case []interface{}:
		for _, item := range v {
			result := o.searchSchemaProperty(item, schemaPath)
			if result != "" {
				return result
			}
		}
	}

	return ""
}

func (o *opensnowcatProcessor) getNestedProperty(data map[string]interface{}, path string) string {
	parts := strings.Split(path, ".")

	var current interface{} = data
	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			current = m[part]
		} else {
			return ""
		}
	}

	switch v := current.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return strconv.FormatBool(v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (o *opensnowcatProcessor) anonymizeIP(ipAddress string, transform *fieldTransform) string {
	if strings.Contains(ipAddress, ":") {
		return o.anonymizeIPv6(ipAddress, transform.anonSegments)
	}
	return o.anonymizeIPv4(ipAddress, transform.anonOctets)
}

func (o *opensnowcatProcessor) anonymizeIPv4(ipAddress string, octetsToMask int) string {
	if octetsToMask <= 0 {
		return ipAddress
	}

	parts := strings.Split(ipAddress, ".")
	if len(parts) != 4 {
		o.log.Warnf("Invalid IPv4 address: %s", ipAddress)
		return ipAddress
	}

	// Mask the last N octets
	for i := len(parts) - octetsToMask; i < len(parts); i++ {
		if i >= 0 && i < len(parts) {
			parts[i] = "x"
		}
	}

	return strings.Join(parts, ".")
}

func (o *opensnowcatProcessor) anonymizeIPv6(ipAddress string, segmentsToMask int) string {
	if segmentsToMask <= 0 {
		return ipAddress
	}

	parts := strings.Split(ipAddress, ":")

	maskedCount := 0
	for i := len(parts) - 1; i >= 0 && maskedCount < segmentsToMask; i-- {
		if parts[i] != "" {
			parts[i] = "x"
			maskedCount++
		}
	}

	return strings.Join(parts, ":")
}

func (o *opensnowcatProcessor) hashValue(value string, transform *fieldTransform) string {
	salt := transform.salt
	if salt == "" {
		salt = o.transformConfig.salt
	}

	hashAlgo := transform.hashAlgo
	if hashAlgo == "" {
		hashAlgo = o.transformConfig.hashAlgo
	}

	input := value + salt

	var hasher hash.Hash
	switch strings.ToUpper(hashAlgo) {
	case "MD5":
		hasher = md5.New()
	case "SHA-1":
		hasher = sha1.New()
	case "SHA-256", "":
		hasher = sha256.New()
	case "SHA-384":
		hasher = sha512.New384()
	case "SHA-512":
		hasher = sha512.New()
	default:
		o.log.Warnf("Unknown hash algorithm %s, using SHA-256", hashAlgo)
		hasher = sha256.New()
	}

	hasher.Write([]byte(input))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (o *opensnowcatProcessor) applyTransformations(columns []string) {
	for fieldName, transform := range o.transformConfig.fields {
		colIndex, exists := o.columnIndexMap[fieldName]
		if !exists {
			o.log.Warnf("Transform field %s not found in column map", fieldName)
			continue
		}

		if colIndex >= len(columns) {
			continue
		}

		originalValue := columns[colIndex]
		if originalValue == "" {
			continue
		}

		switch transform.strategy {
		case "hash":
			columns[colIndex] = o.hashValue(originalValue, transform)
		case "redact":
			columns[colIndex] = transform.redactValue
		case "anonymize_ip":
			columns[colIndex] = o.anonymizeIP(originalValue, transform)
		default:
			o.log.Warnf("Unknown transform strategy: %s", transform.strategy)
		}
	}
}

func (o *opensnowcatProcessor) restructureForEnrichedJSON(eventMap map[string]interface{}, columns []string) map[string]interface{} {
	result := make(map[string]interface{})

	contextsMap := o.parseContextsFromTSV(columns, "contexts")
	derivedContextsMap := o.parseContextsFromTSV(columns, "derived_contexts")
	unstructEvent := o.parseUnstructEventFromTSV(columns)

	for key, value := range eventMap {
		if !strings.HasPrefix(key, "contexts_") &&
			!strings.HasPrefix(key, "derived_contexts_") &&
			!strings.HasPrefix(key, "unstruct_event_") {
			result[key] = value
		}
	}

	if len(contextsMap) > 0 {
		result["contexts"] = contextsMap
	}
	if unstructEvent != nil {
		result["unstruct_event"] = unstructEvent
	}
	if len(derivedContextsMap) > 0 {
		result["derived_contexts"] = derivedContextsMap
	}

	return result
}

func (o *opensnowcatProcessor) parseContextsFromTSV(columns []string, fieldName string) map[string]map[string]interface{} {
	contextsMap := make(map[string]map[string]interface{})

	colIndex, exists := o.columnIndexMap[fieldName]
	if !exists || colIndex >= len(columns) {
		return contextsMap
	}

	jsonValue := columns[colIndex]
	if jsonValue == "" {
		return contextsMap
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonValue), &data); err != nil {
		o.log.Warnf("Failed to parse %s JSON: %v", fieldName, err)
		return contextsMap
	}

	switch v := data.(type) {
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				o.processContextItem(m, contextsMap)
			}
		}
	case map[string]interface{}:
		o.processContextItem(v, contextsMap)
	}

	return contextsMap
}

// processContextItem processes a single context item and unwraps Snowplow wrapper schemas
func (o *opensnowcatProcessor) processContextItem(item map[string]interface{}, contextsMap map[string]map[string]interface{}) {
	schemaURI, ok := item["schema"].(string)
	if !ok {
		return
	}

	vendor, name, version := o.parseSchemaURI(schemaURI)
	if name == "" {
		return
	}

	// For filtering, we use the dotted format (e.g., com.vendor.schema)
	dottedSchemaKey := vendor + "." + name

	// Check if this is a Snowplow wrapper schema (com.snowplowanalytics.snowplow.contexts)
	// These wrapper schemas contain an array of actual contexts in their data field
	if dottedSchemaKey == "com.snowplowanalytics.snowplow.contexts" {
		// Unwrap: extract the actual contexts from inside the wrapper
		if dataField, ok := item["data"].([]interface{}); ok {
			for _, nestedItem := range dataField {
				if nestedMap, ok := nestedItem.(map[string]interface{}); ok {
					// Recursively process the unwrapped context
					o.processContextItem(nestedMap, contextsMap)
				}
			}
		}
		// Don't add the wrapper itself to the output
		return
	}

	// For output, we convert dots to underscores for database compatibility
	// Replace all dots in the full schema key (vendor.name) with underscores
	schemaKey := strings.ReplaceAll(dottedSchemaKey, ".", "_")

	// Regular context - add it to the map
	if _, exists := contextsMap[schemaKey]; !exists {
		contextsMap[schemaKey] = map[string]interface{}{
			"version": version,
			"data":    []interface{}{},
		}
	}

	if dataField, ok := item["data"]; ok {
		dataArray := contextsMap[schemaKey]["data"].([]interface{})
		dataArray = append(dataArray, dataField)
		contextsMap[schemaKey]["data"] = dataArray
	}
}

func (o *opensnowcatProcessor) parseUnstructEventFromTSV(columns []string) map[string]interface{} {
	colIndex, exists := o.columnIndexMap["unstruct_event"]
	if !exists || colIndex >= len(columns) {
		return nil
	}

	jsonValue := columns[colIndex]
	if jsonValue == "" {
		return nil
	}

	var unstructMap map[string]interface{}
	if err := json.Unmarshal([]byte(jsonValue), &unstructMap); err != nil {
		o.log.Warnf("Failed to parse unstruct_event JSON: %v", err)
		return nil
	}

	schemaURI, ok := unstructMap["schema"].(string)
	if !ok {
		return nil
	}

	vendor, name, version := o.parseSchemaURI(schemaURI)
	if name == "" {
		return nil
	}

	// For output, we convert dots to underscores for database compatibility
	dottedSchemaKey := vendor + "." + name
	schemaKey := strings.ReplaceAll(dottedSchemaKey, ".", "_")

	var dataArray []interface{}
	if data, ok := unstructMap["data"]; ok {
		dataArray = []interface{}{data}
	}

	return map[string]interface{}{
		schemaKey: map[string]interface{}{
			"version": version,
			"data":    dataArray,
		},
	}
}

func (o *opensnowcatProcessor) parseSchemaURI(schemaURI string) (string, string, string) {
	if !strings.HasPrefix(schemaURI, "iglu:") {
		return "", "", ""
	}

	uri := strings.TrimPrefix(schemaURI, "iglu:")

	parts := strings.Split(uri, "/")
	if len(parts) < 4 {
		return "", "", ""
	}

	vendor := parts[0]
	schemaName := parts[1]
	// parts[2] is "jsonschema" or format - skip it
	version := parts[3]

	return vendor, schemaName, version
}

func (o *opensnowcatProcessor) Close(context.Context) error {
	if o.schemaDiscovery != nil {
		o.schemaDiscovery.stop()
	}
	return nil
}
