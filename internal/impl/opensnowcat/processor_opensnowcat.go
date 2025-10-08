package opensnowcat

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	analytics "github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	oscFieldFilters      = "filters"
	oscFieldFiltersDrop  = "drop"
	oscFieldOutputFormat = "output_format"
)

// OpenSnowcat TSV column names (lowercase, 131 columns)
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
		Summary("Processes [OpenSnowcat](https://opensnowcat.io/)/[Snowplow enriched TSV](https://docs.snowplow.io/docs/fundamentals/canonical-event/understanding-the-enriched-tsv-format/) events. Convert enriched TSV to flattened JSON, or filter events based on payload field values.").
		Version("1.0.0").
		Field(service.NewStringField(oscFieldOutputFormat).
			Description("Output format: `json` for flattened JSON, `tsv` to maintain enriched TSV format. When `json` is selected, contexts, derived_contexts, and unstruct_event are automatically flattened into top-level json objects").
			Default("tsv").
			Optional()).
		Field(service.NewObjectField(oscFieldFilters,
			service.NewAnyMapField(oscFieldFiltersDrop).
				Description("Map of field names to filter criteria. Events matching ANY criteria will be dropped (OR logic). Supports both regular TSV columns (e.g., `user_ipaddress`, `useragent`) and schema property paths (e.g., `com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily`). Each filter uses 'contains' for substring matching.").
				Optional(),
		).Description("Filter configuration to drop events based on field values").Optional()).
		Example(
			"Enriched TSV to JSON",
			"Converts OpenSnowcat/Snowplow enriched TSV events to flattened JSON format, extracting all contexts, derived contexts, and unstruct events into top-level fields.",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: json

`,
		).
		Example(
			"Filter by IP address",
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
			"Filter by schema property values",
			"Filters events based on schema property values. The processor automatically searches contexts, derived_contexts, and unstruct_event fields for matching vendor, schemas and properties.",
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
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"opensnowcat",
		opensnowcatProcessorConfig(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			mgr := interop.UnwrapManagement(res)
			p, err := newOpenSnowcatProcessorFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedProcessor("opensnowcat", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type filterCriteria struct {
	contains []string
	schemas  []string // Schema patterns like "com.snowplowanalytics.snowplow.ua_parser_context" or "com.wunderflats.request_confirmed"
}

type opensnowcatProcessor struct {
	flattenFields   map[string]bool
	prefixSeparator string
	dropFilters     map[string]*filterCriteria
	outputFormat    string
	columnIndexMap  map[string]int
	log             log.Modular
}

func newOpenSnowcatProcessorFromConfig(conf *service.ParsedConfig, mgr bundle.NewManagement) (*opensnowcatProcessor, error) {
	outputFormat := "json"

	// Get output format
	if conf.Contains(oscFieldOutputFormat) {
		format, err := conf.FieldString(oscFieldOutputFormat)
		if err != nil {
			return nil, err
		}
		outputFormat = format
	}

	// Always flatten these three fields with underscore separator (only used for JSON output)
	flattenFields := map[string]bool{
		"contexts":         true,
		"derived_contexts": true,
		"unstruct_event":   true,
	}
	prefixSeparator := "_"

	// Build column index map for TSV parsing (all lowercase)
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Parse filter configuration
	dropFilters := make(map[string]*filterCriteria)
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

						// Parse "contains" filter
						if containsList, ok := criteriaMap["contains"]; ok {
							if containsSlice, ok := containsList.([]interface{}); ok {
								for _, item := range containsSlice {
									if str, ok := item.(string); ok {
										fc.contains = append(fc.contains, str)
									}
								}
							}
						}

						// Parse "schemas" filter
						if schemasList, ok := criteriaMap["schemas"]; ok {
							if schemasSlice, ok := schemasList.([]interface{}); ok {
								for _, item := range schemasSlice {
									if str, ok := item.(string); ok {
										fc.schemas = append(fc.schemas, str)
									}
								}
							}
						}

						// Only add filter if it has criteria
						if len(fc.contains) > 0 || len(fc.schemas) > 0 {
							// Normalize field name to lowercase for column matching, but keep schema paths case-sensitive
							normalizedFieldName := fieldName
							if !strings.Contains(fieldName, ".") || strings.HasPrefix(fieldName, "geo.") || strings.HasPrefix(fieldName, "metrics.") || strings.HasPrefix(fieldName, "site.") {
								// Regular column name - normalize to lowercase
								normalizedFieldName = strings.ToLower(fieldName)
							}
							dropFilters[normalizedFieldName] = fc
						}
					}
				}
			}
		}
	}

	return &opensnowcatProcessor{
		flattenFields:   flattenFields,
		prefixSeparator: prefixSeparator,
		dropFilters:     dropFilters,
		outputFormat:    outputFormat,
		columnIndexMap:  columnIndexMap,
		log:             mgr.Logger(),
	}, nil
}

func (o *opensnowcatProcessor) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	tsvBytes := msg.AsBytes()
	tsvString := string(tsvBytes)

	// Parse TSV into columns
	columns := strings.Split(tsvString, "\t")

	// Check if we have filters to apply
	if len(o.dropFilters) > 0 {
		if o.shouldDropEventFromTSV(columns) {
			o.log.Debug("Event dropped by filter\n")
			return nil, nil // Drop the event
		}
	}

	// If output format is TSV, just return the original (filtered) message
	if o.outputFormat == "tsv" {
		return []*message.Part{msg}, nil
	}

	// For JSON output, use the Snowplow SDK to parse
	parsedEvent, err := analytics.ParseEvent(tsvString)
	if err != nil {
		o.log.Error("Failed to parse OpenSnowcat event: %v\n", err)
		return nil, fmt.Errorf("failed to parse OpenSnowcat event: %w", err)
	}

	// Convert to map for flattening
	eventMap, err := parsedEvent.ToMap()
	if err != nil {
		o.log.Error("Failed to convert event to map: %v\n", err)
		return nil, fmt.Errorf("failed to convert event to map: %w", err)
	}

	// Flatten specified fields
	flattenedMap := o.flattenEvent(eventMap)

	// Set the flattened map as structured data
	msg.SetStructuredMut(flattenedMap)

	return []*message.Part{msg}, nil
}

func (o *opensnowcatProcessor) shouldDropEventFromTSV(columns []string) bool {
	for fieldName, criteria := range o.dropFilters {
		// Check if this is a schema property path (e.g., "com.vendor.schema.property")
		if strings.Contains(fieldName, ".") && !strings.HasPrefix(fieldName, "geo.") && !strings.HasPrefix(fieldName, "metrics.") && !strings.HasPrefix(fieldName, "site.") {
			// This is a schema property path
			if o.matchesSchemaProperty(columns, fieldName, criteria) {
				o.log.Debug("Event matched schema property filter: %s\n", fieldName)
				return true
			}
			continue
		}

		// Regular field filter
		colIndex, exists := o.columnIndexMap[fieldName]
		if !exists {
			o.log.Warn("Filter field %s not found in column map\n", fieldName)
			continue
		}

		// Check if we have enough columns
		if colIndex >= len(columns) {
			continue
		}

		fieldValue := columns[colIndex]

		// Check contains criteria
		for _, containsStr := range criteria.contains {
			if strings.Contains(strings.ToLower(fieldValue), strings.ToLower(containsStr)) {
				o.log.Debug("Event matched drop filter: %s contains %s\n", fieldName, containsStr)
				return true
			}
		}
	}
	return false
}

// matchesSchemaProperty checks if a schema property matches the filter criteria
// Format: "com.vendor.schema_name.property.nested.path"
// Example: "com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily"
func (o *opensnowcatProcessor) matchesSchemaProperty(columns []string, schemaPath string, criteria *filterCriteria) bool {
	// Parse the schema path to extract vendor.schema and property path
	// Example: "com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily"
	// Need to find where schema ends and property begins

	// Check in contexts, derived_contexts, and unstruct_event fields
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

		// Try to extract the property value from this JSON field
		propertyValue := o.extractSchemaPropertyValue(jsonValue, schemaPath)
		if propertyValue != "" {
			// Check if the property value matches the filter criteria
			for _, containsStr := range criteria.contains {
				if strings.Contains(strings.ToLower(propertyValue), strings.ToLower(containsStr)) {
					return true
				}
			}
		}
	}

	return false
}

// extractSchemaPropertyValue extracts a property value from a JSON field based on schema path
// Example: "com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily" -> "Chrome"
func (o *opensnowcatProcessor) extractSchemaPropertyValue(jsonValue string, schemaPath string) string {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonValue), &data); err != nil {
		return ""
	}

	// Recursively search for matching schema and extract property
	return o.searchSchemaProperty(data, schemaPath)
}

// searchSchemaProperty recursively searches for a schema and extracts the property value
func (o *opensnowcatProcessor) searchSchemaProperty(data interface{}, schemaPath string) string {
	switch v := data.(type) {
	case map[string]interface{}:
		// Check if this map has a "schema" field
		if schemaVal, ok := v["schema"].(string); ok && strings.HasPrefix(schemaVal, "iglu:") {
			// Parse the schema URI
			schemaURI := strings.TrimPrefix(schemaVal, "iglu:")
			parts := strings.SplitN(schemaURI, "/", 2)
			if len(parts) >= 2 {
				vendor := parts[0]
				schemaParts := strings.Split(parts[1], "/")
				if len(schemaParts) > 0 {
					schemaName := schemaParts[0]
					fullSchema := vendor + "." + schemaName

					// Check if this schema matches the path prefix
					if strings.HasPrefix(schemaPath, fullSchema+".") {
						// Extract the property path
						propertyPath := strings.TrimPrefix(schemaPath, fullSchema+".")

						// Look for the property in the "data" field
						if dataObj, ok := v["data"].(map[string]interface{}); ok {
							return o.getNestedProperty(dataObj, propertyPath)
						}
					}
				}
			}
		}

		// Recursively search nested maps
		for _, value := range v {
			result := o.searchSchemaProperty(value, schemaPath)
			if result != "" {
				return result
			}
		}

	case []interface{}:
		// Recursively search array elements
		for _, item := range v {
			result := o.searchSchemaProperty(item, schemaPath)
			if result != "" {
				return result
			}
		}
	}

	return ""
}

// getNestedProperty gets a nested property value using dot notation
// Example: "geo.country" from {"geo": {"country": "US"}}
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

	// Convert the final value to string
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

func (o *opensnowcatProcessor) flattenEvent(eventMap map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range eventMap {
		shouldFlatten := false
		for prefix := range o.flattenFields {
			if strings.HasPrefix(key, prefix) {
				shouldFlatten = true
				break
			}
		}

		if shouldFlatten {
			// Flatten this field
			if nestedMap, ok := value.(map[string]interface{}); ok {
				o.flattenMap(result, key, nestedMap)
			} else if nestedSlice, ok := value.([]interface{}); ok {
				// Handle array of objects (like contexts)
				o.flattenSlice(result, key, nestedSlice)
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}

	return result
}

func (o *opensnowcatProcessor) flattenMap(target map[string]interface{}, prefix string, source map[string]interface{}) {
	for key, value := range source {
		newKey := prefix + o.prefixSeparator + key
		if nestedMap, ok := value.(map[string]interface{}); ok {
			o.flattenMap(target, newKey, nestedMap)
		} else if nestedSlice, ok := value.([]interface{}); ok {
			o.flattenSlice(target, newKey, nestedSlice)
		} else {
			target[newKey] = value
		}
	}
}

func (o *opensnowcatProcessor) flattenSlice(target map[string]interface{}, prefix string, source []interface{}) {
	for i, item := range source {
		newKey := fmt.Sprintf("%s%s%d", prefix, o.prefixSeparator, i)
		if itemMap, ok := item.(map[string]interface{}); ok {
			o.flattenMap(target, newKey, itemMap)
		} else {
			target[newKey] = item
		}
	}
}

func (o *opensnowcatProcessor) Close(context.Context) error {
	return nil
}
