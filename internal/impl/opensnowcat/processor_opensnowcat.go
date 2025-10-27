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

	analytics "github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	oscFieldFilters      = "filters"
	oscFieldFiltersDrop  = "drop"
	oscFieldOutputFormat = "output_format"
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
			"json": "Convert enriched TSV to flattened JSON with contexts, derived_contexts, and unstruct_event automatically flattened into top-level objects.",
			"tsv":  "Maintain enriched TSV format without conversion.",
		}).Description("Output format for processed events.").Default("tsv")).
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
			"Combined",
			"Drops unwanted events while transforming sensitive fields in the remaining events. Useful for processing only relevant data while maintaining privacy.",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: json
        filters:
          drop:
            user_ipaddress:
              contains: ["127.0.0.1", "10.0.", "192.168."]
            com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily:
              contains: ["bot", "crawler", "spider"]
          transform:
            salt: "production-salt-v1"
            hash_algo: SHA-256
            fields:
              user_id:
                strategy: hash
              user_ipaddress:
                strategy: anonymize_ip
                anon_octets: 2
                anon_segments: 3
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
	schemas  []string // Schema patterns like "com.snowplowanalytics.snowplow.ua_parser_context" or "com.vendor.request"
}

type fieldTransform struct {
	strategy     string // "hash", "redact", "anonymize_ip"
	hashAlgo     string // "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512"
	salt         string
	redactValue  string
	anonOctets   int // For IPv4 anonymization
	anonSegments int // For IPv6 anonymization
}

type transformConfig struct {
	salt     string
	hashAlgo string
	fields   map[string]*fieldTransform
}

type opensnowcatProcessor struct {
	dropFilters     map[string]*filterCriteria
	transformConfig *transformConfig
	outputFormat    string
	columnIndexMap  map[string]int
	log             *service.Logger
	mDropped        *service.MetricCounter
}

func newOpenSnowcatProcessorFromConfig(conf *service.ParsedConfig, res *service.Resources) (*opensnowcatProcessor, error) {
	outputFormat := "tsv"

	// Get output format
	if conf.Contains(oscFieldOutputFormat) {
		format, err := conf.FieldString(oscFieldOutputFormat)
		if err != nil {
			return nil, err
		}
		outputFormat = format
	}

	// Build column index map for TSV parsing (all lowercase)
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Parse filter configuration
	dropFilters := make(map[string]*filterCriteria)
	var transformCfg *transformConfig

	if conf.Contains(oscFieldFilters) {
		filtersConf, err := conf.FieldAnyMap(oscFieldFilters)
		if err != nil {
			return nil, err
		}

		// Parse drop filters
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

		// Parse transform config
		if transformParsed, exists := filtersConf["transform"]; exists {
			transformAny, _ := transformParsed.FieldAny()
			if transformMap, ok := transformAny.(map[string]interface{}); ok {
				transformCfg = &transformConfig{
					fields: make(map[string]*fieldTransform),
				}

				// Parse global salt
				if salt, ok := transformMap["salt"].(string); ok {
					transformCfg.salt = salt
				}

				// Parse global hash_algo
				if hashAlgo, ok := transformMap["hash_algo"].(string); ok {
					transformCfg.hashAlgo = hashAlgo
				} else {
					transformCfg.hashAlgo = "SHA-256"
				}

				// Parse fields
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

							// Normalize field name to lowercase for column matching, but keep schema paths case-sensitive
							normalizedFieldName := fieldName
							if !strings.Contains(fieldName, ".") || strings.HasPrefix(fieldName, "geo.") || strings.HasPrefix(fieldName, "metrics.") || strings.HasPrefix(fieldName, "site.") {
								// Regular column name - normalize to lowercase
								normalizedFieldName = strings.ToLower(fieldName)
							}
							transformCfg.fields[normalizedFieldName] = ft
						}
					}
				}
			}
		}
	}

	return &opensnowcatProcessor{
		dropFilters:     dropFilters,
		transformConfig: transformCfg,
		outputFormat:    outputFormat,
		columnIndexMap:  columnIndexMap,
		log:             res.Logger(),
		mDropped:        res.Metrics().NewCounter("dropped"),
	}, nil
}

func (o *opensnowcatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	tsvBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	tsvString := string(tsvBytes)

	// Parse TSV into columns
	columns := strings.Split(tsvString, "\t")

	// Check if we have filters to apply
	if len(o.dropFilters) > 0 {
		if o.shouldDropEventFromTSV(columns) {
			o.mDropped.Incr(1)
			return nil, nil // Drop the event
		}
	}

	// Apply field transformations if configured
	if o.transformConfig != nil && len(o.transformConfig.fields) > 0 {
		o.applyTransformations(columns)
	}

	// If output format is TSV, reconstruct and return the TSV
	if o.outputFormat == "tsv" {
		transformedTSV := strings.Join(columns, "\t")
		msg = service.NewMessage([]byte(transformedTSV))
		return service.MessageBatch{msg}, nil
	}

	// For JSON output, reconstruct TSV then use the Snowplow SDK to parse
	transformedTSV := strings.Join(columns, "\t")
	parsedEvent, err := analytics.ParseEvent(transformedTSV)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OpenSnowcat event: %w", err)
	}

	// Convert to map - Snowplow SDK already extracts contexts nicely
	eventMap, err := parsedEvent.ToMap()
	if err != nil {
		return nil, fmt.Errorf("failed to convert event to map: %w", err)
	}

	// Set the map as structured data (no extra flattening needed)
	msg.SetStructuredMut(eventMap)

	return service.MessageBatch{msg}, nil
}

func (o *opensnowcatProcessor) shouldDropEventFromTSV(columns []string) bool {
	for fieldName, criteria := range o.dropFilters {
		// Check if this is a schema property path (e.g., "com.vendor.schema.property")
		if strings.Contains(fieldName, ".") && !strings.HasPrefix(fieldName, "geo.") && !strings.HasPrefix(fieldName, "metrics.") && !strings.HasPrefix(fieldName, "site.") {
			// This is a schema property path
			if o.matchesSchemaProperty(columns, fieldName, criteria) {
				return true
			}
			continue
		}

		// Regular field filter
		colIndex, exists := o.columnIndexMap[fieldName]
		if !exists {
			o.log.Warnf("Filter field %s not found in column map", fieldName)
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

// anonymizeIP anonymizes an IP address by masking octets (IPv4) or segments (IPv6)
func (o *opensnowcatProcessor) anonymizeIP(ipAddress string, transform *fieldTransform) string {
	// Determine if this is IPv4 or IPv6
	if strings.Contains(ipAddress, ":") {
		// IPv6
		return o.anonymizeIPv6(ipAddress, transform.anonSegments)
	}
	// IPv4
	return o.anonymizeIPv4(ipAddress, transform.anonOctets)
}

// anonymizeIPv4 anonymizes an IPv4 address by masking the last N octets
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

// anonymizeIPv6 anonymizes an IPv6 address by masking the last N segments
func (o *opensnowcatProcessor) anonymizeIPv6(ipAddress string, segmentsToMask int) string {
	if segmentsToMask <= 0 {
		return ipAddress
	}

	// Handle compressed IPv6 addresses (::)
	parts := strings.Split(ipAddress, ":")

	// Mask the last N segments
	maskedCount := 0
	for i := len(parts) - 1; i >= 0 && maskedCount < segmentsToMask; i-- {
		if parts[i] != "" {
			parts[i] = "x"
			maskedCount++
		}
	}

	return strings.Join(parts, ":")
}

// hashValue hashes a value using the specified algorithm and salt
func (o *opensnowcatProcessor) hashValue(value string, transform *fieldTransform) string {
	// Determine which salt to use (field-specific or global default)
	salt := transform.salt
	if salt == "" {
		salt = o.transformConfig.salt
	}

	// Determine which hash algorithm to use (field-specific or global default)
	hashAlgo := transform.hashAlgo
	if hashAlgo == "" {
		hashAlgo = o.transformConfig.hashAlgo
	}

	// Combine value with salt
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

// applyTransformations applies field transformations to TSV columns in-place
func (o *opensnowcatProcessor) applyTransformations(columns []string) {
	for fieldName, transform := range o.transformConfig.fields {
		// Get column index
		colIndex, exists := o.columnIndexMap[fieldName]
		if !exists {
			o.log.Warnf("Transform field %s not found in column map", fieldName)
			continue
		}

		// Check if we have enough columns
		if colIndex >= len(columns) {
			continue
		}

		originalValue := columns[colIndex]
		if originalValue == "" {
			continue // Skip empty values
		}

		// Apply transformation based on strategy
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

func (o *opensnowcatProcessor) Close(context.Context) error {
	return nil
}
