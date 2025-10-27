package opensnowcat

import (
	"context"
	_ "embed"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

//go:embed testdata/page_view.tsv
var testPageViewTSVRaw string

var testPageViewTSV = strings.TrimSuffix(testPageViewTSVRaw, "\n")

// TestProcessPageViewJSON tests that a real page_view TSV is converted to flattened JSON
func TestProcessPageViewJSON(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	res := service.MockResources()
	proc := &opensnowcatProcessor{
		outputFormat:   "json",
		columnIndexMap: columnIndexMap,
		dropFilters:    make(map[string]*filterCriteria),
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Should process one message")

	// Parse JSON output
	msgBytes, err := msgs[0].AsBytes()
	require.NoError(t, err)
	var jsonOutput map[string]interface{}
	err = json.Unmarshal(msgBytes, &jsonOutput)
	require.NoError(t, err)

	// Verify basic fields
	assert.Equal(t, "snwcat", jsonOutput["app_id"])
	assert.Equal(t, "page_view", jsonOutput["event"])
	assert.Equal(t, "9fd5fd06-24ad-471b-9f73-f1a054cb0b31", jsonOutput["event_id"])
	assert.Equal(t, "joaocorreia", jsonOutput["user_id"])

	// Verify contexts are arrays (not over-flattened)
	assert.Contains(t, jsonOutput, "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1")
	uaContexts := jsonOutput["contexts_com_snowplowanalytics_snowplow_ua_parser_context_1"].([]interface{})
	require.Len(t, uaContexts, 1)
	uaContext := uaContexts[0].(map[string]interface{})
	assert.Equal(t, "Chrome", uaContext["useragentFamily"])
	assert.Equal(t, "Mac OS X", uaContext["osFamily"])

	assert.Contains(t, jsonOutput, "contexts_com_snowplowanalytics_snowplow_web_page_1")
	webPageContexts := jsonOutput["contexts_com_snowplowanalytics_snowplow_web_page_1"].([]interface{})
	require.Len(t, webPageContexts, 1)
	webPageContext := webPageContexts[0].(map[string]interface{})
	assert.Equal(t, "9689656e-ebab-4c10-9413-59a6dcefadd2", webPageContext["id"])

	assert.Contains(t, jsonOutput, "contexts_com_fingerprintjs_fingerprint_1")
	fpContexts := jsonOutput["contexts_com_fingerprintjs_fingerprint_1"].([]interface{})
	require.Len(t, fpContexts, 1)
	fpContext := fpContexts[0].(map[string]interface{})
	assert.Equal(t, "nmnY3NEe0lGJc4tzh5KM", fpContext["visitorId"])

	// Verify nested objects are preserved
	assert.Contains(t, jsonOutput, "contexts_com_dbip_location_1")
	locationContexts := jsonOutput["contexts_com_dbip_location_1"].([]interface{})
	require.Len(t, locationContexts, 1)
	locationContext := locationContexts[0].(map[string]interface{})
	cityMap := locationContext["city"].(map[string]interface{})
	namesMap := cityMap["names"].(map[string]interface{})
	assert.Equal(t, "Del Mar", namesMap["en"])

	assert.Contains(t, jsonOutput, "contexts_com_clearbit_company_1")
	clearbitContexts := jsonOutput["contexts_com_clearbit_company_1"].([]interface{})
	require.Len(t, clearbitContexts, 1)
	clearbitContext := clearbitContexts[0].(map[string]interface{})
	assert.Equal(t, "SnowcatCloud", clearbitContext["name"])

	// Verify arrays within contexts are preserved
	assert.Contains(t, jsonOutput, "contexts_org_ietf_http_cookie_1")
	cookieContexts := jsonOutput["contexts_org_ietf_http_cookie_1"].([]interface{})
	require.GreaterOrEqual(t, len(cookieContexts), 2)
	cookie0 := cookieContexts[0].(map[string]interface{})
	assert.Equal(t, "_gaexp", cookie0["name"])
	cookie1 := cookieContexts[1].(map[string]interface{})
	assert.Equal(t, "ajs_user_id", cookie1["name"])

	// Verify nested arrays are preserved
	techArray := clearbitContext["tech"].([]interface{})
	require.GreaterOrEqual(t, len(techArray), 1)
	assert.Equal(t, "google_apps", techArray[0])

}

// TestProcessPageViewTSV_FilterByIP tests that filtering by IP address drops matching events
func TestProcessPageViewTSV_FilterByIP(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"user_ipaddress": {
				contains: []string{"75.80.110.186"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because IP matches filter")

}

// TestProcessPageViewTSV_FilterBySchemaProperty tests filtering by schema property value
func TestProcessPageViewTSV_FilterBySchemaProperty(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test filtering by useragentFamily property in derived_contexts
	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily": {
				contains: []string{"Chrome"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because useragentFamily is Chrome")

}

// TestProcessPageViewTSV_FilterBySchemaProperty_NoMatch tests that events without matching property values are not dropped
func TestProcessPageViewTSV_FilterBySchemaProperty_NoMatch(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test filtering by a property value that doesn't match
	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily": {
				contains: []string{"Firefox"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Event should NOT be dropped because useragentFamily is not Firefox")
	msgBytes, err := msgs[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testPageViewTSV, string(msgBytes))

}

// TestProcessPageViewTSV_FilterBySchemaProperty_osFamily tests filtering by osFamily property
func TestProcessPageViewTSV_FilterBySchemaProperty_osFamily(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test filtering by osFamily property in derived_contexts
	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"com.snowplowanalytics.snowplow.ua_parser_context.osFamily": {
				contains: []string{"Mac OS X"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because osFamily is Mac OS X")

}

// TestProcessPageViewTSV_FilterCombinedRegularAndSchemaProperty tests combining regular field and schema property filters
func TestProcessPageViewTSV_FilterCombinedRegularAndSchemaProperty(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test combining regular field filter (useragent) with schema property filter
	// This matches the exact format from the user's example
	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"useragent": {
				contains: []string{"bot", "crawler", "spider"},
			},
			"com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily": {
				contains: []string{"Chrome"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because useragentFamily contains Chrome")

}

// TestProcessPageViewTSV_FilterMultipleConditions tests multiple filters with OR logic
func TestProcessPageViewTSV_FilterMultipleConditions(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test with multiple filters - should drop if ANY match (OR logic)
	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"user_ipaddress": {
				contains: []string{"192.168.1.1"}, // Won't match (actual IP is 75.80.110.186)
			},
			"useragent": {
				contains: []string{"bot", "crawler"}, // Won't match (actual useragent is Chrome)
			},
			"com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily": {
				contains: []string{"Firefox", "Safari"}, // Won't match (actual is Chrome)
			},
			"com.snowplowanalytics.snowplow.ua_parser_context.osFamily": {
				contains: []string{"Mac OS X"}, // WILL MATCH - should drop the event
			},
			"nl.basjes.yauaa_context.deviceClass": {
				contains: []string{"Phone"}, // Won't match (actual is Desktop)
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because osFamily matches 'Mac OS X' (OR logic)")

}

// TestProcessPageViewTSV_FilterMultipleConditions_NoMatch tests multiple filters where none match
func TestProcessPageViewTSV_FilterMultipleConditions_NoMatch(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test with multiple filters - NONE should match, so event should NOT be dropped
	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"user_ipaddress": {
				contains: []string{"192.168.1.1", "10.0.0.1"}, // Won't match
			},
			"useragent": {
				contains: []string{"bot", "crawler", "spider"}, // Won't match
			},
			"com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily": {
				contains: []string{"Firefox", "Safari", "Edge"}, // Won't match
			},
			"com.snowplowanalytics.snowplow.ua_parser_context.osFamily": {
				contains: []string{"Windows", "Linux", "Android"}, // Won't match
			},
			"nl.basjes.yauaa_context.deviceClass": {
				contains: []string{"Phone", "Tablet"}, // Won't match
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Event should NOT be dropped because none of the filters match")
	msgBytes, err := msgs[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testPageViewTSV, string(msgBytes))

}

// TestProcessPageViewTSV_TransformFields tests field transformations
func TestProcessPageViewTSV_TransformFields(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: make(map[string]*filterCriteria),
		transformConfig: &transformConfig{
			salt:     "test-salt-12345",
			hashAlgo: "SHA-256",
			fields: map[string]*fieldTransform{
				"user_ipaddress": {
					strategy:     "anonymize_ip",
					anonOctets:   2,
					anonSegments: 4,
				},
				"user_id": {
					strategy: "hash",
					hashAlgo: "SHA-256",
					salt:     "test-salt-12345",
				},
				"network_userid": {
					strategy:    "redact",
					redactValue: "[REDACTED]",
				},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Should process one message with transformations applied")

	// Parse TSV output
	msgBytes, err := msgs[0].AsBytes()
	require.NoError(t, err)
	columns := strings.Split(string(msgBytes), "\t")

	// Verify user_ipaddress is anonymized (75.80.110.186 -> 75.80.x.x)
	userIPIndex := columnIndexMap["user_ipaddress"]
	assert.Equal(t, "75.80.x.x", columns[userIPIndex], "user_ipaddress should have last 2 octets anonymized")

	// Verify user_id is hashed (should not be "joaocorreia" anymore)
	userIDIndex := columnIndexMap["user_id"]
	assert.NotEqual(t, "joaocorreia", columns[userIDIndex], "user_id should be hashed")
	assert.NotEmpty(t, columns[userIDIndex], "user_id should not be empty")
	// SHA-256 produces 64 character hex string
	assert.Len(t, columns[userIDIndex], 64, "SHA-256 hash should be 64 characters")

	// Verify network_userid is redacted
	networkUserIDIndex := columnIndexMap["network_userid"]
	assert.Equal(t, "[REDACTED]", columns[networkUserIDIndex], "network_userid should be redacted")

	// Verify other fields remain unchanged (spot check a few)
	assert.Equal(t, "snwcat", columns[columnIndexMap["app_id"]], "app_id should remain unchanged")
	assert.Equal(t, "page_view", columns[columnIndexMap["event"]], "event should remain unchanged")
	assert.Equal(t, "9fd5fd06-24ad-471b-9f73-f1a054cb0b31", columns[columnIndexMap["event_id"]], "event_id should remain unchanged")
}

// TestProcessPageViewJSON_TransformFields tests field transformations with JSON output
func TestProcessPageViewJSON_TransformFields(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	res := service.MockResources()
	proc := &opensnowcatProcessor{
		dropFilters: make(map[string]*filterCriteria),
		transformConfig: &transformConfig{
			salt:     "test-salt-12345",
			hashAlgo: "SHA-256",
			fields: map[string]*fieldTransform{
				"user_ipaddress": {
					strategy:     "anonymize_ip",
					anonOctets:   2,
					anonSegments: 4,
				},
				"user_id": {
					strategy: "hash",
					hashAlgo: "SHA-256",
					salt:     "test-salt-12345",
				},
				"network_userid": {
					strategy:    "redact",
					redactValue: "[REDACTED]",
				},
			},
		},
		outputFormat:   "json",
		columnIndexMap: columnIndexMap,
		log:            res.Logger(),
		mDropped:       res.Metrics().NewCounter("dropped"),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Should process one message with transformations applied")

	// Parse JSON output
	msgBytes, err := msgs[0].AsBytes()
	require.NoError(t, err)
	var jsonOutput map[string]interface{}
	err = json.Unmarshal(msgBytes, &jsonOutput)
	require.NoError(t, err)

	// Verify user_ipaddress is anonymized
	assert.Equal(t, "75.80.x.x", jsonOutput["user_ipaddress"], "user_ipaddress should have last 2 octets anonymized")

	// Verify user_id is hashed
	assert.NotEqual(t, "joaocorreia", jsonOutput["user_id"], "user_id should be hashed")
	assert.NotEmpty(t, jsonOutput["user_id"], "user_id should not be empty")
	userIDStr, ok := jsonOutput["user_id"].(string)
	require.True(t, ok, "user_id should be a string")
	assert.Len(t, userIDStr, 64, "SHA-256 hash should be 64 characters")

	// Verify network_userid is redacted
	assert.Equal(t, "[REDACTED]", jsonOutput["network_userid"], "network_userid should be redacted")

	// Verify other fields remain unchanged
	assert.Equal(t, "snwcat", jsonOutput["app_id"], "app_id should remain unchanged")
	assert.Equal(t, "page_view", jsonOutput["event"], "event should remain unchanged")
	assert.Equal(t, "9fd5fd06-24ad-471b-9f73-f1a054cb0b31", jsonOutput["event_id"], "event_id should remain unchanged")
}
