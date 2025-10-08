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

	proc := &opensnowcatProcessor{
		flattenFields: map[string]bool{
			"contexts":         true,
			"derived_contexts": true,
			"unstruct_event":   true,
		},
		prefixSeparator: "_",
		outputFormat:    "json",
		columnIndexMap:  columnIndexMap,
		dropFilters:     make(map[string]*filterCriteria),
		log:             service.MockResources().Logger(),
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

	t.Logf("✅ Page view converted to JSON with %d fields", len(jsonOutput))

	// Verify flattened context fields exist with underscore separator
	assert.Contains(t, jsonOutput, "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_useragentFamily")
	assert.Equal(t, "Chrome", jsonOutput["contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_useragentFamily"])

	assert.Contains(t, jsonOutput, "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_osFamily")
	assert.Equal(t, "Mac OS X", jsonOutput["contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_osFamily"])

	assert.Contains(t, jsonOutput, "contexts_com_snowplowanalytics_snowplow_web_page_1_0_id")
	assert.Equal(t, "9689656e-ebab-4c10-9413-59a6dcefadd2", jsonOutput["contexts_com_snowplowanalytics_snowplow_web_page_1_0_id"])

	assert.Contains(t, jsonOutput, "contexts_com_fingerprintjs_fingerprint_1_0_visitorId")
	assert.Equal(t, "nmnY3NEe0lGJc4tzh5KM", jsonOutput["contexts_com_fingerprintjs_fingerprint_1_0_visitorId"])

	// Verify deeply nested fields are flattened with underscore separator
	assert.Contains(t, jsonOutput, "contexts_com_dbip_location_1_0_city_names_en")
	assert.Equal(t, "Del Mar", jsonOutput["contexts_com_dbip_location_1_0_city_names_en"])

	assert.Contains(t, jsonOutput, "contexts_com_dbip_location_1_0_country_names_en")
	assert.Equal(t, "United States", jsonOutput["contexts_com_dbip_location_1_0_country_names_en"])

	assert.Contains(t, jsonOutput, "contexts_com_clearbit_company_1_0_name")
	assert.Equal(t, "SnowcatCloud", jsonOutput["contexts_com_clearbit_company_1_0_name"])

	// Verify arrays within contexts are flattened with numeric indices
	assert.Contains(t, jsonOutput, "contexts_org_ietf_http_cookie_1_0_name")
	assert.Equal(t, "_gaexp", jsonOutput["contexts_org_ietf_http_cookie_1_0_name"])

	assert.Contains(t, jsonOutput, "contexts_org_ietf_http_cookie_1_1_name")
	assert.Equal(t, "ajs_user_id", jsonOutput["contexts_org_ietf_http_cookie_1_1_name"])

	// Verify nested arrays are flattened
	assert.Contains(t, jsonOutput, "contexts_com_clearbit_company_1_0_tech_0")
	assert.Equal(t, "google_apps", jsonOutput["contexts_com_clearbit_company_1_0_tech_0"])

	t.Logf("✅ Verified flattened JSON structure with underscore separators and proper deep nesting")
}

// TestProcessPageViewTSV_FilterByIP tests that filtering by IP address drops matching events
func TestProcessPageViewTSV_FilterByIP(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"user_ipaddress": {
				contains: []string{"75.80.110.186"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            service.MockResources().Logger(),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because IP matches filter")

	t.Logf("✅ Event with IP 75.80.110.186 was correctly dropped")
}

// TestProcessPageViewTSV_FilterBySchemaProperty tests filtering by schema property value
func TestProcessPageViewTSV_FilterBySchemaProperty(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test filtering by useragentFamily property in derived_contexts
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily": {
				contains: []string{"Chrome"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            service.MockResources().Logger(),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because useragentFamily is Chrome")

	t.Logf("✅ Event with useragentFamily=Chrome was correctly dropped")
}

// TestProcessPageViewTSV_FilterBySchemaProperty_NoMatch tests that events without matching property values are not dropped
func TestProcessPageViewTSV_FilterBySchemaProperty_NoMatch(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test filtering by a property value that doesn't match
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"com.snowplowanalytics.snowplow.ua_parser_context.useragentFamily": {
				contains: []string{"Firefox"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            service.MockResources().Logger(),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Event should NOT be dropped because useragentFamily is not Firefox")
	msgBytes, err := msgs[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testPageViewTSV, string(msgBytes))

	t.Logf("✅ Event with useragentFamily=Chrome (not Firefox) was correctly kept")
}

// TestProcessPageViewTSV_FilterBySchemaProperty_osFamily tests filtering by osFamily property
func TestProcessPageViewTSV_FilterBySchemaProperty_osFamily(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test filtering by osFamily property in derived_contexts
	proc := &opensnowcatProcessor{
		dropFilters: map[string]*filterCriteria{
			"com.snowplowanalytics.snowplow.ua_parser_context.osFamily": {
				contains: []string{"Mac OS X"},
			},
		},
		outputFormat:   "tsv",
		columnIndexMap: columnIndexMap,
		log:            service.MockResources().Logger(),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because osFamily is Mac OS X")

	t.Logf("✅ Event with osFamily='Mac OS X' was correctly dropped")
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
		log:            service.MockResources().Logger(),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because useragentFamily contains Chrome")

	t.Logf("✅ Event with useragentFamily containing 'Chrome' was correctly dropped (combined filter test)")
}

// TestProcessPageViewTSV_FilterMultipleConditions tests multiple filters with OR logic
func TestProcessPageViewTSV_FilterMultipleConditions(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test with multiple filters - should drop if ANY match (OR logic)
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
		log:            service.MockResources().Logger(),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because osFamily matches 'Mac OS X' (OR logic)")

	t.Logf("✅ Event correctly dropped with multiple filters using OR logic (osFamily='Mac OS X' matched)")
}

// TestProcessPageViewTSV_FilterMultipleConditions_NoMatch tests multiple filters where none match
func TestProcessPageViewTSV_FilterMultipleConditions_NoMatch(t *testing.T) {
	// Build column index map
	columnIndexMap := make(map[string]int)
	for i, col := range opensnowcatColumns {
		columnIndexMap[col] = i
	}

	// Test with multiple filters - NONE should match, so event should NOT be dropped
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
		log:            service.MockResources().Logger(),
	}

	msg := service.NewMessage([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Event should NOT be dropped because none of the filters match")
	msgBytes, err := msgs[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testPageViewTSV, string(msgBytes))

	t.Logf("✅ Event correctly kept when multiple filters don't match (Chrome on Mac OS X, Desktop)")
}
