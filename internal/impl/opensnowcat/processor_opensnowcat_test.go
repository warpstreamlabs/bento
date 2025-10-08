package opensnowcat

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

// Real OpenSnowcat enriched TSV event (page_view with Mac OS)
const testPageViewTSV = `snwcat	web	2022-03-21 22:12:44.790	2022-03-21 22:12:43.358	2022-03-21 22:12:43.007	page_view	9fd5fd06-24ad-471b-9f73-f1a054cb0b31		cf	js-2.14.0	ssc-2.3.0-kinesis	stream-2.0.1-common-2.0.1	joaocorreia	75.80.110.186		d67ba93a-0ada-4f04-a8ce-9634d14fa9c9	36	77a06a7f-58b2-464c-8916-653edd8d6788												https://www.snowcatcloud.com/	SnowcatCloud: Hosted Snowplow Analytics		https	www.snowcatcloud.com	443	/fingerprint												medium	source	term	content	campaign	{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.fingerprintjs/fingerprint/jsonschema/1-0-0","data":{"visitorId":"nmnY3NEe0lGJc4tzh5KM","visitorFound":true,"requestId":"1647900762966.WcixoX","confidence":{"score":0.9986469217469743}}},{"schema":"iglu:com.clearbit/company/jsonschema/1-0-0","data":{"name":"SnowcatCloud","domain":"snowcatcloud.com","geo.country":"United States","geo.state":"California","geo.city":"San Diego","metrics.annualRevenue":null,"metrics.employees":5,"tags":["Internet"],"tech":["google_apps","cloud_flare","zendesk","sendgrid","heroku","mandrill","amazon_ses","google_tag_manager","customer_io","facebook_advertiser","snowplow_analytics","google_analytics"],"industry":"Internet Software & Services","sector":"Information Technology","industryGroup":"Software & Services","site.url":"snowcatcloud.com"}},{"schema":"iglu:com.snowcatcloud.iceberg/group/jsonschema/1-0-0","data":{"id":"snowcatcloud.com","name":"SnowcatCloud","domain":"snowcatcloud.com"}},{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"9689656e-ebab-4c10-9413-59a6dcefadd2"}},{"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"navigationStart":1647900761735,"unloadEventStart":1647900761905,"unloadEventEnd":1647900761905,"redirectStart":0,"redirectEnd":0,"fetchStart":1647900761737,"domainLookupStart":1647900761737,"domainLookupEnd":1647900761737,"connectStart":1647900761737,"connectEnd":1647900761737,"secureConnectionStart":0,"requestStart":1647900761737,"responseStart":1647900761803,"responseEnd":1647900761952,"domLoading":1647900761911,"domInteractive":1647900762058,"domContentLoadedEventStart":1647900762255,"domContentLoadedEventEnd":1647900762255,"domComplete":0,"loadEventStart":0,"loadEventEnd":0}}]}																									Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36						en-US	1	0	0	0	0	0	0	0	0	1	24	2135	380				America/Los_Angeles			3440	1440	UTF-8	2135	6344												2022-03-21 22:12:43.139			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"99","useragentMinor":"0","useragentPatch":"4844","useragentVersion":"Chrome 99.0.4844","osFamily":"Mac OS X","osMajor":"10","osMinor":"15","osPatch":"7","osPatchMinor":null,"osVersion":"Mac OS X 10.15.7","deviceFamily":"Other"}},{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-2","data":{"deviceBrand":"Apple","deviceName":"Apple Macintosh","operatingSystemVersionMajor":"10","layoutEngineNameVersion":"Blink 99.0","operatingSystemNameVersion":"Mac OS X 10.15.7","layoutEngineNameVersionMajor":"Blink 99","operatingSystemName":"Mac OS X","agentVersionMajor":"99","layoutEngineVersionMajor":"99","deviceClass":"Desktop","agentNameVersionMajor":"Chrome 99","operatingSystemNameVersionMajor":"Mac OS X 10","deviceCpuBits":"32","operatingSystemClass":"Desktop","layoutEngineName":"Blink","agentName":"Chrome","agentVersion":"99.0.4844.74","layoutEngineClass":"Browser","agentNameVersion":"Chrome 99.0.4844.74","operatingSystemVersion":"10.15.7","deviceCpu":"Intel","agentClass":"Browser","layoutEngineVersion":"99.0"}},{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"_gaexp","value":"gaexpvalue"}},{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"ajs_user_id","value":"ajsuseridvalue"}},{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"cart","value":"cartvalueexample"}},{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"ajs_anonymous_id","value":"ajs_anonymous_idvalue"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"X-Forwarded-For","value":"75.80.110.186"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"Host","value":"sp.snowcatcloud.com"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"User-Agent","value":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"Origin","value":"https://www.snowcatcloud.com"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"Referer","value":"https://www.snowcatcloud.com/"}},{"schema":"iglu:com.dbip/location/jsonschema/1-0-0","data":{"city":{"geoname_id":5342353,"names":{"en":"Del Mar","fa":"دل مار، کالیفرنیا","ja":"デル・マー","zh-CN":"德尔马"}},"continent":{"code":"NA","geoname_id":6255149,"names":{"de":"Nordamerika","en":"North America","es":"Norteamérica","fa":" امریکای شمالی","fr":"Amérique Du Nord","ja":"北アメリカ大陸","ko":"북아메리카","pt-BR":"América Do Norte","ru":"Северная Америка","zh-CN":"北美洲"}},"country":{"geoname_id":6252001,"is_in_european_union":false,"iso_code":"US","names":{"de":"Vereinigte Staaten von Amerika","en":"United States","es":"Estados Unidos de América (los)","fa":"ایالات متحدهٔ امریکا","fr":"États-Unis","ja":"アメリカ合衆国","ko":"미국","pt-BR":"Estados Unidos","ru":"США","zh-CN":"美国"}},"location":{"latitude":32.9595,"longitude":-117.265,"time_zone":"America/Los_Angeles","weather_code":"USCA0288"},"postal":{"code":"92014"},"subdivisions":[{"geoname_id":5332921,"iso_code":"CA","names":{"de":"Kalifornien","en":"California","es":"California","fa":"کالیفرنیا","fr":"Californie","ja":"カリフォルニア州","ko":"캘리포니아 주","pt-BR":"Califórnia","ru":"Калифорния","zh-CN":"加州"}},{"geoname_id":5391832,"names":{"en":"San Diego","es":"Condado de San Diego","fa":"شهرستان سن دیگو، کالیفرنیا","fr":"Comté de San Diego","ja":"サンディエゴ郡","ko":"샌디에이고 군","pt-BR":"Condado de San Diego","ru":"Сан-Диего","zh-CN":"圣迭戈县"}}]}},{"schema":"iglu:com.dbip/isp/jsonschema/1-0-0","data":{"traits":{"autonomous_system_number":20001,"autonomous_system_organization":"Charter Communications Inc","connection_type":"Corporate","isp":"Charter Communications","organization":"Spectrum"}}}]}	76255390-7115-4aa0-8a23-a1b1d4ef8d7c	2022-03-21 22:12:43.226	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	eca23f7cfc0c763e96ce4de2bb7ad273	`

// TestProcessPageViewJSON tests that a real page_view TSV is converted to flattened JSON
func TestProcessPageViewJSON(t *testing.T) {
	mgr := mock.NewManager()

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
		log:             mgr.Logger(),
	}

	msg := message.NewPart([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Should process one message")

	// Parse JSON output
	var jsonOutput map[string]interface{}
	err = json.Unmarshal(msgs[0].AsBytes(), &jsonOutput)
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
	mgr := mock.NewManager()

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
		log:            mgr.Logger(),
	}

	msg := message.NewPart([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because IP matches filter")

	t.Logf("✅ Event with IP 75.80.110.186 was correctly dropped")
}

// TestProcessPageViewTSV_FilterBySchemaProperty tests filtering by schema property value
func TestProcessPageViewTSV_FilterBySchemaProperty(t *testing.T) {
	mgr := mock.NewManager()

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
		log:            mgr.Logger(),
	}

	msg := message.NewPart([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because useragentFamily is Chrome")

	t.Logf("✅ Event with useragentFamily=Chrome was correctly dropped")
}

// TestProcessPageViewTSV_FilterBySchemaProperty_NoMatch tests that events without matching property values are not dropped
func TestProcessPageViewTSV_FilterBySchemaProperty_NoMatch(t *testing.T) {
	mgr := mock.NewManager()

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
		log:            mgr.Logger(),
	}

	msg := message.NewPart([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Event should NOT be dropped because useragentFamily is not Firefox")
	assert.Equal(t, testPageViewTSV, string(msgs[0].AsBytes()))

	t.Logf("✅ Event with useragentFamily=Chrome (not Firefox) was correctly kept")
}

// TestProcessPageViewTSV_FilterBySchemaProperty_osFamily tests filtering by osFamily property
func TestProcessPageViewTSV_FilterBySchemaProperty_osFamily(t *testing.T) {
	mgr := mock.NewManager()

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
		log:            mgr.Logger(),
	}

	msg := message.NewPart([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because osFamily is Mac OS X")

	t.Logf("✅ Event with osFamily='Mac OS X' was correctly dropped")
}

// TestProcessPageViewTSV_FilterCombinedRegularAndSchemaProperty tests combining regular field and schema property filters
func TestProcessPageViewTSV_FilterCombinedRegularAndSchemaProperty(t *testing.T) {
	mgr := mock.NewManager()

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
		log:            mgr.Logger(),
	}

	msg := message.NewPart([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because useragentFamily contains Chrome")

	t.Logf("✅ Event with useragentFamily containing 'Chrome' was correctly dropped (combined filter test)")
}

// TestProcessPageViewTSV_FilterMultipleConditions tests multiple filters with OR logic
func TestProcessPageViewTSV_FilterMultipleConditions(t *testing.T) {
	mgr := mock.NewManager()

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
		log:            mgr.Logger(),
	}

	msg := message.NewPart([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	assert.Nil(t, msgs, "Event should be dropped because osFamily matches 'Mac OS X' (OR logic)")

	t.Logf("✅ Event correctly dropped with multiple filters using OR logic (osFamily='Mac OS X' matched)")
}

// TestProcessPageViewTSV_FilterMultipleConditions_NoMatch tests multiple filters where none match
func TestProcessPageViewTSV_FilterMultipleConditions_NoMatch(t *testing.T) {
	mgr := mock.NewManager()

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
		log:            mgr.Logger(),
	}

	msg := message.NewPart([]byte(testPageViewTSV))
	msgs, err := proc.Process(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, msgs, 1, "Event should NOT be dropped because none of the filters match")
	assert.Equal(t, testPageViewTSV, string(msgs[0].AsBytes()))

	t.Logf("✅ Event correctly kept when multiple filters don't match (Chrome on Mac OS X, Desktop)")
}
