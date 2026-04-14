package confluent

import (
	"encoding/json"
	"testing"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateNamespaces(t *testing.T) {
	tests := map[string]struct {
		testSchema    string
		cleanedSchema string
	}{
		// test we are not altering "references"
		"schemaWithDodgyNamespaceNamesWithReferences": {
			testSchema: `{
	"schema": {
		"type": "record",
		"name": "User",
		"namespace": "com.example-dodgy",
		"fields": [
			{
				"name": "name",
				"type": "string"
			},
			{
				"name": "email",
				"type": "string"
			},
			{
				"name": "address",
				"type": "Address-dodgy"
			}
		]
	},
	"references": {
		"namespace": "com.example-dodgy",
		"name": "Address-dodgy",
		"subject": "Address-dodgy",
		"version": 1
	}
}`,
			cleanedSchema: `{
	"schema": {
		"type": "record",
		"name": "User",
		"namespace": "com.exampledodgy",
		"fields": [
			{
				"name": "name",
				"type": "string"
			},
			{
				"name": "email",
				"type": "string"
			},
			{
				"name": "address",
				"type": "Address-dodgy"
			}
		]
	},
	"references": {
		"namespace": "com.example-dodgy",
		"name": "Address-dodgy",
		"subject": "Address-dodgy",
		"version": 1
	}
}`,
		},
		// Test we are only fixing "namespaces" not names
		"dodgyNameSpaceAndDodgyName": {
			testSchema: `{
	"schema": {
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{
				"name": "com.example-dodgy.dodgy-name",
				"type": "string"
			},
			{	
				"namespace": "com.example-dodgy",
				"name": "age",
				"type": "int"
			}
		]
	}
}`,
			cleanedSchema: `{
	"schema": {
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{
				"name": "com.exampledodgy.dodgy-name",
				"type": "string"
			},
			{	
				"namespace": "com.exampledodgy",
				"name": "age",
				"type": "int"
			}
		]
	}
}`,
		},
		// test nested schemas
		"nestedSchemas": {
			testSchema: `{
    "name": "person",
    "type": "record",
    "fields": [
        {
			"name": "firstname",
			"type": "string"
		},
        {
			"name": "lastname",
			"type": "string"
		},
        {
            "name": "address",
            "type": {
				"type" : "record",
				"name" : "AddressUSRecord",
				"fields" : [
					{
						"namespace": "com.example-dodgy",
						"name": "streetaddress",
						"type": "string"
					},
					{
						"name": "com.example-dodgy.city",
						"type": "string"
					}
				]
			}
        }
    ]
}`,
			cleanedSchema: `{
    "name": "person",
    "type": "record",
    "fields": [
        {
			"name": "firstname",
			"type": "string"
		},
        {
			"name": "lastname",
			"type": "string"
		},
        {
            "name": "address",
            "type": {
				"type" : "record",
				"name" : "AddressUSRecord",
				"fields" : [
					{
						"namespace": "com.exampledodgy",
						"name": "streetaddress",
						"type": "string"
					},
					{
						"name": "com.exampledodgy.city",
						"type": "string"
					}
				]
			}
        }
    ]
}`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var res map[string]any
			err := json.Unmarshal([]byte(test.testSchema), &res)
			require.NoError(t, err)

			updateNamespaces(res)

			cleaned, err := json.Marshal(res)
			require.NoError(t, err)

			jdopts := jsondiff.DefaultJSONOptions()
			diff, explanation := jsondiff.Compare(cleaned, []byte(test.cleanedSchema), &jdopts)
			assert.Equalf(t, jsondiff.FullMatch.String(), diff.String(), "%s: %s", name, explanation)
		})
	}
}
