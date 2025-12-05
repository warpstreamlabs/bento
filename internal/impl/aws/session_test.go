package aws

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestBuildCredentialsCacheOptions(t *testing.T) {
	tests := []struct {
		name          string
		expiryWindow  string
		expectedValue time.Duration
	}{
		{
			name:          "valid value",
			expiryWindow:  "10s",
			expectedValue: 10 * time.Second,
		},
		{
			name:          "no value",
			expiryWindow:  "",
			expectedValue: 0,
		},
		{
			name:          "invalid value",
			expiryWindow:  "invalid",
			expectedValue: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := service.NewConfigSpec().
				Field(service.NewObjectField("credentials",
					service.NewStringField("expiry_window").Default(""),
				))

			yaml := "credentials:\n"
			if tt.expiryWindow != "" {
				yaml += "  expiry_window: " + tt.expiryWindow + "\n"
			}

			parsedConf, err := conf.ParseYAML(yaml, nil)
			if err != nil {
				t.Fatal(err)
			}

			credsConf := parsedConf.Namespace("credentials")
			credsCacheOpts := getCredentialsCacheOptions(credsConf)

			opts := &aws.CredentialsCacheOptions{}
			credsCacheOpts(opts)

			assert.Equal(t, tt.expectedValue, opts.ExpiryWindow)
		})
	}
}
