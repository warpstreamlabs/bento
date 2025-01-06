package sftp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestSFTPInterpolation(t *testing.T) {
	rootMsg := service.NewMessage([]byte{})

	t.Setenv("addr", "TEST_ADDR")

	parsedConf, err := configSpec.ParseYAML(`    address: '${! "TEST_ADDR" }'
    user: '${! 1+1 }'
    pass: ${! "a"+"b"}
    insecureIgnoreHostKey: true
    knownHosts: /home/me/.ssh/known_hosts
    paths:
      - /.*.csv
      - ${! "ab"+"cd"}.csv
`, nil)
	assert.NoError(t, err)

	finalConf, err := buildConfig(parsedConf)
	assert.NoError(t, err)

	addr, err := finalConf.address.TryString(rootMsg)
	assert.NoError(t, err)

	user, err := finalConf.user.TryString(rootMsg)
	assert.NoError(t, err)

	pass, err := finalConf.pass.TryString(rootMsg)
	assert.NoError(t, err)

	knPath, err := finalConf.knownHostsPath.TryString(rootMsg)
	assert.NoError(t, err)

	assert.Equal(t, "TEST_ADDR", addr)
	assert.Equal(t, "2", user)
	assert.Equal(t, "ab", pass)

	assert.Len(t, finalConf.paths, 2)

	for i, v := range finalConf.paths {
		apath, err := v.TryString(rootMsg)

		assert.NoError(t, err)

		switch i {
		case 0:
			assert.Equal(t, "/.*.csv", apath)
		case 1:
			assert.Equal(t, "abcd.csv", apath)
		}
	}

	assert.True(t, finalConf.insecureIgnoreHostKey)

	assert.Equal(t, "/home/me/.ssh/known_hosts", knPath)
}

func TestFullScript(t *testing.T) {
	err := Setup()
	assert.NoError(t, err)

	for _, testCase := range []struct {
		script      string
		shouldError bool
	}{
		{
			script: `input:
  # All are interpolation capable
  sftp_v2:
    address: localhost:2022
    user: uw
    pass: ${! "ab" }
    paths:
      - ${! "ab" }

output:
  label: ""
  sftp_v2:
    address: ${! "asd" }
    user: ${! 1+1 }
    pass: ${! "ab" }
    paths:
      - /${! "asd" }`,
			shouldError: false,
		},
		{
			script: `input:
  # All are interpolation capable
  sftp_v2:
    address: localhost:2022
    user: uw
    paths:
      - ${! "ab" }

output:
  stdout: {}`,
			shouldError: false,
		},
		{
			script: `input:
  # All are interpolation capable
  sftp_v2:
    user: uw
    paths:
      - ${! "ab" }

output:
  stdout: {}`,
			shouldError: true,
		},
		{
			script: `input:
  # All are interpolation capable
  sftp_v2:
    address: localhost:2022
    paths:
      - ${! "ab" }

output:
  stdout: {}`,
			shouldError: true,
		}, {
			script: `input:
  # All are interpolation capable
  sftp_v2:
    address: localhost:2022
    user: uw
    paths: []

output:
  stdout: {}`,
			shouldError: false,
		}, {
			script: `input:
  # All are interpolation capable
  sftp_v2:
    address: localhost:2022
    user: uw

output:
  stdout: {}`,
			shouldError: false,
		},
	} {
		err := service.NewStreamBuilder().SetYAML(testCase.script)
		if testCase.shouldError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
