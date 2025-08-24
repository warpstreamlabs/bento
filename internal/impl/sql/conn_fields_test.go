package sql_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	baws "github.com/warpstreamlabs/bento/internal/impl/sql/aws"

	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
	_ "github.com/warpstreamlabs/bento/public/components/sql"
)

func TestConnSettingsInitStmt(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	tmpDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	outputConf := fmt.Sprintf(`
sql_insert:
  driver: sqlite
  dsn: file:%v/foo.db
  table: things
  columns: [ foo, bar, baz ]
  args_mapping: 'root = [ this.foo, this.bar, this.baz ]'
  init_statement: |
    CREATE TABLE IF NOT EXISTS things (
      foo varchar(50) not null,
      bar varchar(50) not null,
      baz varchar(50) not null,
      primary key (foo)
    ) WITHOUT ROWID;
`, tmpDir)

	streamInBuilder := service.NewStreamBuilder()
	require.NoError(t, streamInBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamInBuilder.AddOutputYAML(outputConf))

	inFn, err := streamInBuilder.AddBatchProducerFunc()
	require.NoError(t, err)

	streamIn, err := streamInBuilder.Build()
	require.NoError(t, err)

	go func() {
		assert.NoError(t, streamIn.Run(tCtx))
	}()

	require.NoError(t, inFn(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"first","bar":"first bar","baz":"first baz"}`)),
		service.NewMessage([]byte(`{"foo":"second","bar":"second bar","baz":"second baz"}`)),
		service.NewMessage([]byte(`{"foo":"third","bar":"third bar","baz":"third baz"}`)),
	}))

	require.NoError(t, streamIn.Stop(tCtx))

	inputConf := fmt.Sprintf(`
sql_select:
  driver: sqlite
  dsn: file:%v/foo.db
  table: things
  columns: [ foo, bar, baz ]
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

	var msgs []string
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(ctx context.Context, m *service.Message) error {
		bMsg, err := m.AsBytes()
		require.NoError(t, err)
		msgs = append(msgs, string(bMsg))
		return nil
	}))
	require.NoError(t, err)

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	assert.NoError(t, streamOut.Run(tCtx))

	assert.Equal(t, []string{
		`{"bar":"first bar","baz":"first baz","foo":"first"}`,
		`{"bar":"second bar","baz":"second baz","foo":"second"}`,
		`{"bar":"third bar","baz":"third baz","foo":"third"}`,
	}, msgs)
}

func TestConnSettingsInitFiles(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	tmpDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "foo.sql"), []byte(`
CREATE TABLE IF NOT EXISTS things (
  foo varchar(50) not null,
  bar varchar(50) not null,
  primary key (foo)
) WITHOUT ROWID;
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "bar.sql"), []byte(`
ALTER TABLE things
ADD COLUMN baz varchar(50);
`), 0o644))

	outputConf := fmt.Sprintf(`
sql_insert:
  driver: sqlite
  dsn: file:%v/foo.db
  table: things
  columns: [ foo, bar, baz ]
  args_mapping: 'root = [ this.foo, this.bar, this.baz ]'
  init_files: [ "%v/foo.sql", "%v/bar.sql" ]
`, tmpDir, tmpDir, tmpDir)

	streamInBuilder := service.NewStreamBuilder()
	require.NoError(t, streamInBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamInBuilder.AddOutputYAML(outputConf))

	inFn, err := streamInBuilder.AddBatchProducerFunc()
	require.NoError(t, err)

	streamIn, err := streamInBuilder.Build()
	require.NoError(t, err)

	go func() {
		assert.NoError(t, streamIn.Run(tCtx))
	}()

	require.NoError(t, inFn(tCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"first","bar":"first bar","baz":"first baz"}`)),
		service.NewMessage([]byte(`{"foo":"second","bar":"second bar","baz":"second baz"}`)),
		service.NewMessage([]byte(`{"foo":"third","bar":"third bar","baz":"third baz"}`)),
	}))

	require.NoError(t, streamIn.Stop(tCtx))

	inputConf := fmt.Sprintf(`
sql_select:
  driver: sqlite
  dsn: file:%v/foo.db
  table: things
  columns: [ foo, bar, baz ]
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

	var msgs []string
	require.NoError(t, streamOutBuilder.AddConsumerFunc(func(ctx context.Context, m *service.Message) error {
		bMsg, err := m.AsBytes()
		require.NoError(t, err)
		msgs = append(msgs, string(bMsg))
		return nil
	}))
	require.NoError(t, err)

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	assert.NoError(t, streamOut.Run(tCtx))

	assert.Equal(t, []string{
		`{"bar":"first bar","baz":"first baz","foo":"first"}`,
		`{"bar":"second bar","baz":"second baz","foo":"second"}`,
		`{"bar":"third bar","baz":"third baz","foo":"third"}`,
	}, msgs)
}

func mockGetSecretFromAWS(secretName string) (secretString string, err error) {
	var secret map[string]interface{}
	switch secretName {
	case "validFullSecret":
		secret = map[string]interface{}{
			"username": "testUser",
			"password": "testPassword",
			"host":     "testHost",
			"port":     5432,
			"dbName":   "testDB",
		}
	case "validUserPassSecret":
		secret = map[string]interface{}{
			"username": "testUser",
			"password": "testPassword",
		}
	case "SecretDoesNotExist":
		return "", errors.New("ResourceNotFoundException: Secrets Manager can't find the specified secret.")
	}
	secretBytes, _ := json.Marshal(secret)
	return string(secretBytes), nil
}

func TestBuildAwsDsnFromSecret(t *testing.T) {
	tests := []struct {
		name          string
		dsn           string
		driver        string
		secretName    string
		expectedDSN   string
		expectedError bool
		errorValue    string
	}{
		{
			name:          "validFullSecretTest",
			dsn:           "postgres://user:password@host:5432/dbname?param1=value1&param2=value2",
			driver:        "postgres",
			secretName:    "validFullSecret",
			expectedDSN:   "postgres://testUser:testPassword@host:5432/dbname?param1=value1&param2=value2",
			expectedError: false,
		},
		{
			name:          "validUserPassSecretTest",
			dsn:           "postgres://user:password@host:5432/dbname?param1=value1&param2=value2",
			driver:        "postgres",
			secretName:    "validUserPassSecret",
			expectedDSN:   "postgres://testUser:testPassword@host:5432/dbname?param1=value1&param2=value2",
			expectedError: false,
		},
		{
			name:          "SecretNotFoundTest",
			dsn:           "postgres://user:password@host:5432/dbname?param1=value1&param2=value2",
			driver:        "postgres",
			secretName:    "SecretDoesNotExist",
			expectedDSN:   "postgres://testUser:testPassword@host:5432/dbname?param1=value1&param2=value2",
			expectedError: true,
			errorValue:    "error retrieving secret: ResourceNotFoundException: Secrets Manager can't find the specified secret.",
		},
		{
			name:          "DriverNotPostgresTest",
			dsn:           "mysql://root@localhost/username",
			driver:        "mysql",
			secretName:    "validFullSecret",
			expectedDSN:   "",
			expectedError: true,
			errorValue:    "secret_name with DSN info currently only works for postgres DSNs",
		},
		{
			name:          "NoSecretName",
			dsn:           "postgres://user:password@host:5432/dbname?param1=value1&param2=value2",
			driver:        "postgres",
			secretName:    "",
			expectedDSN:   "postgres://user:password@host:5432/dbname?param1=value1&param2=value2",
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := func() (string, error) {
				return mockGetSecretFromAWS(tt.secretName)
			}
			awsSecretDsn, err := baws.BuildAWSDsnFromSecret(tt.dsn, tt.driver, fn)
			if tt.expectedError {
				assert.Error(t, err)
				assert.Equal(t, tt.errorValue, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedDSN, awsSecretDsn)
			}
		})
	}
}

func mockGenerateIAMToken(dbEndpoint string, dbUser string) (string, error) {
	tokenMap := map[string]string{
		"mydb.cluster.region.rds.amazonaws.com:5432": "mock-postgres-token",
		"mydb.cluster.region.rds.amazonaws.com:3306": "mock-mysql-token",
	}

	if dbEndpoint == "error.endpoint:5432" {
		return "", fmt.Errorf("failed to generate IAM token")
	}

	if token, exists := tokenMap[dbEndpoint]; exists {
		return token, nil
	}

	return "default-mock-token", nil
}

func TestBuildAwsDsnFromIAM(t *testing.T) {
	tests := []struct {
		name          string
		dsn           string
		driver        string
		endpoint      string
		username      string
		expectedDSN   string
		expectedError bool
		errorValue    string
	}{
		{
			name:          "valid_postgres_iam",
			dsn:           "postgres://myuser:password@mydb.cluster.region.rds.amazonaws.com:5432/mydb?sslmode=require",
			driver:        "postgres",
			endpoint:      "mydb.cluster.region.rds.amazonaws.com:5432",
			username:      "myuser",
			expectedDSN:   "postgres://myuser:mock-postgres-token@mydb.cluster.region.rds.amazonaws.com:5432/mydb?sslmode=require",
			expectedError: false,
		},
		{
			name:          "valid_mysql_iam",
			dsn:           "mysql://admin@mydb.cluster.region.rds.amazonaws.com:3306/mydb?tls=true",
			driver:        "mysql",
			endpoint:      "mydb.cluster.region.rds.amazonaws.com:3306",
			username:      "admin",
			expectedDSN:   "mysql://admin:mock-mysql-token@mydb.cluster.region.rds.amazonaws.com:3306/mydb?tls=true",
			expectedError: false,
		},
		{
			name:          "invalid_driver",
			dsn:           "mongodb://user:pass@localhost:27017/db",
			driver:        "mongodb",
			expectedDSN:   "",
			expectedError: true,
			errorValue:    "cannot create DSN from IAM when driver is not postgres or mysql",
		},
		{
			name:          "iam_token_error",
			dsn:           "postgres://myuser:password@error.endpoint:5432/mydb",
			driver:        "postgres",
			username:      "myuser",
			expectedDSN:   "",
			expectedError: true,
			errorValue:    "error retrieving IAM token: failed to generate IAM token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			awsSecretDsn, err := baws.BuildAWSDsnFromIAMCredentials(tt.dsn, tt.driver, mockGenerateIAMToken)
			if tt.expectedError {
				assert.Error(t, err)
				assert.Equal(t, tt.errorValue, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedDSN, awsSecretDsn)
			}

			assert.Equal(t, tt.expectedDSN, awsSecretDsn)
		})
	}
}
