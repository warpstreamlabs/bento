package kafka_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func createKafkaTopicSaslOauthConn(ctx context.Context, address, id string, partitions int32) error {
	topicName := fmt.Sprintf("topic-%v", id)

	opts := []kgo.Opt{kgo.SeedBrokers(address)}
	var token string

	var err error
	token, err = unsecuredToken("test-client", 30*time.Minute)
	if err != nil {
		return err
	}

	opts = append(opts, kgo.SASL(oauth.Auth{
		Token: token,
	}.AsMechanism()))

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	createTopicsReq := kmsg.NewPtrCreateTopicsRequest()
	topicReq := kmsg.NewCreateTopicsRequestTopic()
	topicReq.NumPartitions = partitions
	topicReq.Topic = topicName
	topicReq.ReplicationFactor = 1
	createTopicsReq.Topics = append(createTopicsReq.Topics, topicReq)

	res, err := createTopicsReq.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	if len(res.Topics) != 1 {
		return fmt.Errorf("expected one topic in response, saw %d", len(res.Topics))
	}
	return kerr.ErrorForCode(res.Topics[0].ErrorCode)
}

func TestIntegrationKafkaOauth2(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	kafkaConfig := fmt.Sprintf(`
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=BROKER://0.0.0.0:9092,CONTROLLER://localhost:9093
advertised.listeners=BROKER://localhost:%s
listener.security.protocol.map=BROKER:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=BROKER
controller.listener.names=CONTROLLER

sasl.mechanism.inter.broker.protocol=OAUTHBEARER
sasl.enabled.mechanisms=OAUTHBEARER
sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler

offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
`, kafkaPortStr)

	jaasConfig := `KafkaServer {
  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
  unsecuredLoginStringClaim_sub="kafka-broker";
};
KafkaClient {
  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
  unsecuredLoginStringClaim_sub="kafka-broker";
};`

	// write kafka & jaas config to file to mount later
	tmpDir := t.TempDir()
	kafkaConfigPath := fmt.Sprintf("%s/server.properties", tmpDir)
	jaasConfigPath := fmt.Sprintf("%s/kafka_server_jaas.conf", tmpDir)

	err = os.WriteFile(kafkaConfigPath, []byte(kafkaConfig), 0644)
	require.NoError(t, err)

	err = os.WriteFile(jaasConfigPath, []byte(jaasConfig), 0644)
	require.NoError(t, err)

	// set up kafka container
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	options := &dockertest.RunOptions{
		Repository:   "apache/kafka",
		Tag:          "4.1.2",
		Hostname:     "kafka",
		ExposedPorts: []string{"9092"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr}},
		},
		Env: []string{"KAFKA_OPTS=-Djava.security.auth.login.config=/tmp/kafka_server_jaas.conf"},
		Mounts: []string{
			fmt.Sprintf("%s:/tmp", tmpDir),
		},
		Cmd: []string{
			"sh", "-c",
			`/opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /tmp/server.properties --ignore-formatted && exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties`,
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopicSaslOauthConn(context.Background(), "localhost:"+kafkaPortStr, "sasloauth", 1)
	}))

	oAuthMockServer := StartMockOAuthServer(t)

	t.Run("kafka franz", func(t *testing.T) {
		template := fmt.Sprintf(`
        output:
          kafka_franz:
            seed_brokers: [ localhost:$PORT ]
            topic: $ID
            sasl:
              - mechanism: OAUTHBEARER
                oauth2:
                  enabled: true
                  client_key: foo
                  client_secret: bar
                  token_url: %s/oauth2/token
        input:
          kafka_franz:
            seed_brokers: [ localhost:$PORT ]
            topics: [ $ID ]
            consumer_group: consumer-group-$ID
            sasl:
              - mechanism: OAUTHBEARER
                oauth2:
                  enabled: true
                  client_key: foo
                  client_secret: bar
                  token_url: %s/oauth2/token`, oAuthMockServer.URL, oAuthMockServer.URL)

		suite := integration.StreamTests(
			integration.StreamTestSendBatch(10),
		)

		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createKafkaTopicSaslOauthConn(context.Background(), "localhost:"+kafkaPortStr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
		)
	})

	t.Run("kafka sarama", func(t *testing.T) {
		template := fmt.Sprintf(`
        output:
          kafka:
            addresses: [ localhost:$PORT ]
            topic: $ID
            sasl:
              mechanism: OAUTHBEARER
              oauth2:
                enabled: true
                client_key: foo
                client_secret: bar
                token_url: %s/oauth2/token
        input:
          kafka:
            addresses: [ localhost:$PORT ]
            topics: [ $ID ]
            consumer_group: consumer-group-$ID
            sasl:
              mechanism: OAUTHBEARER
              oauth2:
                enabled: true
                client_key: foo
                client_secret: bar
                token_url: %s/oauth2/token`, oAuthMockServer.URL, oAuthMockServer.URL)

		suite := integration.StreamTests(
			integration.StreamTestSendBatch(10),
		)

		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createKafkaTopicSaslOauthConn(context.Background(), "localhost:"+kafkaPortStr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
		)
	})
}

//------------------------------------------------------------------------------

type MockOAuthServer struct {
	URL    string
	server *http.Server
}

func StartMockOAuthServer(t *testing.T) *MockOAuthServer {
	t.Helper()

	ms := &MockOAuthServer{}

	mux := http.NewServeMux()
	mux.HandleFunc("/oauth2/token", ms.handleToken)

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	port := ln.Addr().(*net.TCPAddr).Port
	ms.URL = fmt.Sprintf("http://localhost:%d", port)
	ms.server = &http.Server{Handler: mux}
	go func() {
		_ = ms.server.Serve(ln)
	}()
	t.Cleanup(func() {
		_ = ms.server.Close()
	})

	return ms
}

func (ms *MockOAuthServer) handleToken(w http.ResponseWriter, r *http.Request) {
	token, err := unsecuredToken("service-account", 30*time.Minute)
	if err != nil {
		http.Error(w, "token generation failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": token,
		"token_type":   "Bearer",
		"expires_in":   1800,
	})
}

func unsecuredToken(subject string, ttl time.Duration) (string, error) {
	now := time.Now()
	header, err := json.Marshal(map[string]string{"alg": "none"})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"sub": subject,
		"iat": now.Unix(),
		"exp": now.Add(ttl).Unix(),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims) + ".", nil
}
