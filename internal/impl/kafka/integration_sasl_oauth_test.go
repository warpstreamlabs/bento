package kafka_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/oauth"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func createKafkaTopicSaslOauthConn(ctx context.Context, address, id string, partitions int32) error {
	token, err := unsecuredToken("test-client", 30*time.Minute)
	if err != nil {
		return err
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(address),
		kgo.SASL(oauth.Auth{Token: token}.AsMechanism()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	createTopicsReq := kmsg.NewPtrCreateTopicsRequest()
	topicReq := kmsg.NewCreateTopicsRequestTopic()
	topicReq.NumPartitions = partitions
	topicReq.Topic = fmt.Sprintf("topic-%v", id)
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
	brokerAddr := "localhost:" + kafkaPortStr

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	_ = pool.RunT(t, "apache/kafka-native",
		// TODO: Currently only fixed in 4.4.0-rc1, so replace when officially released.
		// See https://issues.apache.org/jira/browse/KAFKA-19583
		dockertest.WithTag("4.4.0-rc1"),
		dockertest.WithHostname("kafka"),
		dockertest.WithPortBindings(dockernetwork.PortMap{
			dockernetwork.MustParsePort("9092/tcp"): {{HostPort: kafkaPortStr}},
		}),
		dockertest.WithEnv([]string{
			"KAFKA_NODE_ID=1",
			"KAFKA_PROCESS_ROLES=broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_LISTENERS=BROKER://0.0.0.0:9092,CONTROLLER://localhost:9093",
			"KAFKA_ADVERTISED_LISTENERS=BROKER://" + brokerAddr,
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=BROKER:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME=BROKER",
			"KAFKA_SASL_ENABLED_MECHANISMS=OAUTHBEARER",
			"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=OAUTHBEARER",
			"KAFKA_LISTENER_NAME_BROKER_OAUTHBEARER_SASL_JAAS_CONFIG=" +
				`org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub="kafka-broker";`,
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		}),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		return createKafkaTopicSaslOauthConn(t.Context(), brokerAddr, "sasloauth", 1)
	}))

	oAuthMockServer := StartMockOAuthServer(t)
	tokenURL := oAuthMockServer.URL + "/oauth2/token"

	for _, tc := range []struct {
		name     string
		template string
	}{
		{
			name: "kafka franz",
			template: fmt.Sprintf(`
output:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    sasl:
      - mechanism: OAUTHBEARER
        oauth2:
          enabled: true
          client_key: foo
          client_secret: bar
          token_url: %[1]s
input:
  kafka_franz:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID ]
    consumer_group: consumer-group-$ID
    sasl:
      - mechanism: OAUTHBEARER
        oauth2:
          enabled: true
          client_key: foo
          client_secret: bar
          token_url: %[1]s
`, tokenURL),
		},
		{
			name: "kafka sarama",
			template: fmt.Sprintf(`
output:
  kafka:
    addresses: [ localhost:$PORT ]
    topic: topic-$ID
    sasl:
      mechanism: OAUTHBEARER
      oauth2:
        enabled: true
        client_key: foo
        client_secret: bar
        token_url: %[1]s
input:
  kafka:
    addresses: [ localhost:$PORT ]
    topics: [ topic-$ID ]
    consumer_group: consumer-group-$ID
    sasl:
      mechanism: OAUTHBEARER
      oauth2:
        enabled: true
        client_key: foo
        client_secret: bar
        token_url: %[1]s
`, tokenURL),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			integration.StreamTests(
				integration.StreamTestSendBatch(10),
			).Run(
				t, tc.template,
				integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
					require.NoError(t, createKafkaTopicSaslOauthConn(ctx, brokerAddr, vars.ID, 1))
				}),
				integration.StreamTestOptPort(kafkaPortStr),
			)
		})
	}
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
