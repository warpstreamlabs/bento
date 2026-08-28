package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"

	_ "github.com/warpstreamlabs/bento/internal/impl/kafka"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func createKafkaTopicGSSAPI(ctx context.Context, address, krb5ConfPath, keytabPath, id string, partitions int32) error {
	topicName := fmt.Sprintf("topic-%v", id)

	krb5Conf, err := config.Load(krb5ConfPath)
	if err != nil {
		return fmt.Errorf("failed to load kerberos config: %w", err)
	}

	krbt, err := keytab.Load(keytabPath)
	if err != nil {
		return fmt.Errorf("failed to load keytab: %w", err)
	}

	krbClient := client.NewWithKeytab("kafkaclient", "EXAMPLE.COM", krbt, krb5Conf)
	if err := krbClient.Login(); err != nil {
		return fmt.Errorf("failed to login to kerberos: %w", err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(address),
		kgo.SASL(kerberos.Auth{Client: krbClient, Service: "kafka"}.AsMechanism()),
	)
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

func TestIntegrationKafkaGSSAPI(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	tmpDir := t.TempDir()
	keytabsDir := filepath.Join(tmpDir, "keytabs")
	require.NoError(t, os.MkdirAll(keytabsDir, 0755))

	krb5ConfContent := `[libdefaults]
    default_realm = EXAMPLE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false

[realms]
    EXAMPLE.COM = {
        kdc = localhost:88
        admin_server = localhost:749
    }

[domain_realm]
    .example.com = EXAMPLE.COM
    example.com = EXAMPLE.COM
`
	kdcConfContent := `[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    EXAMPLE.COM = {
        acl_file = /var/kerberos/krb5kdc/kadm5.acl
        supported_enctypes = aes256-cts:normal aes128-cts:normal
        max_renewable_life = 7d
    }
`
	kadmAclContent := "*/admin@EXAMPLE.COM *\n"
	kdcSetupScript := `#!/bin/bash
set -e

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y krb5-kdc krb5-admin-server krb5-user

mkdir -p /etc/krb5kdc
cp /testdata/kdc.conf /etc/krb5kdc/kdc.conf

mkdir -p /var/kerberos/krb5kdc
cp /testdata/kadm5.acl /var/kerberos/krb5kdc/kadm5.acl

cp /testdata/krb5.conf /etc/krb5.conf

kdb5_util create -s -P krbmaster123

krb5kdc
kadmind

	kadmin.local -q "addprinc -randkey kafka/localhost@EXAMPLE.COM"
	kadmin.local -q "addprinc -randkey kafka/127.0.0.1@EXAMPLE.COM"
	kadmin.local -q "addprinc -pw kafkaclient-secret kafkaclient@EXAMPLE.COM"

	mkdir -p /keytabs
	kadmin.local -q "ktadd -k /keytabs/kafka.keytab kafka/localhost@EXAMPLE.COM"
	kadmin.local -q "ktadd -k /keytabs/kafka.keytab kafka/127.0.0.1@EXAMPLE.COM"
	kadmin.local -q "ktadd -k /keytabs/client.keytab kafkaclient@EXAMPLE.COM"
	chmod 644 /keytabs/*.keytab

echo "KDC setup complete"
tail -f /dev/null
`

	krb5ConfPath := filepath.Join(tmpDir, "krb5.conf")
	kdcConfPath := filepath.Join(tmpDir, "kdc.conf")
	kadmAclPath := filepath.Join(tmpDir, "kadm5.acl")
	setupScriptPath := filepath.Join(tmpDir, "kdc-setup.sh")

	require.NoError(t, os.WriteFile(krb5ConfPath, []byte(krb5ConfContent), 0644))
	require.NoError(t, os.WriteFile(kdcConfPath, []byte(kdcConfContent), 0644))
	require.NoError(t, os.WriteFile(kadmAclPath, []byte(kadmAclContent), 0644))
	require.NoError(t, os.WriteFile(setupScriptPath, []byte(kdcSetupScript), 0755))

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kdcPort, err := integration.GetFreePort()
	require.NoError(t, err)
	kdcPortStr := strconv.Itoa(kdcPort)

	networkName := fmt.Sprintf("bento-krb5-%d", time.Now().UnixNano())
	network, err := pool.CreateNetwork(networkName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.RemoveNetwork(network)
	})

	kdcOptions := &dockertest.RunOptions{
		Repository:   "ubuntu",
		Tag:          "22.04",
		Hostname:     "kdc",
		ExposedPorts: []string{"88/tcp", "88/udp", "749/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"88/tcp": {{HostIP: "", HostPort: kdcPortStr}},
			"88/udp": {{HostIP: "", HostPort: kdcPortStr}},
		},
		Networks: []*dockertest.Network{network},
		Mounts: []string{
			fmt.Sprintf("%s:/testdata", tmpDir),
			fmt.Sprintf("%s:/keytabs", keytabsDir),
		},
		Cmd: []string{
			"bash", "-c",
			"/testdata/kdc-setup.sh",
		},
	}

	pool.MaxWait = 2 * time.Minute
	kdcResource, err := pool.RunWithOptions(kdcOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(kdcResource))
	})

	pool.MaxWait = 3 * time.Minute
	require.NoError(t, pool.Retry(func() error {
		if _, err := os.Stat(filepath.Join(keytabsDir, "client.keytab")); os.IsNotExist(err) {
			return fmt.Errorf("keytab not ready yet")
		}
		return nil
	}))

	clientKrb5ConfContent := fmt.Sprintf(`[libdefaults]
    default_realm = EXAMPLE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    udp_preference_limit = 1
    default_ccache_name = FILE:/tmp/krb5cc_%%{uid}

[realms]
    EXAMPLE.COM = {
        kdc = 127.0.0.1:%s
        admin_server = 127.0.0.1:%s
    }

[domain_realm]
    .example.com = EXAMPLE.COM
    example.com = EXAMPLE.COM
`, kdcPortStr, kdcPortStr)
	clientKrb5ConfPath := filepath.Join(tmpDir, "client-krb5.conf")
	require.NoError(t, os.WriteFile(clientKrb5ConfPath, []byte(clientKrb5ConfContent), 0644))

	kafkaKrb5ConfContent := `[libdefaults]
    default_realm = EXAMPLE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false

[realms]
    EXAMPLE.COM = {
        kdc = kdc:88
        admin_server = kdc:749
    }

[domain_realm]
    .example.com = EXAMPLE.COM
    example.com = EXAMPLE.COM
`

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)
	kafkaPortStr := strconv.Itoa(kafkaPort)

	principal := "kafka/127.0.0.1@EXAMPLE.COM"

	kafkaConfig := fmt.Sprintf(`process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=BROKER://0.0.0.0:9092,CONTROLLER://localhost:9093,INTERNAL://localhost:9094
advertised.listeners=BROKER://127.0.0.1:%s,INTERNAL://localhost:9094
listener.security.protocol.map=BROKER:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
sasl.enabled.mechanisms=GSSAPI
sasl.mechanism.inter.broker.protocol=GSSAPI
sasl.kerberos.service.name=kafka
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
`, kafkaPortStr)

	jaasConfig := fmt.Sprintf(`KafkaServer {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/keytabs/kafka.keytab"
    principal="%s";
};
`, principal)

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "server.properties"), []byte(kafkaConfig), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "kafka-krb5.conf"), []byte(kafkaKrb5ConfContent), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "kafka_server_jaas.conf"), []byte(jaasConfig), 0644))

	kafkaOptions := &dockertest.RunOptions{
		Repository:   "apache/kafka",
		Tag:          "4.1.2",
		ExposedPorts: []string{"9092"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr}},
		},
		Networks: []*dockertest.Network{network},
		Env: []string{
			"KAFKA_OPTS=-Djava.security.krb5.conf=/testclient/kafka-krb5.conf -Djava.security.auth.login.config=/testclient/kafka_server_jaas.conf",
		},
		Mounts: []string{
			fmt.Sprintf("%s:/testclient", tmpDir),
			fmt.Sprintf("%s:/keytabs", keytabsDir),
		},
		Cmd: []string{
			"sh", "-c",
			"/opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /testclient/server.properties --ignore-formatted && exec /opt/kafka/bin/kafka-server-start.sh /testclient/server.properties",
		},
	}

	pool.MaxWait = 2 * time.Minute
	kafkaResource, err := pool.RunWithOptions(kafkaOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(kafkaResource))
	})

	_ = kafkaResource.Expire(900)

	var lastErr error
	pool.MaxWait = 3 * time.Minute
	retryErr := pool.Retry(func() error {
		lastErr = createKafkaTopicGSSAPI(context.Background(), "127.0.0.1:"+kafkaPortStr, clientKrb5ConfPath, filepath.Join(keytabsDir, "client.keytab"), "testingconnection", 1)
		return lastErr
	})
	if retryErr != nil {
		var logBuf bytes.Buffer
		_ = pool.Client.Logs(docker.LogsOptions{
			Container:    kafkaResource.Container.ID,
			OutputStream: &logBuf,
			ErrorStream:  &logBuf,
			Stdout:       true,
			Stderr:       true,
			Timestamps:   false,
			Follow:       false,
		})
		t.Logf("Kafka container logs:\n%s", logBuf.String())
		require.NoError(t, retryErr)
	}

	clientKeytabPath := filepath.Join(keytabsDir, "client.keytab")

	template := fmt.Sprintf(`
output:
  kafka_franz:
    seed_brokers: [ 127.0.0.1:$PORT ]
    topic: topic-$ID
    sasl:
      - mechanism: GSSAPI
        kerberos_config_path: %s
        keytab_path: %s
        principal: kafkaclient
        realm: EXAMPLE.COM
        service_name: kafka

input:
  kafka_franz:
    seed_brokers: [ 127.0.0.1:$PORT ]
    topics: [ topic-$ID ]
    consumer_group: group-$ID
    start_from_oldest: true
    sasl:
      - mechanism: GSSAPI
        kerberos_config_path: %s
        keytab_path: %s
        principal: kafkaclient
        realm: EXAMPLE.COM
        service_name: kafka
`, clientKrb5ConfPath, clientKeytabPath, clientKrb5ConfPath, clientKeytabPath)

	suite := integration.StreamTests(
		integration.StreamTestSendBatch(10),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			require.NoError(t, createKafkaTopicGSSAPI(ctx, "127.0.0.1:"+kafkaPortStr, clientKrb5ConfPath, clientKeytabPath, vars.ID, 1))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
	)
}
