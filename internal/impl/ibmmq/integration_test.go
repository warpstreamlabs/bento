//go:build x_bento_extra

package ibmmq

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func ibmMQTestQueueName(id string) string {
	id = strings.ToUpper(strings.ReplaceAll(id, "-", ""))
	if len(id) > 32 {
		id = id[:32]
	}
	return "BENTO.TEST." + id
}

func ibmMQRunMQSC(tb testing.TB, resource dockertest.Resource, command string) {
	tb.Helper()

	result, err := resource.Exec(tb.Context(), []string{
		"bash",
		"-lc",
		fmt.Sprintf("printf '%%s\\n' %q | runmqsc QM1", command),
	})
	require.NoError(tb, err, "runmqsc stdout=%s stderr=%s", result.StdOut, result.StdErr)
	require.Equalf(tb, 0, result.ExitCode, "runmqsc exit code %d stdout=%s stderr=%s", result.ExitCode, result.StdOut, result.StdErr)
}

func ibmMQCreateTestQueue(tb testing.TB, resource dockertest.Resource, queueName string) {
	tb.Helper()

	ibmMQRunMQSC(tb, resource, fmt.Sprintf("DEFINE QLOCAL(%s) REPLACE", queueName))
	ibmMQRunMQSC(tb, resource, fmt.Sprintf("SET AUTHREC PROFILE(%s) OBJTYPE(QUEUE) PRINCIPAL('app') AUTHADD(BROWSE,GET,INQ,PUT)", queueName))
	ibmMQRunMQSC(tb, resource, fmt.Sprintf("CLEAR QLOCAL(%s)", queueName))
}

func ibmMQStreamTestOptions(resource dockertest.Resource) []integration.StreamTestOptFunc {
	return []integration.StreamTestOptFunc{
		integration.StreamTestOptPort(resource.GetPort("1414/tcp")),
		integration.StreamTestOptSleepAfterInput(300 * time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(300 * time.Millisecond),
		integration.StreamTestOptPreTest(func(tb testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
			queueName := ibmMQTestQueueName(vars.ID)
			ibmMQCreateTestQueue(tb, resource, queueName)
			vars.General["QUEUE"] = queueName
		}),
	}
}

func TestIntegrationIBMMQ(t *testing.T) {
	integration.CheckSkip(t)

	images := []string{"9.3.0.0-r1", "10.0.1.0-r1"}
	for _, tag := range images {
		t.Run(tag, func(t *testing.T) {
			ibmMQRunIntegrationSuite(t, tag)
		})
	}
}

func ibmMQRunIntegrationSuite(t *testing.T, imageTag string) {
	t.Helper()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(2*time.Minute))

	resource := pool.RunT(t, "icr.io/ibm-messaging/mq",
		dockertest.WithTag(imageTag),
		dockertest.WithEnv([]string{"LICENSE=accept", "MQ_QMGR_NAME=QM1", "MQ_APP_PASSWORD=passw0rd"}),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		queueManager, err := ibmMQConnect(ibmMQConnectionConfig{
			queueManager:   "QM1",
			channel:        "DEV.APP.SVRCONN",
			connectionName: fmt.Sprintf("localhost(%s)", resource.GetPort("1414/tcp")),
			authUser:       "app",
			authPassword:   "passw0rd",
		}, service.MockResources().Logger())
		if err != nil {
			return err
		}
		_ = queueManager.Disc()
		return nil
	}))

	template := `
output:
  ibm_mq:
    queue_manager: QM1
    channel: DEV.APP.SVRCONN
    connection_name: localhost($PORT)
    queue: $QUEUE
    auth:
      user: app
      password: passw0rd
input:
  ibm_mq:
    queue_manager: QM1
    channel: DEV.APP.SVRCONN
    connection_name: localhost($PORT)
    queue: $QUEUE
    auth:
      user: app
      password: passw0rd
`
	streamOptions := ibmMQStreamTestOptions(resource)

	for _, streamSuite := range []struct {
		name string
		test integration.StreamTestDefinition
	}{
		{name: "open_close_suite", test: integration.StreamTestOpenClose()},
		{name: "send_batch_suite", test: integration.StreamTestSendBatch(10)},
		{name: "sequential_suite", test: integration.StreamTestStreamSequential(200)},
		{name: "parallel_suite", test: integration.StreamTestStreamParallel(1000)},
		{name: "parallel_lossy_suite", test: integration.StreamTestStreamParallelLossy(1000)},
		{name: "parallel_lossy_through_reconnect_suite", test: integration.StreamTestStreamParallelLossyThroughReconnect(1000)},
	} {
		t.Run(streamSuite.name, func(t *testing.T) {
			integration.StreamTests(streamSuite.test).Run(t, template, streamOptions...)
		})
	}

	t.Run("large_messages", func(t *testing.T) {
		ibmMQTestLargeMessages(t, resource)
	})
}

func ibmMQTestLargeMessages(t *testing.T, resource dockertest.Resource) {
	queueName := ibmMQTestQueueName("large-messages")
	ibmMQCreateTestQueue(t, resource, queueName)

	config := fmt.Sprintf(`
queue_manager: QM1
channel: DEV.APP.SVRCONN
connection_name: localhost(%s)
queue: %s
auth:
  user: app
  password: passw0rd
`, resource.GetPort("1414/tcp"), queueName)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	outputConfig, err := ibmMQOutputSpec().ParseYAML(config, nil)
	require.NoError(t, err)

	writer, err := ibmMQWriterFromParsed(outputConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, writer.Connect(ctx))
	defer func() {
		_ = writer.Close(context.Background())
	}()

	inputConfig, err := ibmMQInputSpec().ParseYAML(config, nil)
	require.NoError(t, err)

	reader, err := ibmMQReaderFromParsed(inputConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, reader.Connect(ctx))
	defer func() {
		_ = reader.Close(context.Background())
	}()

	// The first message is re-got by its message token and grows the buffer,
	// and the second is read straight into the grown buffer.
	payloads := [][]byte{
		bytes.Repeat([]byte("a"), 1024*1024),
		bytes.Repeat([]byte("b"), 512*1024),
	}

	for _, payload := range payloads {
		require.NoError(t, writer.Write(ctx, service.NewMessage(payload)))
	}

	var messages []*service.Message
	for range payloads {
		message, ackFunc, err := reader.Read(ctx)
		require.NoError(t, err)
		require.NoError(t, ackFunc(ctx, nil))
		messages = append(messages, message)
	}

	for i, payload := range payloads {
		received, err := messages[i].AsBytes()
		require.NoError(t, err)
		require.True(t, bytes.Equal(payload, received), "message %d differs from what was sent", i)
	}
}
