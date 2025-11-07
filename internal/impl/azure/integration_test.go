package azure

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/gofrs/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/bloblang"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationAzure(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		// Expose blob, queue and table service ports
		ExposedPorts: []string{"10000/tcp", "10001/tcp", "10002/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	connString := getEmulatorConnectionString(resource.GetPort("10000/tcp"), resource.GetPort("10001/tcp"), resource.GetPort("10002/tcp"))

	// Wait for Azurite to start up
	err = pool.Retry(func() error {
		client, err := azblob.NewClientFromConnectionString(connString, nil)
		if err != nil {
			return err
		}

		ctx, done := context.WithTimeout(context.Background(), 1*time.Second)
		defer done()

		if _, err = client.NewListContainersPager(nil).NextPage(ctx); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err, "Failed to start Azurite")

	dummyContainer := "jotunheim"
	dummyPrefix := "kvenn"
	t.Run("blob_storage", func(t *testing.T) {
		template := `
output:
  azure_blob_storage:
    blob_type: BLOCK
    container: $VAR1-$ID
    max_in_flight: 1
    path: $VAR2/${!count("$ID")}.txt
    public_access_level: PRIVATE
    storage_connection_string: $VAR3

input:
  azure_blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2
    storage_connection_string: $VAR3
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarSet("VAR1", dummyContainer),
			integration.StreamTestOptVarSet("VAR2", dummyPrefix),
			integration.StreamTestOptVarSet("VAR3", connString),
		)
	})

	t.Run("blob_storage_streamed", func(t *testing.T) {
		template := `
output:
  azure_blob_storage:
    blob_type: BLOCK
    container: $VAR1-$ID
    max_in_flight: 1
    path: $VAR2/${!count("$ID")}.txt
    public_access_level: PRIVATE
    storage_connection_string: $VAR3

input:
  azure_blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2
    storage_connection_string: $VAR3
    targets_input:
      azure_blob_storage:
        container: $VAR1-$ID
        prefix: $VAR2
        storage_connection_string: $VAR3
      processors:
        - mapping: 'root.name = @blob_storage_key'
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarSet("VAR1", dummyContainer),
			integration.StreamTestOptVarSet("VAR2", dummyPrefix),
			integration.StreamTestOptVarSet("VAR3", connString),
		)
	})

	t.Run("blob_storage_append", func(t *testing.T) {
		template := `
output:
  broker:
    pattern: fan_out_sequential
    outputs:
      - azure_blob_storage:
          blob_type: APPEND
          container: $VAR1-$ID
          max_in_flight: 1
          path: $VAR2/data.txt
          public_access_level: PRIVATE
          storage_connection_string: $VAR3
      - azure_blob_storage:
          blob_type: APPEND
          container: $VAR1-$ID
          max_in_flight: 1
          path: $VAR2/data.txt
          public_access_level: PRIVATE
          storage_connection_string: $VAR3

input:
  azure_blob_storage:
    container: $VAR1-$ID
    prefix: $VAR2/data.txt
    storage_connection_string: $VAR3
  processors:
    - mapping: |
        root = if content() == "hello worldhello world" { "hello world" } else { "" }
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
		).Run(
			t, template,
			integration.StreamTestOptVarSet("VAR1", dummyContainer),
			integration.StreamTestOptVarSet("VAR2", dummyPrefix),
			integration.StreamTestOptVarSet("VAR3", connString),
		)
	})

	os.Setenv("AZURITE_QUEUE_ENDPOINT_PORT", resource.GetPort("10001/tcp")) //nolint: tenv // this test runs in parallel
	dummyQueue := "foo"
	t.Run("queue_storage", func(t *testing.T) {
		template := `
output:
  azure_queue_storage:
    queue_name: $VAR1$ID
    storage_connection_string: $VAR2

input:
  azure_queue_storage:
    queue_name: $VAR1$ID
    storage_connection_string: $VAR2
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptVarSet("VAR1", dummyQueue),
			integration.StreamTestOptVarSet("VAR2", "UseDevelopmentStorage=true;"),
		)
	})
}

func TestIntegrationCosmosDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator",
		Tag:        "latest",
		Env: []string{
			// The bigger the value, the longer it takes for the container to start up.
			"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=4",
			"AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false",
		},
		ExposedPorts: []string{"8081/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	// Start a HTTP -> HTTPS proxy server on a background goroutine to work around the self-signed certificate that the
	// CosmosDB container provides, because unfortunately, it doesn't expose a plain HTTP endpoint.
	// This listener will be owned and closed automatically by the HTTP server
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	srv := &http.Server{Handler: http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		url, err := url.Parse("https://localhost:" + resource.GetPort("8081/tcp"))
		require.NoError(t, err)

		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

		p := httputil.NewSingleHostReverseProxy(url)
		p.Transport = customTransport
		// Don't log proxy errors, but return an error downstream
		p.ErrorHandler = func(rw http.ResponseWriter, r *http.Request, err error) {
			rw.WriteHeader(http.StatusBadGateway)
		}

		p.ServeHTTP(res, req)
	})}
	go func() {
		require.ErrorIs(t, srv.Serve(listener), http.ErrServerClosed)
	}()
	t.Cleanup(func() {
		assert.NoError(t, srv.Close())
	})

	_, servicePort, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	err = pool.Retry(func() error {
		resp, err := http.Get("http://localhost:" + servicePort + "/_explorer/emulator.pem")
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to get emulator.pem, got status: %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if len(body) == 0 {
			return errors.New("failed to get emulator.pem")
		}

		return nil
	})
	require.NoError(t, err, "Failed to start CosmosDB emulator")

	emulatorKey := "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	dummyDatabase := "Asgard"
	dummyContainer := "Valhalla"
	dummyPartitionKeyField := "Ifing"
	dummyPartitionKeyValue := "Jotunheim"

	dbSetup := func(t testing.TB, ctx context.Context, databaseID string) {
		t.Helper()

		cred, err := azcosmos.NewKeyCredential(emulatorKey)
		require.NoError(t, err)

		client, err := azcosmos.NewClientWithKey("http://localhost:"+servicePort, cred, nil)
		require.NoError(t, err)

		_, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{
			ID: databaseID,
		}, nil)
		require.NoError(t, err)

		db, err := client.NewDatabase(databaseID)
		require.NoError(t, err)

		_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
			ID: dummyContainer,
			PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
				Paths: []string{"/" + dummyPartitionKeyField},
			},
		}, nil)
		require.NoError(t, err)
	}

	t.Run("cosmosdb output -> input roundtrip", func(t *testing.T) {
		template := `
output:
  azure_cosmosdb:
    endpoint: http://localhost:$PORT
    account_key: $VAR1
    database: $VAR2-$ID
    container: $VAR3
    partition_keys_map: root = "$VAR5"
    auto_id: true
    operation: Create
  processors:
    - mapping: |
        root.$VAR4 = "$VAR5"
        root.content = content().string()
        root.foo = "bar"

input:
  azure_cosmosdb:
    endpoint: http://localhost:$PORT
    account_key: $VAR1
    database: $VAR2-$ID
    container: $VAR3
    partition_keys_map: root = "$VAR5"
    query: |
      select * from $VAR3 as c where c.foo = @foo
    args_mapping: |
      root = [
        { "Name": "@foo", "Value": "bar" },
      ]
  processors:
    - mapping: |
        root = this.content
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptPort(servicePort),
			integration.StreamTestOptVarSet("VAR1", emulatorKey),
			integration.StreamTestOptVarSet("VAR2", dummyDatabase),
			integration.StreamTestOptVarSet("VAR3", dummyContainer),
			integration.StreamTestOptVarSet("VAR4", dummyPartitionKeyField),
			integration.StreamTestOptVarSet("VAR5", dummyPartitionKeyValue),
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				dbSetup(t, ctx, fmt.Sprintf("%s-%s", dummyDatabase, vars.ID))
			}),
		)
	})

	t.Run("cosmosdb processor", func(t *testing.T) {
		dummyUUID, err := uuid.NewV4()
		require.NoError(t, err)

		ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(done)

		database := fmt.Sprintf("%s-%s", dummyDatabase, dummyUUID)
		dbSetup(t, ctx, database)

		env := service.NewEnvironment()

		createConfig, err := cosmosDBProcessorConfig().ParseYAML(fmt.Sprintf(`
endpoint: http://localhost:%s
account_key: %s
database: %s
container: %s
partition_keys_map: root = "%s"
auto_id: false
operation: Create
`, servicePort, emulatorKey, database, dummyContainer, dummyPartitionKeyValue), env)
		require.NoError(t, err)

		readConfig, err := cosmosDBProcessorConfig().ParseYAML(fmt.Sprintf(`
endpoint: http://localhost:%s
account_key: %s
database: %s
container: %s
partition_keys_map: root = "%s"
item_id: ${! json("id") }
operation: Read
`, servicePort, emulatorKey, database, dummyContainer, dummyPartitionKeyValue), env)
		require.NoError(t, err)

		patchConfig, err := cosmosDBProcessorConfig().ParseYAML(fmt.Sprintf(`
endpoint: http://localhost:%s
account_key: %s
database: %s
container: %s
partition_keys_map: root = "%s"
operation: Patch
patch_condition: from c where not is_defined(c.blobfish)
patch_operations:
  - operation: Add
    path: /blobfish
    value_map: root = json("blobfish")
item_id: ${! json("id") }
enable_content_response_on_write: true
`, servicePort, emulatorKey, database, dummyContainer, dummyPartitionKeyValue), env)
		require.NoError(t, err)

		createProc, err := newCosmosDBProcessorFromParsed(createConfig, service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(func() { createProc.Close(ctx) })

		readProc, err := newCosmosDBProcessorFromParsed(readConfig, service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(func() { readProc.Close(ctx) })

		patchProc, err := newCosmosDBProcessorFromParsed(patchConfig, service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(func() { patchProc.Close(ctx) })

		var insertBatch service.MessageBatch
		for i := 0; i < 10; i++ {
			insertBatch = append(insertBatch, service.NewMessage([]byte(
				fmt.Sprintf(`{
  "%s": "%s",
  "id": "%d",
  "foo": %d
}`, dummyPartitionKeyField, dummyPartitionKeyValue, i, i))),
			)
		}

		resBatches, err := createProc.ProcessBatch(ctx, insertBatch)
		require.NoError(t, err)
		require.Len(t, resBatches, 1)
		require.Len(t, resBatches[0], len(insertBatch))
		for _, m := range resBatches[0] {
			require.NoError(t, m.GetError())
		}

		var readBatch service.MessageBatch
		for i := 0; i < 10; i++ {
			readBatch = append(readBatch, service.NewMessage([]byte(
				fmt.Sprintf(`{"id": "%d"}`, i))),
			)
		}
		resBatches, err = readProc.ProcessBatch(ctx, readBatch)
		require.NoError(t, err)
		require.Len(t, resBatches, 1)
		require.Len(t, resBatches[0], len(readBatch))

		blobl, err := bloblang.GlobalEnvironment().Parse(fmt.Sprintf(`root = this.with("%s", "id", "foo")`, dummyPartitionKeyField))
		require.NoError(t, err)
		for idx, m := range resBatches[0] {
			m, err := m.BloblangMutate(blobl)
			require.NoError(t, err)
			require.NoError(t, m.GetError())

			data, err := m.AsBytes()
			require.NoError(t, err)

			// Check if partition key, string and int fields are returned correctly
			expected, err := json.Marshal(map[string]any{dummyPartitionKeyField: dummyPartitionKeyValue, "id": strconv.Itoa(idx), "foo": idx})
			require.NoError(t, err)
			assert.JSONEq(t, string(expected), string(data))

			// Ensure metadata fields are set
			activityID, ok := m.MetaGetMut("activity_id")
			assert.True(t, ok)
			assert.NotEmpty(t, activityID)
			requestCharge, ok := m.MetaGetMut("request_charge")
			assert.True(t, ok)
			assert.EqualValues(t, 1.0, requestCharge)
		}

		var patchBatch service.MessageBatch
		for i := 0; i < 10; i++ {
			patchBatch = append(patchBatch, service.NewMessage([]byte(
				fmt.Sprintf(`{"id": "%d", "blobfish": "are cool"}`, i))),
			)
		}
		resBatches, err = patchProc.ProcessBatch(ctx, patchBatch)
		require.NoError(t, err)
		require.Len(t, resBatches, 1)
		require.Len(t, resBatches[0], len(patchBatch))
		for _, m := range resBatches[0] {
			require.NoError(t, m.GetError())
			data, err := m.AsStructured()
			require.NoError(t, err)
			assert.Contains(t, data, "blobfish")
		}
	})
}

func TestIntegrationAzureServiceBus(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Set timeout to 5 minutes for containers to start (Microsoft SQL Server + Service Bus emulator)
	pool.MaxWait = 5 * time.Minute
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	dummyQueue := "bento-test-queue"

	// Create a Docker network for the containers to communicate
	networkUUID, err := uuid.NewV4()
	require.NoError(t, err)
	networkName := fmt.Sprintf("servicebus-test-network-%s", networkUUID.String())
	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{
		Name: networkName,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.Client.RemoveNetwork(networkName)
	})

	sqlPassword := "StrongP@ssw0rd!"
	sqlResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/mssql/server",
		Tag:        "2022-latest",
		Env: []string{
			"ACCEPT_EULA=Y",
			fmt.Sprintf("MSSQL_SA_PASSWORD=%s", sqlPassword),
		},
		NetworkID: network.ID,
		Hostname:  "sqlserver",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(sqlResource))
	})
	_ = sqlResource.Expire(900)

	// Create Config.json for the emulator with queue definition
	configJSON := fmt.Sprintf(`{
  "UserConfig": {
    "Logging": {
      "Type": "Console"
    },
    "Namespaces": [
      {
        "Name": "sbemulatorns",
        "Queues": [
          {
            "Name": "%s"
          }
        ]
      }
    ]
  }
}`, dummyQueue)

	// Write config to a temp file
	tmpDir := t.TempDir()
	configPath := fmt.Sprintf("%s/Config.json", tmpDir)
	err = os.WriteFile(configPath, []byte(configJSON), 0644)
	require.NoError(t, err)

	// Start Azure Service Bus emulator connected to Microsoft SQL Server
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-messaging/servicebus-emulator",
		Tag:        "latest",
		Env: []string{
			"ACCEPT_EULA=Y",
			"CONFIG_PATH=/ServiceBus_Emulator/ConfigFiles/Config.json",
			fmt.Sprintf("MSSQL_SA_PASSWORD=%s", sqlPassword),
			"SQL_SERVER=sqlserver",
		},
		Mounts: []string{
			fmt.Sprintf("%s:/ServiceBus_Emulator/ConfigFiles", tmpDir),
		},
		NetworkID:    network.ID,
		ExposedPorts: []string{"5672/tcp"}, // AMQP port
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	// Get the mapped AMQP port for host access
	amqpPort := resource.GetPort("5672/tcp")

	// Connection string with the mapped port
	connString := fmt.Sprintf("Endpoint=sb://127.0.0.1:%s;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;", amqpPort)

	// Helper to send test messages
	sendTestMessages := func(t testing.TB, ctx context.Context, queue string, messages []string) {
		t.Helper()

		client, err := azservicebus.NewClientFromConnectionString(connString, nil)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, client.Close(ctx))
		}()

		sender, err := client.NewSender(queue, nil)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, sender.Close(ctx))
		}()

		for _, msg := range messages {
			err = sender.SendMessage(ctx, &azservicebus.Message{
				Body: []byte(msg),
			}, nil)
			require.NoError(t, err)
		}
	}

	// Wait for Service Bus emulator to start
	err = pool.Retry(func() error {
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()

		client, err := azservicebus.NewClientFromConnectionString(connString, nil)
		if err != nil {
			return err
		}
		defer func() { _ = client.Close(ctx) }()

		// Try to create a sender for the actual queue to verify connectivity
		sender, err := client.NewSender(dummyQueue, nil)
		if err != nil {
			return err
		}
		defer func() { _ = sender.Close(ctx) }()

		// Try to send a test message to ensure the queue is ready
		testMsg := &azservicebus.Message{
			Body: []byte("emulator-ready-test"),
		}
		if err := sender.SendMessage(ctx, testMsg, nil); err != nil {
			return err
		}

		// Receive and complete the test message to clear it from the queue
		receiver, err := client.NewReceiverForQueue(dummyQueue, nil)
		if err != nil {
			return err
		}
		defer func() { _ = receiver.Close(ctx) }()

		messages, err := receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			return err
		}
		if len(messages) > 0 {
			_ = receiver.CompleteMessage(ctx, messages[0], nil)
		}

		return nil
	})
	require.NoError(t, err, "Failed to start Service Bus emulator")

	t.Run("service_bus_queue", func(t *testing.T) {
		template := fmt.Sprintf(`
connection_string: %s
queue: %s
max_in_flight: 10
`, connString, dummyQueue)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Send test messages
		testMessages := []string{
			"test message 1",
			"test message 2",
			"test message 3",
			"test message 4",
			"test message 5",
		}
		sendTestMessages(t, ctx, dummyQueue, testMessages)

		// Create and test input
		env := service.NewEnvironment()
		conf, err := sbqSpec().ParseYAML(template, env)
		require.NoError(t, err)

		config, err := sbqConfigFromParsed(conf)
		require.NoError(t, err)

		input, err := newAzureServiceBusQueueReader(config, service.MockResources())
		require.NoError(t, err)

		err = input.Connect(ctx)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, input.Close(context.Background()))
		})

		// Read and verify messages
		receivedCount := 0
		for i := 0; i < len(testMessages); i++ {
			msg, ackFn, err := input.Read(ctx)
			require.NoError(t, err)
			require.NotNil(t, msg)

			content, err := msg.AsBytes()
			require.NoError(t, err)
			assert.Contains(t, testMessages, string(content))

			// Verify metadata
			messageID, exists := msg.MetaGet("service_bus_message_id")
			assert.True(t, exists)
			assert.NotEmpty(t, messageID)

			sequenceNumber, exists := msg.MetaGet("service_bus_sequence_number")
			assert.True(t, exists)
			assert.NotEmpty(t, sequenceNumber)

			err = ackFn(ctx, nil)
			require.NoError(t, err)

			receivedCount++
		}

		assert.Equal(t, len(testMessages), receivedCount)
	})

	t.Run("service_bus_queue_auto_ack", func(t *testing.T) {
		template := fmt.Sprintf(`
connection_string: %s
queue: %s
max_in_flight: 10
auto_ack: true
`, connString, dummyQueue)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		testMessages := []string{"auto_ack_1", "auto_ack_2", "auto_ack_3"}
		sendTestMessages(t, ctx, dummyQueue, testMessages)

		env := service.NewEnvironment()
		conf, err := sbqSpec().ParseYAML(template, env)
		require.NoError(t, err)

		config, err := sbqConfigFromParsed(conf)
		require.NoError(t, err)

		input, err := newAzureServiceBusQueueReader(config, service.MockResources())
		require.NoError(t, err)

		err = input.Connect(ctx)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, input.Close(context.Background()))
		})

		receivedCount := 0
		for i := 0; i < len(testMessages); i++ {
			msg, ackFn, err := input.Read(ctx)
			require.NoError(t, err)
			require.NotNil(t, msg)

			// With auto_ack, this should be a no-op
			err = ackFn(ctx, nil)
			require.NoError(t, err)

			receivedCount++
		}

		assert.Equal(t, len(testMessages), receivedCount)
	})

	t.Run("service_bus_queue_metadata", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Send message with custom properties
		client, err := azservicebus.NewClientFromConnectionString(connString, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, client.Close(context.Background()))
		})

		sender, err := client.NewSender(dummyQueue, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, sender.Close(context.Background()))
		})

		messageID := "test-msg-123"
		contentType := "application/json"
		correlationID := "corr-456"

		err = sender.SendMessage(ctx, &azservicebus.Message{
			Body:          []byte(`{"test": "metadata"}`),
			MessageID:     &messageID,
			ContentType:   &contentType,
			CorrelationID: &correlationID,
		}, nil)
		require.NoError(t, err)

		// Configure and read message
		template := fmt.Sprintf(`
connection_string: %s
queue: %s
max_in_flight: 1
`, connString, dummyQueue)

		env := service.NewEnvironment()
		conf, err := sbqSpec().ParseYAML(template, env)
		require.NoError(t, err)

		config, err := sbqConfigFromParsed(conf)
		require.NoError(t, err)

		input, err := newAzureServiceBusQueueReader(config, service.MockResources())
		require.NoError(t, err)

		err = input.Connect(ctx)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, input.Close(context.Background()))
		})

		msg, ackFn, err := input.Read(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Verify all metadata fields
		msgID, exists := msg.MetaGet("service_bus_message_id")
		assert.True(t, exists)
		assert.Equal(t, messageID, msgID)

		ct, exists := msg.MetaGet("service_bus_content_type")
		assert.True(t, exists)
		assert.Equal(t, contentType, ct)

		cid, exists := msg.MetaGet("service_bus_correlation_id")
		assert.True(t, exists)
		assert.Equal(t, correlationID, cid)

		lockToken, exists := msg.MetaGet("service_bus_lock_token")
		assert.True(t, exists)
		assert.NotEmpty(t, lockToken)

		err = ackFn(ctx, nil)
		require.NoError(t, err)
	})
}
