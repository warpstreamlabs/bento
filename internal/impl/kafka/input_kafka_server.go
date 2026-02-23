package kafka

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ksfFieldAddress           = "address"
	ksfFieldAdvertisedAddress = "advertised_address"
	ksfFieldTopics            = "topics"
	ksfFieldCertFile          = "cert_file"
	ksfFieldKeyFile           = "key_file"
	ksfFieldMTLSAuth          = "mtls_auth"
	ksfFieldMTLSCAsFiles      = "mtls_cas_files"
	ksfFieldSASL              = "sasl"
	ksfFieldTimeout           = "timeout"
	ksfFieldIdleTimeout       = "idle_timeout"
	ksfFieldMaxMessageBytes   = "max_message_bytes"
	ksfFieldIdempotentWrite   = "idempotent_write"
)

// Server configuration constants
const (
	// defaultMessageChanBuffer is the buffer size for the message channel
	defaultMessageChanBuffer = 10

	// shutdownGracePeriod is the maximum time to wait for connections to close during shutdown
	shutdownGracePeriod = 5 * time.Second

	// protocolOverheadBytes is the estimated overhead for Kafka protocol headers and metadata
	protocolOverheadBytes = 102400 // 100KB

	// requestSizeMultiplier is the multiplier applied to maxMessageBytes to account for protocol overhead
	requestSizeMultiplier = 2

	// maxClientIDLength is the maximum allowed client ID length to prevent DoS attacks
	maxClientIDLength = 10000
)

// saslServerField returns the SASL configuration field for the kafka_server input.
// This is different from the client-side saslField() as it only supports mechanisms
// that can be validated server-side (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
func saslServerField() *service.ConfigField {
	return service.NewObjectListField(ksfFieldSASL,
		service.NewStringAnnotatedEnumField("mechanism", map[string]string{
			"PLAIN":         "Plain text authentication. Credentials are sent in clear text, so TLS is recommended.",
			"SCRAM-SHA-256": "SCRAM based authentication as specified in RFC5802.",
			"SCRAM-SHA-512": "SCRAM based authentication as specified in RFC5802.",
		}).
			Description("The SASL mechanism to use for this credential."),
		service.NewStringField("username").
			Description("The username for authentication."),
		service.NewStringField("password").
			Description("The password for authentication.").
			Secret(),
	).
		Description("Configure one or more SASL credentials that clients can use to authenticate. When SASL is configured, clients must authenticate before sending produce requests. Multiple credentials can be configured for the same or different mechanisms.").
		Advanced().Optional().
		Example(
			[]any{
				map[string]any{
					"mechanism": "PLAIN",
					"username":  "user1",
					"password":  "password1",
				},
				map[string]any{
					"mechanism": "SCRAM-SHA-256",
					"username":  "user2",
					"password":  "password2",
				},
			},
		)
}

func kafkaServerInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("1.15.0").
		Beta().
		Categories("Services").
		Summary("Runs a Kafka-protocol-compatible server that accepts produce requests from Kafka producers.").
		Description(`
This input acts as a Kafka broker endpoint, allowing Kafka producers to send messages directly into a Bento pipeline without requiring a full Kafka cluster.

Similar to the `+"`http_server`"+` input, this creates a server that external clients can push data to. The difference is that clients use the Kafka protocol instead of HTTP.

### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- kafka_server_topic
- kafka_server_partition
- kafka_server_key
- kafka_server_timestamp
- kafka_server_timestamp_unix
- kafka_server_client_address
- kafka_server_offset
- kafka_server_tombstone_message
`+"```"+`

Message headers from Kafka records are also added as metadata fields.

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Field(service.NewStringField(ksfFieldAddress).
			Description("The address to listen on for Kafka protocol connections.").
			Default("0.0.0.0:9092")).
		Field(service.NewStringField(ksfFieldAdvertisedAddress).
			Description("The address to advertise to clients in metadata responses. If empty, the listen address is used. This is useful when the server is behind a NAT or load balancer, or when clients connect through a different network interface (e.g., Docker containers connecting via host.docker.internal).").
			Default("").
			Advanced()).
		Field(service.NewStringListField(ksfFieldTopics).
			Description("Optional list of topic names to accept. If empty, all topics are accepted.").
			Default([]string{}).
			Advanced()).
		Field(service.NewStringField(ksfFieldCertFile).
			Description("An optional server certificate file for enabling TLS.").
			Advanced().
			Default("")).
		Field(service.NewStringField(ksfFieldKeyFile).
			Description("An optional server key file for enabling TLS.").
			Advanced().
			Default("")).
		Field(service.NewStringAnnotatedEnumField(ksfFieldMTLSAuth, map[string]string{
			"none":               "Server will not request a client certificate",
			"request":            "Server will request a client certificate but doesn't require the client to send one",
			"require":            "Server will require any client certificate (doesn't verify it)",
			"verify_if_given":    "Server will request a client certificate and verify it if provided",
			"require_and_verify": "Server requires a client certificate and will verify it against the mtls_cas_files",
		}).
			Description("Sets the policy the server will follow for mTLS client authentication. Only used when TLS is enabled.").
			Default("").
			Advanced().
			Optional()).
		Field(service.NewStringListField(ksfFieldMTLSCAsFiles).
			Description("An optional list of paths to files containing client certificate authorities to use for verifying client certificates. Only used when mtls_auth is set to verify_if_given or require_and_verify.").
			Default([]string{}).
			Advanced().
			Optional()).
		Field(saslServerField()).
		Field(service.NewDurationField(ksfFieldTimeout).
			Description("The maximum time to wait for a produce request to be processed by the pipeline and acknowledged. This timeout also applies to write operations when sending responses back to clients. If processing takes longer than this duration, the producer receives a timeout error, but the message may still be delivered to the pipeline.").
			Default("5s").
			Advanced()).
		Field(service.NewDurationField(ksfFieldIdleTimeout).
			Description("The maximum time a connection can be idle (no data received) before being closed. Use a larger value for producers that send infrequently. Set to 0 to disable idle timeout.").
			Default("0").
			Advanced()).
		Field(service.NewIntField(ksfFieldMaxMessageBytes).
			Description("The maximum size in bytes of a message payload.").
			Default(1048576).
			Advanced()).
		Field(service.NewBoolField(ksfFieldIdempotentWrite).
			Description("Enable support for idempotent Kafka producers. When enabled, the server will respond to InitProducerID requests and allocate producer IDs. Note: This enables clients to use idempotent producers but does NOT provide actual exactly-once semantics - it only satisfies the protocol requirements.").
			Default(false).
			Advanced()).
		Example("Basic Usage", "Accept Kafka produce requests and write to stdout", `
input:
  kafka_server:
    address: "0.0.0.0:9092"
    topics:
      - events
      - logs

output:
  stdout: {}
`).
		Example("With TLS", "Accept Kafka produce requests over TLS", `
input:
  kafka_server:
    address: "0.0.0.0:9093"
    cert_file: /path/to/server-cert.pem
    key_file: /path/to/server-key.pem
`).
		Example("With mTLS", "Accept Kafka produce requests with mutual TLS authentication", `
input:
  kafka_server:
    address: "0.0.0.0:9093"
    cert_file: /path/to/server-cert.pem
    key_file: /path/to/server-key.pem
    mtls_auth: require_and_verify
    mtls_cas_files:
      - /path/to/client-ca.pem
`).
		Example("With mTLS (multiple CAs)", "Accept Kafka produce requests with multiple client certificate authorities", `
input:
  kafka_server:
    address: "0.0.0.0:9093"
    cert_file: /path/to/server-cert.pem
    key_file: /path/to/server-key.pem
    mtls_auth: require_and_verify
    mtls_cas_files:
      - /path/to/client-ca-1.pem
      - /path/to/client-ca-2.pem
`).
		Example("With mTLS (optional verification)", "Accept Kafka produce requests with optional client certificate verification", `
input:
  kafka_server:
    address: "0.0.0.0:9093"
    cert_file: /path/to/server-cert.pem
    key_file: /path/to/server-key.pem
    mtls_auth: verify_if_given
    mtls_cas_files:
      - /path/to/client-ca.pem
`).
		Example("With SASL PLAIN Authentication", "Accept authenticated Kafka produce requests using PLAIN", `
input:
  kafka_server:
    address: "0.0.0.0:9092"
    cert_file: /path/to/server-cert.pem
    key_file: /path/to/server-key.pem
    sasl:
      - mechanism: PLAIN
        username: producer1
        password: secret123
      - mechanism: PLAIN
        username: producer2
        password: secret456
`).
		Example("With SASL SCRAM Authentication", "Accept authenticated Kafka produce requests using SCRAM-SHA-256", `
input:
  kafka_server:
    address: "0.0.0.0:9092"
    cert_file: /path/to/server-cert.pem
    key_file: /path/to/server-key.pem
    sasl:
      - mechanism: SCRAM-SHA-256
        username: producer1
        password: secret123
      - mechanism: SCRAM-SHA-512
        username: producer2
        password: secret456
`)
}

func init() {
	err := service.RegisterBatchInput(
		"kafka_server", kafkaServerInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newKafkaServerInputFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type kafkaServerInput struct {
	address                 string
	advertisedAddress       string // Address to advertise to clients in metadata responses
	allowedTopics           map[string]struct{}
	tlsConfig               *tls.Config
	saslEnabled             bool
	saslMechanisms          []string         // List of enabled SASL mechanisms (in order)
	saslPlainCredentials    PlainCredentials // username -> password (for PLAIN)
	saslSCRAM256Credentials SCRAMCredentials // username -> credentials (for SCRAM-SHA-256)
	saslSCRAM512Credentials SCRAMCredentials // username -> credentials (for SCRAM-SHA-512)
	timeout                 time.Duration
	idleTimeout             time.Duration // Maximum idle time before closing connection (0 = disabled)
	maxMessageBytes         int
	idempotentWrite         bool // Enable idempotent producer support (allocates producer IDs)

	// Producer ID allocation for idempotent writes
	producerIDCounter atomic.Int64 // Counter for generating unique producer IDs

	listener   net.Listener
	shutdownCh chan struct{}
	logger     *service.Logger

	shutSig *shutdown.Signaller
	connWG  sync.WaitGroup

	// Synchronization for safe shutdown
	shutdownOnce sync.Once
	shutdownDone atomic.Bool
	msgChanValue atomic.Value // Stores chan messageBatch
	connectMu    sync.Mutex   // Protects Connect() from concurrent calls

	// Connection tracking for structured logging
	connCounter atomic.Uint64 // Generates unique connection IDs

	// Decompressor for processing compressed message batches
	decompressor kgo.Decompressor
}

type messageBatch struct {
	batch   service.MessageBatch
	ackFn   service.AckFunc
	resChan chan error
}

// createTopicID returns a deterministic UUID for a topic name.
// The UUID is derived from SHA-256 hash of the topic name (truncated to 16 bytes),
// ensuring consistent topic IDs across server restarts and multiple instances.
// This is used for Kafka protocol v10+ metadata responses that include topic IDs.
func createTopicID(topic string) [16]byte {
	hash := sha256.Sum256([]byte(topic))
	var id [16]byte
	copy(id[:], hash[:16])
	// Set UUID version 4 bits (random) for compatibility, even though it's deterministic
	id[6] = (id[6] & 0x0f) | 0x40 // Version 4
	id[8] = (id[8] & 0x3f) | 0x80 // Variant 1
	return id
}

func newKafkaServerInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*kafkaServerInput, error) {
	k := &kafkaServerInput{
		logger:       mgr.Logger(),
		shutdownCh:   make(chan struct{}),
		shutSig:      shutdown.NewSignaller(),
		decompressor: kgo.DefaultDecompressor(),
	}

	var err error
	if k.address, err = conf.FieldString(ksfFieldAddress); err != nil {
		return nil, err
	}

	if k.advertisedAddress, err = conf.FieldString(ksfFieldAdvertisedAddress); err != nil {
		return nil, err
	}

	topicList, err := conf.FieldStringList(ksfFieldTopics)
	if err != nil {
		return nil, err
	}
	if len(topicList) > 0 {
		k.allowedTopics = make(map[string]struct{})
		for _, topic := range topicList {
			k.allowedTopics[topic] = struct{}{}
		}
	}

	// Parse TLS configuration (cert_file and key_file)
	var certFile, keyFile string
	if certFile, err = conf.FieldString(ksfFieldCertFile); err != nil {
		return nil, err
	}
	if keyFile, err = conf.FieldString(ksfFieldKeyFile); err != nil {
		return nil, err
	}

	// If both cert and key are provided, set up TLS
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}

		k.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		k.logger.Infof("TLS enabled with certificate: %s", certFile)

		// Parse mTLS configuration
		if conf.Contains(ksfFieldMTLSAuth) {
			mtlsAuth, err := conf.FieldString(ksfFieldMTLSAuth)
			if err != nil {
				return nil, fmt.Errorf("failed to parse mtls_auth: %w", err)
			}

			if mtlsAuth != "" {
				// Validate and apply client auth type
				switch mtlsAuth {
				case "none":
					k.tlsConfig.ClientAuth = tls.NoClientCert
				case "request":
					k.tlsConfig.ClientAuth = tls.RequestClientCert
				case "require":
					k.tlsConfig.ClientAuth = tls.RequireAnyClientCert
				case "verify_if_given":
					k.tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
				case "require_and_verify":
					k.tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
				default:
					return nil, fmt.Errorf("invalid mtls_auth %q (valid options: none, request, require, verify_if_given, require_and_verify)", mtlsAuth)
				}

				// Parse client CA files
				var clientCAsFiles []string

				if conf.Contains(ksfFieldMTLSCAsFiles) {
					if clientCAsFiles, err = conf.FieldStringList(ksfFieldMTLSCAsFiles); err != nil {
						return nil, fmt.Errorf("failed to parse mtls_cas_files: %w", err)
					}
				}

				// Validate that CAs are provided for verification modes
				if (mtlsAuth == "verify_if_given" || mtlsAuth == "require_and_verify") && len(clientCAsFiles) == 0 {
					return nil, fmt.Errorf("mtls_cas_files must be specified when mtls_auth is set to %q", mtlsAuth)
				}

				// Load client CAs if provided
				if len(clientCAsFiles) > 0 {
					k.tlsConfig.ClientCAs = x509.NewCertPool()
					for _, clientCAsFile := range clientCAsFiles {
						caCert, err := ifs.ReadFile(mgr.FS(), clientCAsFile)
						if err != nil {
							return nil, fmt.Errorf("failed to read client CA file %s: %w", clientCAsFile, err)
						}
						if !k.tlsConfig.ClientCAs.AppendCertsFromPEM(caCert) {
							return nil, fmt.Errorf("failed to parse client CA certificates from file %s", clientCAsFile)
						}
					}
					k.logger.Debugf("Loaded client CAs from %d file(s)", len(clientCAsFiles))
				}

				k.logger.Infof("mTLS authentication enabled with mode: %s", mtlsAuth)
			}
		}
	} else if certFile != "" || keyFile != "" {
		return nil, fmt.Errorf("both cert_file and key_file must be specified to enable TLS")
	}

	// Parse SASL configuration
	if conf.Contains(ksfFieldSASL) {
		saslList, err := conf.FieldObjectList(ksfFieldSASL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SASL config: %w", err)
		}

		k.saslPlainCredentials = make(PlainCredentials)
		k.saslSCRAM256Credentials = make(SCRAMCredentials)
		k.saslSCRAM512Credentials = make(SCRAMCredentials)
		mechanismSet := make(map[string]bool)

		for i, saslConf := range saslList {
			mechanismStr, err := saslConf.FieldString("mechanism")
			if err != nil {
				return nil, fmt.Errorf("SASL config %d: %w", i, err)
			}

			// Validate mechanism string and store credentials accordingly
			username, err := saslConf.FieldString("username")
			if err != nil {
				return nil, fmt.Errorf("SASL config %d: %w", i, err)
			}

			password, err := saslConf.FieldString("password")
			if err != nil {
				return nil, fmt.Errorf("SASL config %d: %w", i, err)
			}

			if username == "" {
				return nil, fmt.Errorf("SASL config %d: username cannot be empty", i)
			}

			switch mechanismStr {
			case saslMechanismPlain:
				addPlainCredentials(k.saslPlainCredentials, username, password)
				k.logger.Debugf("Registered SASL PLAIN user: %s", username)
			case saslMechanismScramSha256:
				if err := addSCRAMCredentials(saslMechanismScramSha256, k.saslSCRAM256Credentials, username, password); err != nil {
					return nil, fmt.Errorf("SASL config %d: failed to generate SCRAM-SHA-256 credentials: %w", i, err)
				}
				k.logger.Debugf("Registered SASL SCRAM-SHA-256 user: %s", username)
			case saslMechanismScramSha512:
				if err := addSCRAMCredentials(saslMechanismScramSha512, k.saslSCRAM512Credentials, username, password); err != nil {
					return nil, fmt.Errorf("SASL config %d: failed to generate SCRAM-SHA-512 credentials: %w", i, err)
				}
				k.logger.Debugf("Registered SASL SCRAM-SHA-512 user: %s", username)
			default:
				return nil, fmt.Errorf("SASL config %d: unsupported mechanism %q (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", i, mechanismStr)
			}

			// Track unique mechanisms
			mechanismSet[mechanismStr] = true
		}

		// Build list of enabled mechanisms in priority order (SCRAM priorityOrder over PLAIN)
		priorityOrder := []string{
			saslMechanismScramSha512,
			saslMechanismScramSha256,
			saslMechanismPlain,
		}
		for _, mechanism := range priorityOrder {
			if mechanismSet[mechanism] {
				k.saslMechanisms = append(k.saslMechanisms, mechanism)
			}
		}

		k.saslEnabled = len(k.saslMechanisms) > 0
		if k.saslEnabled {
			k.logger.Debugf("SASL authentication enabled with mechanisms: %v", k.saslMechanisms)
		}
	}

	if k.timeout, err = conf.FieldDuration(ksfFieldTimeout); err != nil {
		return nil, err
	}

	if k.idleTimeout, err = conf.FieldDuration(ksfFieldIdleTimeout); err != nil {
		return nil, err
	}

	if k.maxMessageBytes, err = conf.FieldInt(ksfFieldMaxMessageBytes); err != nil {
		return nil, err
	}

	if k.idempotentWrite, err = conf.FieldBool(ksfFieldIdempotentWrite); err != nil {
		return nil, err
	}

	return k, nil
}

//------------------------------------------------------------------------------

func (k *kafkaServerInput) Connect(ctx context.Context) error {
	k.connectMu.Lock()
	defer k.connectMu.Unlock()

	if k.listener != nil {
		return nil
	}

	if k.shutSig.IsSoftStopSignalled() {
		return service.ErrEndOfInput
	}

	var listener net.Listener
	var err error

	if k.tlsConfig != nil {
		listener, err = tls.Listen("tcp", k.address, k.tlsConfig)
		if err != nil {
			return fmt.Errorf("failed to start TLS listener: %w", err)
		}
		k.logger.Infof("Kafka server listening on %s (TLS enabled)", k.address)
	} else {
		listener, err = net.Listen("tcp", k.address)
		if err != nil {
			return fmt.Errorf("failed to start listener: %w", err)
		}
		k.logger.Infof("Kafka server listening on %s", k.address)
	}

	k.listener = listener
	k.setMsgChan(make(chan messageBatch, defaultMessageChanBuffer))

	go k.acceptLoop()

	return nil
}

func (k *kafkaServerInput) acceptLoop() {
	defer func() {
		// Safely close msgChan
		msgChan := k.getMsgChan()
		if msgChan != nil {
			close(msgChan)
			k.setMsgChan(nil)
		}
		k.shutSig.TriggerHasStopped()
	}()

	// Create a channel for accepting connections
	connChan := make(chan net.Conn)
	errChan := make(chan error, 1)

	go func() {
		for {
			conn, err := k.listener.Accept()
			if err != nil {
				select {
				case errChan <- err:
				case <-k.shutdownCh:
					// Shutdown triggered, exit without sending error
				}
				return
			}
			select {
			case connChan <- conn:
			case <-k.shutdownCh:
				// Shutdown triggered, close connection and exit
				conn.Close()
				return
			}
		}
	}()

	for {
		select {
		case <-k.shutdownCh:
			return
		case conn := <-connChan:
			k.connWG.Add(1)
			go k.handleConnection(conn)
		case err := <-errChan:
			if !k.shutSig.IsSoftStopSignalled() {
				k.logger.Debugf("Accept loop ended: %v", err)
			}
			return
		}
	}
}

func (k *kafkaServerInput) handleConnection(conn net.Conn) {
	// Generate unique connection ID for structured logging
	connID := k.connCounter.Add(1)
	remoteAddr := conn.RemoteAddr().String()

	// Enable TCP_NODELAY to disable Nagle's algorithm
	// This ensures responses are sent immediately without waiting for more data
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			k.logger.Warnf("[conn:%d] Failed to set TCP_NODELAY: %v", connID, err)
		}
	}

	defer func() {
		conn.Close()
		k.connWG.Done()
		k.logger.Debugf("[conn:%d] Connection closed from %s", connID, remoteAddr)
	}()

	k.logger.Debugf("[conn:%d] Accepted connection from %s", connID, remoteAddr)

	// Track authentication state for this connection
	// If SASL is disabled, consider the connection authenticated
	connState := &connectionState{
		authenticated: !k.saslEnabled,
	}

	for {
		select {
		case <-k.shutdownCh:
			k.logger.Debugf("[conn:%d] Shutdown signaled, closing connection", connID)
			return
		default:
		}

		// Use idleTimeout for waiting on new data (can be long for quiet producers)
		// If idleTimeout is 0, wait indefinitely (no deadline) - only client disconnect will stop
		// Otherwise use idleTimeout as the read deadline
		if k.idleTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(k.idleTimeout)); err != nil {
				k.logger.Errorf("[conn:%d] Failed to set read deadline: %v", connID, err)
				return
			}
		} else {
			// Clear any previous deadline - wait forever for data
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				k.logger.Errorf("[conn:%d] Failed to clear read deadline: %v", connID, err)
				return
			}
		}

		// First, try to peek at the connection to see if any data is available
		peekBuf := make([]byte, 1)
		_, peekErr := conn.Read(peekBuf)
		if peekErr != nil {
			if peekErr == io.EOF {
				k.logger.Debugf("[conn:%d] Client closed connection (EOF) - no data received", connID)
			} else if netErr, ok := peekErr.(net.Error); ok && netErr.Timeout() {
				k.logger.Debugf("[conn:%d] Read timeout waiting for first byte", connID)
			} else {
				k.logger.Debugf("[conn:%d] Failed to read first byte: %v (type: %T)", connID, peekErr, peekErr)
			}
			return
		}
		// Now read the remaining 3 bytes of the size header
		sizeBytes := make([]byte, 4)
		sizeBytes[0] = peekBuf[0]
		if _, err := io.ReadFull(conn, sizeBytes[1:4]); err != nil {
			k.logger.Errorf("[conn:%d] Failed to read remaining size bytes: %v", connID, err)
			return
		}

		// Read request size (4 bytes)
		size := int32(binary.BigEndian.Uint32(sizeBytes))

		// Check if we're expecting legacy unframed SASL data (after SaslHandshake v0)
		if connState.expectUnframedSASL {
			k.logger.Debugf("[conn:%d] Handling legacy unframed SASL %s (size=%d)", connID, connState.scramMechanism, size)

			// Validate size for SASL payload
			if size <= 0 || size > 10000 { // SASL payloads should be small
				k.logger.Errorf("[conn:%d] Invalid unframed SASL size: %d", connID, size)
				return
			}

			// Read raw SASL bytes
			saslData := make([]byte, size)
			if _, err := io.ReadFull(conn, saslData); err != nil {
				k.logger.Errorf("[conn:%d] Failed to read unframed SASL data: %v", connID, err)
				return
			}

			// Handle legacy unframed SASL authentication based on mechanism
			var authenticated bool
			var err error
			switch connState.scramMechanism {
			case saslMechanismPlain:
				connState.expectUnframedSASL = false // PLAIN is single-step
				authenticated, err = k.handleUnframedSaslPlain(conn, connID, saslData, connState)
			case saslMechanismScramSha256, saslMechanismScramSha512:
				// SCRAM is multi-step, so keep expectUnframedSASL=true until done
				authenticated, err = k.handleUnframedSaslScram(conn, connID, saslData, connState)
			default:
				k.logger.Errorf("[conn:%d] Unknown mechanism for unframed SASL: %s", connID, connState.scramMechanism)
				return
			}

			if err != nil {
				k.logger.Errorf("[conn:%d] Legacy SASL %s authentication error: %v", connID, connState.scramMechanism, err)
				return
			}
			if authenticated {
				connState.authenticated = true
				connState.expectUnframedSASL = false // Done with auth
				k.logger.Debugf("[conn:%d] Client authenticated successfully (legacy SASL %s)", connID, connState.scramMechanism)
			}
			continue
		}

		// Request size should not exceed maxMessageBytes * requestSizeMultiplier (to account for protocol overhead)
		// Plus a reasonable upper bound for headers and metadata
		maxRequestSize := int32(k.maxMessageBytes)*requestSizeMultiplier + protocolOverheadBytes
		if size <= 0 || size > maxRequestSize {
			k.logger.Errorf("[conn:%d] Invalid request size: %d (max: %d)", connID, size, maxRequestSize)
			return
		}

		// Read request data
		requestData := make([]byte, size)
		if _, err := io.ReadFull(conn, requestData); err != nil {
			k.logger.Errorf("[conn:%d] Failed to read request data: %v", connID, err)
			return
		}

		// Parse and handle request (may update authenticated state)
		authUpdated, err := k.handleRequest(conn, connID, remoteAddr, requestData, connState)
		if err != nil {
			k.logger.Errorf("[conn:%d] Failed to handle request: %v", connID, err)
			return
		}

		// If authentication was just completed, log it
		if authUpdated && connState.authenticated {
			k.logger.Debugf("[conn:%d] Client authenticated successfully", connID)
		}
	}
}

func (k *kafkaServerInput) handleRequest(conn net.Conn, connID uint64, remoteAddr string, data []byte, connState *connectionState) (bool, error) {
	if len(data) < 8 {
		return false, fmt.Errorf("request too small: %d bytes", len(data))
	}

	reader := kbin.Reader{Src: data}
	apiKey := reader.Int16()
	apiVersion := reader.Int16()
	correlationID := reader.Int32()

	// Read and validate clientID (nullable string)
	clientIDLen := reader.Int16()
	if clientIDLen > maxClientIDLength {
		return false, fmt.Errorf("client ID too large: %d bytes", clientIDLen)
	}
	if clientIDLen > 0 {
		reader.Span(int(clientIDLen))
	}

	k.logger.Debugf("[conn:%d] Received request: apiKey=%d, apiVersion=%d, correlationID=%d, size=%d", connID, apiKey, apiVersion, correlationID, len(data))

	// Check if client needs authentication first (only if SASL is enabled)
	if k.saslEnabled && !connState.authenticated {
		switch kmsg.Key(apiKey) {
		case kmsg.ApiVersions, kmsg.SASLHandshake, kmsg.SASLAuthenticate:
			// These are allowed before authentication
		default:
			k.logger.Warnf("[conn:%d] Rejecting unauthenticated request for API key %d", connID, apiKey)
			return false, fmt.Errorf("authentication required")
		}
	}

	kreq := kmsg.RequestForKey(apiKey)
	if kreq == nil {
		k.logger.Warnf("[conn:%d] Unsupported API key: %d, closing connection", connID, apiKey)
		return false, fmt.Errorf("unsupported API key: %d", apiKey)
	}
	kreq.SetVersion(apiVersion)

	// Skip flexible header tags if applicable (KIP-482)
	if kreq.IsFlexible() {
		kmsg.SkipTags(&reader)
	}

	// Check for reader errors before proceeding
	if err := reader.Complete(); err != nil {
		k.logger.Errorf("[conn:%d] Failed to parse request header: %v", connID, err)
		return false, fmt.Errorf("failed to parse request header: %w", err)
	}

	// The remaining bytes are the request body
	bodyData := reader.Src

	k.logger.Debugf("[conn:%d] Request body size=%d, isFlexible=%v", connID, len(bodyData), kreq.IsFlexible())

	var handleErr error
	authUpdated := false

	// Handle each request type
	// For most requests, we use kreq.ReadFrom() to parse the body automatically.
	// SASL requests need special handling for raw auth bytes.
	switch req := kreq.(type) {
	case *kmsg.ApiVersionsRequest:
		k.logger.Debugf("[conn:%d] Handling ApiVersions", connID)
		resp := kmsg.NewApiVersionsResponse()
		resp.SetVersion(apiVersion)
		handleErr = k.handleApiVersionsReq(conn, connID, correlationID, req, &resp)

	case *kmsg.SASLHandshakeRequest:
		k.logger.Debugf("[conn:%d] Handling SASLHandshake", connID)
		if err := req.ReadFrom(bodyData); err != nil {
			k.logger.Errorf("[conn:%d] Failed to parse SASLHandshakeRequest: %v", connID, err)
			resp := kmsg.NewSASLHandshakeResponse()
			resp.SetVersion(apiVersion)
			resp.ErrorCode = kerr.InvalidRequest.Code
			resp.SupportedMechanisms = append([]string{}, k.saslMechanisms...)
			handleErr = k.sendResponse(conn, connID, correlationID, &resp)
		} else {
			handleErr = k.handleSaslHandshakeReq(conn, connID, correlationID, req, connState)
		}

	case *kmsg.SASLAuthenticateRequest:
		k.logger.Debugf("[conn:%d] Handling SASLAuthenticate", connID)
		// SASLAuthenticate needs special handling for raw auth bytes
		var authStatus saslAuthStatus
		authStatus, handleErr = k.handleSaslAuthenticate(conn, connID, correlationID, bodyData, apiVersion, connState)
		if handleErr == nil && authStatus == saslAuthSuccess {
			connState.authenticated = true
			authUpdated = true
		}
		// If authentication definitively failed (not just in-progress), close connection.
		// The response with ErrorCode has already been sent by handleSaslAuthenticate,
		// so the client will receive the error before the connection closes.
		if authStatus == saslAuthFailed && handleErr == nil {
			handleErr = fmt.Errorf("SASL authentication failed")
		}

	case *kmsg.MetadataRequest:
		k.logger.Debugf("[conn:%d] Handling Metadata", connID)
		resp := kmsg.NewMetadataResponse()
		resp.SetVersion(apiVersion)
		if err := req.ReadFrom(bodyData); err != nil {
			k.logger.Errorf("[conn:%d] Failed to parse MetadataRequest: %v", connID, err)
			handleErr = k.sendResponse(conn, connID, correlationID, &resp)
		} else {
			k.logger.Debugf("[conn:%d] Parsed MetadataRequest: topics count=%d", connID, len(req.Topics))
			handleErr = k.handleMetadataReq(conn, connID, correlationID, req, &resp)
		}

	case *kmsg.ProduceRequest:
		k.logger.Debugf("[conn:%d] Handling Produce, body size=%d", connID, len(bodyData))
		resp := kmsg.NewProduceResponse()
		resp.SetVersion(apiVersion)
		if err := req.ReadFrom(bodyData); err != nil {
			k.logger.Errorf("[conn:%d] Failed to parse ProduceRequest: %v", connID, err)
			handleErr = k.sendResponse(conn, connID, correlationID, &resp)
		} else {
			handleErr = k.handleProduceReq(conn, connID, remoteAddr, correlationID, req, &resp)
		}

	case *kmsg.InitProducerIDRequest:
		k.logger.Debugf("[conn:%d] Handling InitProducerID", connID)
		resp := kmsg.NewInitProducerIDResponse()
		resp.SetVersion(apiVersion)
		if err := req.ReadFrom(bodyData); err != nil {
			k.logger.Errorf("[conn:%d] Failed to parse InitProducerIDRequest: %v", connID, err)
			resp.ErrorCode = kerr.InvalidRequest.Code
			handleErr = k.sendResponse(conn, connID, correlationID, &resp)
		} else {
			handleErr = k.handleInitProducerIDReq(conn, connID, correlationID, req, &resp)
		}

	default:
		k.logger.Warnf("[conn:%d] Unsupported API key: %d (%T), closing connection", connID, apiKey, kreq)
		return false, fmt.Errorf("unsupported API key: %d", apiKey)
	}

	return authUpdated, handleErr
}

func (k *kafkaServerInput) handleApiVersionsReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.ApiVersionsRequest, resp *kmsg.ApiVersionsResponse) error {
	k.logger.Debugf("[conn:%d] Handling ApiVersions request", connID)

	resp.ErrorCode = 0

	// Advertise support for ApiVersions, Metadata, and Produce.
	// Flexible versions are supported for all APIs.
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: int16(kmsg.ApiVersions), MinVersion: 0, MaxVersion: 3}, // ApiVersions (v3 flexible)
		{ApiKey: int16(kmsg.Metadata), MinVersion: 0, MaxVersion: 12},   // Metadata (support up to v12)
		{ApiKey: int16(kmsg.Produce), MinVersion: 0, MaxVersion: 9},     // Produce (support up to v9)
	}

	// Advertise SASL support if enabled
	if k.saslEnabled {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLHandshake), MinVersion: 0, MaxVersion: 1},
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLAuthenticate), MinVersion: 0, MaxVersion: 2},
		)
	}

	// Advertise InitProducerID support if idempotent writes are enabled
	if k.idempotentWrite {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.InitProducerID), MinVersion: 0, MaxVersion: 4},
		)
	}

	k.logger.Debugf("[conn:%d] Sending ApiVersions response (SASL enabled: %v, Idempotent: %v)", connID, k.saslEnabled, k.idempotentWrite)
	return k.sendResponse(conn, connID, correlationID, resp)
}

// handleSaslHandshakeReq handles a parsed SASL handshake request.
func (k *kafkaServerInput) handleSaslHandshakeReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.SASLHandshakeRequest, connState *connectionState) error {
	resp := kmsg.NewSASLHandshakeResponse()
	resp.SetVersion(req.Version)

	k.logger.Debugf("[conn:%d] SASL mechanism %s requested", connID, req.Mechanism)

	// Check if requested mechanism is in the enabled list
	supported := false
	for _, mech := range k.saslMechanisms {
		if mech == req.Mechanism {
			supported = true
			break
		}
	}

	if supported {
		k.logger.Debugf("[conn:%d] SASL mechanism %s accepted (handshake v%d)", connID, req.Mechanism, req.Version)
		resp.ErrorCode = 0
		// Store the mechanism and handshake version for this connection
		connState.scramMechanism = req.Mechanism
		connState.saslHandshakeVersion = req.Version

		// For SaslHandshake v0, the client will send legacy unframed SASL bytes
		// (not a SaslAuthenticate request). This applies to ALL SASL mechanisms.
		if req.Version == 0 {
			connState.expectUnframedSASL = true
			k.logger.Debugf("[conn:%d] Expecting legacy unframed SASL %s authentication", connID, req.Mechanism)
		}
	} else {
		k.logger.Warnf("[conn:%d] Unsupported SASL mechanism requested: %s", connID, req.Mechanism)
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
	}

	// Always send supported mechanism list
	resp.SupportedMechanisms = append([]string{}, k.saslMechanisms...)

	return k.sendResponse(conn, connID, correlationID, &resp)
}

// handleSaslAuthenticate handles SASL authentication requests.
// Returns the authentication status and any error from sending the response.
// The status indicates success, failure, or in-progress (for multi-step SCRAM auth).
func (k *kafkaServerInput) handleSaslAuthenticate(conn net.Conn, connID uint64, correlationID int32, data []byte, apiVersion int16, connState *connectionState) (saslAuthStatus, error) {
	req := kmsg.NewSASLAuthenticateRequest()
	req.SetVersion(apiVersion)
	resp := req.ResponseKind().(*kmsg.SASLAuthenticateResponse)

	k.logger.Debugf("[conn:%d] handleSaslAuthenticate: incoming body len=%d, apiVersion=%d", connID, len(data), apiVersion)

	// Parse auth bytes based on API version
	b := kbin.Reader{Src: data}
	var authBytes []byte
	if apiVersion >= 2 {
		authBytes = b.CompactBytes()
		k.logger.Debugf("[conn:%d] Parsed SASL auth bytes as CompactBytes (len=%d)", connID, len(authBytes))
	} else {
		authBytes = b.Bytes()
		k.logger.Debugf("[conn:%d] Parsed SASL auth bytes as Bytes (len=%d)", connID, len(authBytes))
	}

	status := saslAuthInProgress

	// Check if mechanism was set during handshake
	if connState.scramMechanism == "" {
		k.logger.Errorf("[conn:%d] SASLAuthenticate received without prior handshake", connID)
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		errMsg := "SASL handshake required before authentication"
		resp.ErrorMessage = &errMsg
		return saslAuthFailed, k.sendResponse(conn, connID, correlationID, resp)
	}

	// Handle authentication based on mechanism
	k.logger.Debugf("[conn:%d] handleSaslAuthenticate: mechanism=%s, authBytes len=%d", connID, connState.scramMechanism, len(authBytes))
	switch connState.scramMechanism {
	case saslMechanismPlain:
		username, valid := validatePlainCredentials(authBytes, k.saslPlainCredentials)
		if valid {
			resp.ErrorCode = 0
			status = saslAuthSuccess
			k.logger.Debugf("[conn:%d] SASL PLAIN authentication succeeded for user: %s", connID, username)
		} else {
			k.logger.Warnf("[conn:%d] SASL PLAIN authentication failed", connID)
			resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
			errMsg := "Authentication failed"
			resp.ErrorMessage = &errMsg
			status = saslAuthFailed
		}

	case saslMechanismScramSha256, saslMechanismScramSha512:
		clientMessage := string(authBytes)

		result := processSCRAMStep(clientMessage, connState, k.saslSCRAM256Credentials, k.saslSCRAM512Credentials)
		if result.Error != nil {
			k.logger.Errorf("[conn:%d] SCRAM authentication error: %v", connID, result.Error)
			resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
			errMsg := result.Error.Error()
			resp.ErrorMessage = &errMsg
			status = saslAuthFailed
		} else {
			resp.SASLAuthBytes = []byte(result.ServerMessage)
			resp.ErrorCode = 0
			status = result.Status()
			if status == saslAuthSuccess {
				k.logger.Debugf("[conn:%d] SCRAM authentication succeeded", connID)
			}
		}

	default:
		k.logger.Errorf("[conn:%d] Unknown SASL mechanism: %s", connID, connState.scramMechanism)
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		errMsg := "Unknown SASL mechanism"
		resp.ErrorMessage = &errMsg
		status = saslAuthFailed
	}

	return status, k.sendResponse(conn, connID, correlationID, resp)
}

// handleUnframedSaslPlain handles legacy unframed SASL PLAIN authentication.
// This is used when the client sends SaslHandshake v0.
func (k *kafkaServerInput) handleUnframedSaslPlain(conn net.Conn, connID uint64, saslData []byte, connState *connectionState) (bool, error) {
	k.logger.Debugf("[conn:%d] Processing unframed SASL PLAIN: %d bytes", connID, len(saslData))

	username, authenticated := validatePlainCredentials(saslData, k.saslPlainCredentials)

	if !authenticated {
		k.logger.Warnf("[conn:%d] Legacy SASL PLAIN authentication failed", connID)
		return false, fmt.Errorf("authentication failed")
	}

	// Send success response: 4-byte size = 0 (empty outcome for PLAIN success)
	k.logger.Debugf("[conn:%d] Legacy SASL PLAIN authentication succeeded for user: %s", connID, username)
	if err := conn.SetWriteDeadline(time.Now().Add(k.timeout)); err != nil {
		return false, fmt.Errorf("failed to set write deadline: %w", err)
	}

	successResp := []byte{0x00, 0x00, 0x00, 0x00}
	if _, err := conn.Write(successResp); err != nil {
		return false, fmt.Errorf("failed to write SASL success response: %w", err)
	}

	return true, nil
}

// handleUnframedSaslScram handles legacy unframed SCRAM authentication.
func (k *kafkaServerInput) handleUnframedSaslScram(conn net.Conn, connID uint64, saslData []byte, connState *connectionState) (bool, error) {
	k.logger.Debugf("[conn:%d] Processing unframed SASL SCRAM %s", connID, connState.scramMechanism)

	clientMessage := string(saslData)

	result := processSCRAMStep(clientMessage, connState, k.saslSCRAM256Credentials, k.saslSCRAM512Credentials)
	if result.Error != nil {
		k.logger.Errorf("[conn:%d] SCRAM conversation step failed: %v", connID, result.Error)
		return false, fmt.Errorf("authentication failed: %w", result.Error)
	}

	// Send server response as unframed SASL
	if err := conn.SetWriteDeadline(time.Now().Add(k.timeout)); err != nil {
		return false, fmt.Errorf("failed to set write deadline: %w", err)
	}

	// Write [4-byte length][server message]
	respBytes := []byte(result.ServerMessage)
	if err := binary.Write(conn, binary.BigEndian, int32(len(respBytes))); err != nil {
		return false, fmt.Errorf("failed to write SCRAM response size: %w", err)
	}
	if _, err := conn.Write(respBytes); err != nil {
		return false, fmt.Errorf("failed to write SCRAM response: %w", err)
	}

	switch result.AuthStatus {
	case saslAuthSuccess:
		k.logger.Debugf("[conn:%d] SCRAM authentication succeeded (unframed)", connID)
		return true, nil
	case saslAuthFailed:
		k.logger.Warnf("[conn:%d] SCRAM authentication failed: invalid credentials", connID)
		return false, fmt.Errorf("invalid credentials")
	default:
		// Conversation continues (need more steps)
		return false, nil
	}
}

// handleInitProducerIDReq handles InitProducerID requests for idempotent producers.
// This allocates a unique producer ID to the client. Note that this implementation
// does NOT provide actual exactly-once semantics - it only satisfies the protocol
// requirements so that clients can use idempotent producers.
func (k *kafkaServerInput) handleInitProducerIDReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.InitProducerIDRequest, resp *kmsg.InitProducerIDResponse) error {
	if !k.idempotentWrite {
		k.logger.Warnf("[conn:%d] InitProducerID request received but idempotent_write is not enabled", connID)
		resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code
		return k.sendResponse(conn, connID, correlationID, resp)
	}

	// Transactions are not supported
	if req.TransactionalID != nil && *req.TransactionalID != "" {
		k.logger.Warnf("[conn:%d] InitProducerID request with transactional ID not supported", connID)
		resp.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
		return k.sendResponse(conn, connID, correlationID, resp)
	}

	// Allocate a new producer ID
	// For simplicity, we use an incrementing counter. Real Kafka uses a combination
	// of broker ID and a local counter to ensure uniqueness across the cluster.
	newProducerID := k.producerIDCounter.Add(1)

	resp.ErrorCode = 0
	resp.ProducerID = newProducerID
	resp.ProducerEpoch = 0 // New producer always starts at epoch 0

	k.logger.Debugf("[conn:%d] Allocated producer ID %d (epoch 0) for idempotent producer", connID, newProducerID)
	return k.sendResponse(conn, connID, correlationID, resp)
}

func (k *kafkaServerInput) handleMetadataReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.MetadataRequest, resp *kmsg.MetadataResponse) error {

	k.logger.Debugf("[conn:%d] Metadata request: topics count=%d, AllowAutoTopicCreation=%v, IncludeTopicAuthorizedOperations=%v",
		connID, len(req.Topics), req.AllowAutoTopicCreation, req.IncludeTopicAuthorizedOperations)

	// Extract requested topics
	var requestedTopics []string
	for _, t := range req.Topics {
		if t.Topic != nil {
			requestedTopics = append(requestedTopics, *t.Topic)
		}
	}

	k.logger.Debugf("[conn:%d] Requested topics: %v", connID, requestedTopics)

	// Determine advertised host and port
	var host, portStr string
	var err error

	// Use advertised_address if configured, otherwise use listen address
	addressToUse := k.address
	if k.advertisedAddress != "" {
		addressToUse = k.advertisedAddress
		k.logger.Debugf("[conn:%d] Using advertised address: %s", connID, addressToUse)
	}

	host, portStr, err = net.SplitHostPort(addressToUse)
	if err != nil {
		k.logger.Errorf("[conn:%d] Failed to parse address %s: %v", connID, addressToUse, err)
		// Fallback to defaults
		host = "127.0.0.1"
		portStr = "9092"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		k.logger.Errorf("[conn:%d] Failed to parse port %s: %v", connID, portStr, err)
		port = 9092
	}

	// If listening on wildcard address and no advertised address is set,
	// use the connection's local address so clients can connect back to
	// the same interface they reached the server on
	if k.advertisedAddress == "" && (host == "" || host == "0.0.0.0" || host == "::") {
		if localAddr := conn.LocalAddr(); localAddr != nil {
			if localHost, _, err := net.SplitHostPort(localAddr.String()); err == nil && localHost != "" {
				k.logger.Debugf("[conn:%d] Using connection local address %s instead of wildcard %s", connID, localHost, host)
				host = localHost
			}
		}
	}

	k.logger.Debugf("[conn:%d] Broker metadata: host=%s, port=%d", connID, host, port)

	resp.Brokers = []kmsg.MetadataResponseBroker{
		{
			NodeID: 1,
			Host:   host,
			Port:   int32(port),
			// Rack is not set (defaults to nil for nullable field)
		},
	}

	// Set cluster ID and controller ID
	clusterID := "kafka-server-cluster"
	resp.ClusterID = &clusterID
	resp.ControllerID = 1

	// Determine which topics to include in response
	var topicsToReturn []string
	if len(requestedTopics) > 0 {
		// Client requested specific topics
		topicsToReturn = requestedTopics
		k.logger.Debugf("[conn:%d] Returning requested topics: %v", connID, topicsToReturn)
	} else if k.allowedTopics != nil {
		// No specific topics requested, return all allowed topics
		for topic := range k.allowedTopics {
			topicsToReturn = append(topicsToReturn, topic)
		}
		k.logger.Debugf("[conn:%d] Returning allowed topics: %v", connID, topicsToReturn)
	} else {
		k.logger.Debugf("[conn:%d] No topics to return (no filter configured, no specific topics requested)", connID)
	}

	// Add topics to response
	for _, topic := range topicsToReturn {
		// Check if topic is allowed (if filtering is enabled)
		if k.allowedTopics != nil {
			if _, ok := k.allowedTopics[topic]; !ok {
				// Topic not allowed, return with error code
				resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
					Topic:     kmsg.StringPtr(topic),
					TopicID:   createTopicID(topic),
					ErrorCode: kerr.UnknownTopicOrPartition.Code,
				})
				continue
			}
		}

		// Topic is allowed, return metadata with TopicID and LeaderEpoch
		// TopicID is required for metadata v10+ (flexible versions)
		// LeaderEpoch is used by clients for fencing and leader election awareness
		resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
			Topic:      kmsg.StringPtr(topic),
			TopicID:    createTopicID(topic),
			ErrorCode:  0,
			IsInternal: false,
			Partitions: []kmsg.MetadataResponseTopicPartition{
				{
					Partition:   0,
					Leader:      1,
					LeaderEpoch: 0, // Static epoch since we only have one broker
					Replicas:    []int32{1},
					ISR:         []int32{1},
					ErrorCode:   0,
				},
			},
		})
	}

	k.logger.Debugf("[conn:%d] Sending metadata response with %d topics", connID, len(resp.Topics))
	return k.sendResponse(conn, connID, correlationID, resp)
}

// partitionErrorKey uniquely identifies a topic-partition pair for error tracking.
type partitionErrorKey struct {
	topic     string
	partition int32
}

// buildProduceResponseTopics constructs response topics for all topics/partitions in a produce request.
// partitionErrors provides per-partition error code overrides (e.g., MessageTooLarge, UnknownTopicOrPartition).
func buildProduceResponseTopics(topics []kmsg.ProduceRequestTopic, defaultErrorCode int16, partitionErrors map[partitionErrorKey]int16) []kmsg.ProduceResponseTopic {
	result := make([]kmsg.ProduceResponseTopic, 0, len(topics))
	for _, topic := range topics {
		respTopic := kmsg.ProduceResponseTopic{
			Topic:      topic.Topic,
			Partitions: make([]kmsg.ProduceResponseTopicPartition, 0, len(topic.Partitions)),
		}
		for _, partition := range topic.Partitions {
			// Use NewProduceResponseTopicPartition() to ensure proper initialization
			// of tagged fields (CurrentLeader defaults to -1,-1 which means "not present")
			partResp := kmsg.NewProduceResponseTopicPartition()
			partResp.Partition = partition.Partition
			partResp.BaseOffset = 0
			partResp.LogAppendTime = -1
			partResp.LogStartOffset = 0
			// Use per-partition error if set, otherwise use the default error code
			if errCode, ok := partitionErrors[partitionErrorKey{topic.Topic, partition.Partition}]; ok {
				partResp.ErrorCode = errCode
			} else {
				partResp.ErrorCode = defaultErrorCode
			}
			respTopic.Partitions = append(respTopic.Partitions, partResp)
		}
		result = append(result, respTopic)
	}
	return result
}

func (k *kafkaServerInput) handleProduceReq(conn net.Conn, connID uint64, remoteAddr string, correlationID int32, req *kmsg.ProduceRequest, resp *kmsg.ProduceResponse) error {
	k.logger.Debugf("[conn:%d] Produce request: correlationID=%d, acks=%d, topics=%d", connID, correlationID, req.Acks, len(req.Topics))

	// Validate acks value (must be -1, 0, or 1) per Kafka protocol
	switch req.Acks {
	case -1, 0, 1:
	default:
		k.logger.Warnf("[conn:%d] Invalid acks value: %d", connID, req.Acks)
		resp.Topics = buildProduceResponseTopics(req.Topics, kerr.InvalidRequiredAcks.Code, nil)
		return k.sendResponse(conn, connID, correlationID, resp)
	}

	// Create context with timeout for this request
	ctx, cancel := context.WithTimeout(context.Background(), k.timeout)
	defer cancel()

	var batch service.MessageBatch

	// Track per-partition errors (e.g., topic filtering, message too large)
	partErrors := make(map[partitionErrorKey]int16)

	// Iterate through topics
	for _, topic := range req.Topics {
		topicName := topic.Topic
		k.logger.Debugf("[conn:%d] Processing topic: %s, partitions=%d", connID, topicName, len(topic.Partitions))

		// Check if topic is allowed
		if k.allowedTopics != nil {
			if _, ok := k.allowedTopics[topicName]; !ok {
				k.logger.Warnf("[conn:%d] Rejecting produce to disallowed topic: %s", connID, topicName)
				for _, partition := range topic.Partitions {
					partErrors[partitionErrorKey{topicName, partition.Partition}] = kerr.UnknownTopicOrPartition.Code
				}
				continue
			}
		}

		// Iterate through partitions
		for i, partition := range topic.Partitions {
			k.logger.Debugf("[conn:%d] Partition %d: Records=%v, len=%d", connID, i, partition.Records != nil, len(partition.Records))
			if len(partition.Records) == 0 {
				k.logger.Debugf("[conn:%d] Skipping partition %d (empty records)", connID, i)
				continue
			}

			// Check message size before parsing to avoid wasting CPU on oversized batches
			if k.maxMessageBytes > 0 && len(partition.Records) > k.maxMessageBytes {
				k.logger.Warnf("[conn:%d] Record batch too large for topic=%s partition=%d: %d > %d", connID, topicName, partition.Partition, len(partition.Records), k.maxMessageBytes)
				partErrors[partitionErrorKey{topicName, partition.Partition}] = kerr.MessageTooLarge.Code
				continue
			}

			// Parse records from the record batch
			k.logger.Debugf("[conn:%d] About to parse record batch for partition %d", connID, i)
			messages, err := k.parseRecordBatch(connID, partition.Records, topicName, partition.Partition, remoteAddr)
			if err != nil {
				k.logger.Errorf("[conn:%d] Failed to parse record batch for topic=%s partition=%d: %v", connID, topicName, partition.Partition, err)
				continue
			}
			k.logger.Debugf("[conn:%d] Parsed %d messages from partition %d", connID, len(messages), i)

			batch = append(batch, messages...)
		}
	}

	k.logger.Debugf("[conn:%d] Batch ready: %d messages from %d topics", connID, len(batch), len(req.Topics))
	// If no messages, still need to build response for all requested topics/partitions
	if len(batch) == 0 {
		k.logger.Debugf("[conn:%d] No messages to process, building success response", connID)
		resp.Topics = buildProduceResponseTopics(req.Topics, 0, partErrors)
		return k.sendResponse(conn, connID, correlationID, resp)
	}

	// Send batch to pipeline
	resChan := make(chan error, 1)
	k.logger.Debugf("[conn:%d] Sending batch to pipeline, acks=%d, batch_size=%d", connID, req.Acks, len(batch))
	msgChan := k.getMsgChan()
	if msgChan == nil {
		k.logger.Errorf("[conn:%d] msgChan is nil, cannot send batch", connID)
		return fmt.Errorf("server not connected")
	}
	select {
	case msgChan <- messageBatch{
		batch: batch,
		ackFn: func(ackCtx context.Context, err error) error {
			select {
			case resChan <- err:
			default:
				// Channel full or closed, don't block
				k.logger.Warnf("Failed to send ack result to resChan")
			}
			return nil
		},
		resChan: resChan,
	}:
		k.logger.Debugf("[conn:%d] Successfully sent batch to pipeline", connID)
	case <-ctx.Done():
		k.logger.Warnf("[conn:%d] Timeout sending batch to pipeline", connID)
		resp.Topics = buildProduceResponseTopics(req.Topics, kerr.RequestTimedOut.Code, partErrors)
		return k.sendResponse(conn, connID, correlationID, resp)
	case <-k.shutdownCh:
		return fmt.Errorf("shutting down")
	}

	// For proper backpressure, we wait for the pipeline to acknowledge processing
	// before responding to the producer. This ensures that if the output is slow,
	// the producer will slow down accordingly.
	k.logger.Debugf("[conn:%d] Waiting for pipeline acknowledgment (acks=%d)", connID, req.Acks)
	select {
	case ackErr := <-resChan:
		if ackErr != nil {
			k.logger.Warnf("[conn:%d] Pipeline returned error: %v", connID, ackErr)
			resp.Topics = buildProduceResponseTopics(req.Topics, kerr.UnknownServerError.Code, partErrors)
		} else {
			k.logger.Debugf("[conn:%d] Pipeline acknowledged successfully", connID)
			resp.Topics = buildProduceResponseTopics(req.Topics, 0, partErrors)
		}
	case <-ctx.Done():
		k.logger.Warnf("[conn:%d] Timeout waiting for pipeline acknowledgment", connID)
		resp.Topics = buildProduceResponseTopics(req.Topics, kerr.RequestTimedOut.Code, partErrors)
	case <-k.shutdownCh:
		return fmt.Errorf("shutting down")
	}

	k.logger.Debugf("[conn:%d] Sending produce response, topics=%d", connID, len(resp.Topics))
	return k.sendResponse(conn, connID, correlationID, resp)
}

func (k *kafkaServerInput) parseRecordBatch(connID uint64, data []byte, topic string, partition int32, remoteAddr string) (service.MessageBatch, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Create a FetchResponseTopicPartition with our raw records data
	fakePartition := &kmsg.FetchResponseTopicPartition{
		Partition:     partition,
		RecordBatches: data,
	}

	opts := kgo.ProcessFetchPartitionOpts{
		Topic:                topic,
		Partition:            partition,
		KeepControlRecords:   false,
		DisableCRCValidation: true, // We don't need to validate CRC for incoming data
	}

	// ProcessFetchPartition returns parsed records
	fp, _ := kgo.ProcessFetchPartition(opts, fakePartition, k.decompressor, nil)

	if fp.Err != nil {
		return nil, fmt.Errorf("failed to process records: %w", fp.Err)
	}

	// Convert kgo.Record to service.Message
	batch := make(service.MessageBatch, 0, len(fp.Records))
	for _, record := range fp.Records {
		msg := service.NewMessage(record.Value)
		msg.MetaSetMut("kafka_server_topic", record.Topic)
		msg.MetaSetMut("kafka_server_partition", int(record.Partition))
		msg.MetaSetMut("kafka_server_offset", record.Offset)
		msg.MetaSetMut("kafka_server_tombstone_message", record.Value == nil)

		if record.Key != nil {
			msg.MetaSetMut("kafka_server_key", string(record.Key))
		}

		if !record.Timestamp.IsZero() {
			msg.MetaSetMut("kafka_server_timestamp_unix", record.Timestamp.Unix())
			msg.MetaSetMut("kafka_server_timestamp", record.Timestamp.Format(time.RFC3339))
		}

		msg.MetaSetMut("kafka_server_client_address", remoteAddr)

		// Add record headers as metadata
		for _, header := range record.Headers {
			msg.MetaSetMut(header.Key, string(header.Value))
		}

		batch = append(batch, msg)
	}

	k.logger.Debugf("[conn:%d] Parsed %d records", connID, len(batch))
	return batch, nil
}

func (k *kafkaServerInput) sendResponse(conn net.Conn, connID uint64, correlationID int32, msg kmsg.Response) error {
	buf := kbin.AppendInt32(nil, correlationID)

	// For flexible responses (EXCEPT ApiVersions key 18), add response header TAG_BUFFER
	if msg.IsFlexible() && msg.Key() != int16(kmsg.ApiVersions) {
		buf = append(buf, 0) // Empty TAG_BUFFER (0 tags)
	}

	// AppendTo generates the response body
	buf = msg.AppendTo(buf)

	k.logger.Debugf("[conn:%d] Sending response: correlationID=%d, key=%d, size=%d", connID, correlationID, msg.Key(), len(buf))

	return k.writeResponse(connID, conn, buf)
}

func (k *kafkaServerInput) writeResponse(connID uint64, conn net.Conn, data []byte) error {
	// Set write deadline to prevent hanging on slow clients
	if err := conn.SetWriteDeadline(time.Now().Add(k.timeout)); err != nil {
		k.logger.Errorf("[conn:%d] Failed to set write deadline: %v", connID, err)
		return err
	}

	// Prepare the full response: size prefix (4 bytes) + data
	fullData := kbin.AppendInt32(nil, int32(len(data)))
	fullData = append(fullData, data...)

	// Write everything in one call
	n, err := conn.Write(fullData)
	if err != nil {
		k.logger.Errorf("[conn:%d] Failed to write response: %v", connID, err)
		return err
	}
	k.logger.Debugf("[conn:%d] Wrote %d bytes response", connID, n)
	return nil
}

func (k *kafkaServerInput) getMsgChan() chan messageBatch {
	c, _ := k.msgChanValue.Load().(chan messageBatch)
	return c
}

func (k *kafkaServerInput) setMsgChan(ch chan messageBatch) {
	k.msgChanValue.Store(ch)
}

func (k *kafkaServerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	msgChan := k.getMsgChan()
	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case mb, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return mb.batch, mb.ackFn, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-k.shutdownCh:
		return nil, nil, service.ErrEndOfInput
	}
}

func (k *kafkaServerInput) Close(ctx context.Context) error {
	// Protect against double close with atomic swap
	if k.shutdownDone.Swap(true) {
		return nil // Already closed
	}

	k.shutSig.TriggerSoftStop()

	// Use sync.Once to ensure channel is only closed once
	k.shutdownOnce.Do(func() {
		close(k.shutdownCh)
	})

	if k.listener != nil {
		k.listener.Close()
	}

	// Wait for connections to finish with timeout
	done := make(chan struct{})
	go func() {
		k.connWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(shutdownGracePeriod):
		k.logger.Warn("Timeout waiting for connections to close")
	}

	k.shutSig.TriggerHasStopped()

	return nil
}
